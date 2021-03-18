package queue

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/oklog/ulid/v2"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

var (
	log = logging.Logger("buckets/ps-queue")

	// Interval is the interval between background queue ticks.
	Interval = time.Second * 10

	// MaxConcurrency is the maximum number of requests that will be handled concurrently.
	MaxConcurrency = 100

	// dsQueuePrefix is the global time-ordered queue used internally for processing.
	// Structure: /queue/<requestid>
	dsQueuePrefix = ds.NewKey("/queue")

	// dsQueuedStatusPrefix is a bucket-grouped time-ordered status queue for "queued" requests.
	// Structure: /queued/<bucketkey>/<requestid>
	dsQueuedStatusPrefix = ds.NewKey("/queued")

	// dsPinningStatusPrefix is a bucket-grouped time-ordered status queue for "pinning" requests.
	// Structure: /pinning/<bucketkey>/<requestid>
	dsPinningStatusPrefix = ds.NewKey("/pinning")

	// dsPinnedStatusPrefix is a bucket-grouped time-ordered status queue for "pinned" requests.
	// Structure: /pinned/<bucketkey>/<requestid>
	dsPinnedStatusPrefix = ds.NewKey("/pinned")

	// dsFailedStatusPrefix is a bucket-grouped time-ordered status queue for "failed" requests.
	// Structure: /failed/<bucketkey>/<requestid>
	dsFailedStatusPrefix = ds.NewKey("/failed")
)

const (
	defaultListLimit = 10
	maxListLimit     = 1000
)

type Request struct {
	ID  string
	Cid string

	Thread   core.ID
	Key      string
	Identity did.Token
}

type RequestHandler func(ctx context.Context, request *Request) error

type Queue struct {
	store ds.TxnDatastore

	handler        RequestHandler
	failureHandler RequestHandler

	jobCh chan *Request

	ctx    context.Context
	cancel context.CancelFunc
}

func NewQueue(store ds.TxnDatastore, handler RequestHandler, failureHandler RequestHandler) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		store:          store,
		handler:        handler,
		failureHandler: failureHandler,
		jobCh:          make(chan *Request, MaxConcurrency),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Create queue workers
	for i := 0; i < MaxConcurrency; i++ {
		go q.worker(i + 1)
	}

	go q.start()
	return q
}

func (q *Queue) Close() error {
	q.cancel()
	return nil
}

func (q *Queue) NewID() string {
	return newIDFromTime(time.Now())
}

func newIDFromTime(t time.Time) string {
	return strings.ToLower(ulid.MustNew(ulid.Timestamp(t.UTC()), rand.Reader).String())
}

func (q *Queue) AddRequest(thread core.ID, bucket string, status *openapi.PinStatus, identity did.Token) error {
	return q.enqueue(&Request{
		ID:       status.Requestid,
		Cid:      status.Pin.Cid,
		Thread:   thread,
		Key:      bucket,
		Identity: identity,
	}, true)
}

func (q *Queue) ListRequests(key string, status openapi.Status, before, after time.Time, limit int) ([]Request, error) {
	pre, err := getStatusKeyPrefix(status)
	if err != nil {
		return nil, fmt.Errorf("getting status prefix: %v", err)
	}
	var filters []query.Filter
	if !before.IsZero() {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.LessThan,
			Key: getStatusKey(pre, key, newIDFromTime(before)).String(),
		})
	}
	if !after.IsZero() {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.GreaterThan,
			Key: getStatusKey(pre, key, newIDFromTime(after)).String(),
		})
	}
	if limit <= 0 {
		limit = defaultListLimit
	} else if limit > maxListLimit {
		limit = maxListLimit
	}
	results, err := q.store.Query(query.Query{
		Prefix:  pre.String(),
		Filters: filters,
		Orders:  []query.Order{query.OrderByKey{}},
		Limit:   limit,
	})
	if err != nil {
		return nil, fmt.Errorf("querying requests: %v", err)
	}
	defer results.Close()

	var reqs []Request
	for res := range results.Next() {
		if res.Error != nil {
			return nil, fmt.Errorf("getting next result: %v", res.Error)
		}
		r, err := decode(res.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding request: %v", err)
		}
		reqs = append(reqs, *r)
	}
	return reqs, nil
}

func (q *Queue) GetRequest(bucket, id string, status openapi.Status) (*Request, error) {
	pre, err := getStatusKeyPrefix(status)
	if err != nil {
		return nil, fmt.Errorf("getting status prefix: %v", err)
	}
	val, err := q.store.Get(getStatusKey(pre, bucket, id))
	if err != nil {
		return nil, fmt.Errorf("getting status key: %v", err)
	}
	return decode(val)
}

func (q *Queue) enqueue(r *Request, isNew bool) error {
	// Block while the request is placed in a queue
	if isNew {
		val, err := encode(r)
		if err != nil {
			return fmt.Errorf("encoding request: %v", err)
		}

		// Put the new request directly in the "pinning" queue in case a worker is available now
		if err := q.store.Put(getStatusKey(dsPinningStatusPrefix, r.Key, r.ID), val); err != nil {
			return fmt.Errorf("putting status key: %v", err)
		}
	} else {
		// Move the request to the "pinning" queue
		if err := q.moveRequest(r, openapi.QUEUED, openapi.PINNING); err != nil {
			return fmt.Errorf("updating status (pinning): %v", err)
		}
	}

	// Unblock the caller by letting the rest happen in the background
	go func() {
		select {
		case q.jobCh <- r:
		default:
			// Workers are busy, put back in the "queued" queue
			if err := q.moveRequest(r, openapi.PINNING, openapi.QUEUED); err != nil {
				log.Errorf("updating status (queued): %v", err)
			}
		}
	}()
	return nil
}

func (q *Queue) start() {
	t := time.NewTicker(Interval)
	for {
		select {
		case <-t.C:
			q.process()
		case <-q.ctx.Done():
			t.Stop()
			return
		}
	}
}

func (q *Queue) process() {
	queue, err := q.listQueued()
	if err != nil {
		log.Errorf("listing requests: %v", err)
		return
	}
	if len(queue) > 0 {
		log.Debugf("enqueueing %d requests", len(queue))
	}
	for _, r := range queue {
		if err := q.enqueue(&r, false); err != nil {
			log.Errorf("enqueueing request: %v", err)
		}
	}
}

func (q *Queue) listQueued() ([]Request, error) {
	results, err := q.store.Query(query.Query{
		Prefix: dsQueuePrefix.String(),
		Orders: []query.Order{query.OrderByKey{}},
		Limit:  MaxConcurrency,
	})
	if err != nil {
		return nil, fmt.Errorf("querying requests: %v", err)
	}
	defer results.Close()

	var reqs []Request
	for res := range results.Next() {
		if res.Error != nil {
			return nil, fmt.Errorf("getting next result: %v", res.Error)
		}
		r, err := decode(res.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding request: %v", err)
		}
		reqs = append(reqs, *r)
	}
	return reqs, nil
}

func (q *Queue) worker(num int) {
	for {
		select {
		case <-q.ctx.Done():
			return

		case r := <-q.jobCh:
			if q.ctx.Err() != nil {
				return
			}
			log.Debugf("worker %d got job %s", num, r.ID)

			// Handle the request with the handler func
			status := openapi.PINNED
			if err := q.handler(q.ctx, r); err != nil {
				status = openapi.FAILED

				// The handler returned an error, send the request to the failure handler
				log.Errorf("handling request: %v", err)
				if err := q.failureHandler(q.ctx, r); err != nil {
					log.Errorf("failing request: %v", err)
				}
			}

			// Finalize request by moving it to either the "pinned" or "failed" queue
			if err := q.moveRequest(r, openapi.PINNING, status); err != nil {
				log.Errorf("updating status (%s): %v", status, err)
			}

			log.Debugf("worker %d finished job %s", num, r.ID)
		}
	}
}

func (q *Queue) moveRequest(r *Request, from, to openapi.Status) error {
	fromPre, err := getStatusKeyPrefix(from)
	if err != nil {
		return fmt.Errorf("getting 'from' prefix: %v", err)
	}
	toPre, err := getStatusKeyPrefix(to)
	if err != nil {
		return fmt.Errorf("getting 'to' prefix: %v", err)
	}
	fromKey := getStatusKey(fromPre, r.Key, r.ID)
	toKey := getStatusKey(toPre, r.Key, r.ID)

	val, err := encode(r)
	if err != nil {
		return fmt.Errorf("encoding request: %v", err)
	}

	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	// Delete from global queue
	if from == openapi.QUEUED {
		if err := txn.Delete(dsQueuePrefix.ChildString(r.ID)); err != nil {
			return fmt.Errorf("deleting key: %v", err)
		}
	}
	// Add to global queue
	if to == openapi.QUEUED {
		if err := txn.Put(dsQueuePrefix.ChildString(r.ID), val); err != nil {
			return fmt.Errorf("putting key: %v", err)
		}
	}

	// Move between status queues
	if err := txn.Put(toKey, val); err != nil {
		return fmt.Errorf("putting status key: %v", err)
	}
	if err := txn.Delete(fromKey); err != nil {
		return fmt.Errorf("deleting status key: %v", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing txn: %v", err)
	}
	return nil
}

func getStatusKeyPrefix(status openapi.Status) (ds.Key, error) {
	switch status {
	case openapi.QUEUED:
		return dsQueuedStatusPrefix, nil
	case openapi.PINNING:
		return dsPinningStatusPrefix, nil
	case openapi.PINNED:
		return dsPinnedStatusPrefix, nil
	case openapi.FAILED:
		return dsFailedStatusPrefix, nil
	default:
		return ds.Key{}, fmt.Errorf("invalid status: %s", status)
	}
}

func getStatusKey(pre ds.Key, key, id string) ds.Key {
	return pre.ChildString(key).ChildString(id)
}

func encode(r *Request) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return nil, fmt.Errorf("encoding request: %v", err)
	}
	return buf.Bytes(), nil
}

func decode(v []byte) (*Request, error) {
	var buf bytes.Buffer
	buf.Write(v)
	dec := gob.NewDecoder(&buf)
	var r Request
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("decoding key value: %v", err)
	}
	return &r, nil
}
