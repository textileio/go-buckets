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
	"github.com/textileio/go-buckets/pinning/openapi"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

var (
	// Interval is the interval between background queue ticks.
	Interval = time.Second * 10

	// MaxConcurrency is the maximum number of requests that will be handled concurrently.
	MaxConcurrency = 100

	log = logging.Logger("buckets/ps-queue")

	dsQueuedPrefix  = ds.NewKey("/queued")
	dsPinningPrefix = ds.NewKey("/pinning")
	dsPinnedPrefix  = ds.NewKey("/pinned")
	dsFailedPrefix  = ds.NewKey("/failed")
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

	jobCh chan string

	ctx    context.Context
	cancel context.CancelFunc
}

func NewQueue(store ds.TxnDatastore, handler RequestHandler, failureHandler RequestHandler) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		store:          store,
		handler:        handler,
		failureHandler: failureHandler,
		jobCh:          make(chan string, MaxConcurrency),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Create queue workers
	for i := 0; i < MaxConcurrency; i++ {
		go q.worker()
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

func (q *Queue) AddRequest(thread core.ID, key string, status *openapi.PinStatus, identity did.Token) error {
	req := Request{
		ID:       status.Requestid,
		Cid:      status.Pin.Cid,
		Thread:   thread,
		Key:      key,
		Identity: identity,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(req); err != nil {
		return fmt.Errorf("encoding request: %v", err)
	}
	if err := q.store.Put(dsQueuedPrefix.ChildString(req.ID), buf.Bytes()); err != nil {
		return fmt.Errorf("putting key: %v", err)
	}

	q.enqueue(req.ID)
	return nil
}

func (q *Queue) ListRequests(status openapi.Status, before, after time.Time, limit int) ([]string, error) {
	pre, err := getKeyPrefix(status)
	if err != nil {
		return nil, fmt.Errorf("getting prefix: %v", err)
	}
	var filters []query.Filter
	if !before.IsZero() {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.LessThan,
			Key: pre.Child(ds.NewKey(newIDFromTime(before))).String(),
		})
	}
	if !after.IsZero() {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.GreaterThan,
			Key: pre.Child(ds.NewKey(newIDFromTime(before))).String(),
		})
	}
	if limit <= 0 {
		limit = defaultListLimit
	} else if limit > maxListLimit {
		limit = maxListLimit
	}
	results, err := q.store.Query(query.Query{
		Prefix:   pre.String(),
		Filters:  filters,
		Orders:   []query.Order{query.OrderByKey{}},
		Limit:    limit,
		KeysOnly: true,
	})
	if err != nil {
		return nil, fmt.Errorf("querying requests: %v", err)
	}
	defer results.Close()

	var reqs []string
	for r := range results.Next() {
		if r.Error != nil {
			return nil, fmt.Errorf("getting next result: %v", r.Error)
		}
		reqs = append(reqs, strings.TrimPrefix(r.Key, pre.String()+"/"))
	}
	return reqs, nil
}

func (q *Queue) GetRequest(id string, status openapi.Status) (*Request, error) {
	key, err := getKeyPrefix(status)
	if err != nil {
		return nil, fmt.Errorf("getting prefix: %v", err)
	}
	val, err := q.store.Get(key.ChildString(id))
	if err != nil {
		return nil, fmt.Errorf("getting key: %v", err)
	}
	return decode(val)
}

func (q *Queue) moveRequest(id string, from, to openapi.Status) error {
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	fromPre, err := getKeyPrefix(from)
	if err != nil {
		return fmt.Errorf("getting 'from' prefix: %v", err)
	}
	toPre, err := getKeyPrefix(to)
	if err != nil {
		return fmt.Errorf("getting 'to' prefix: %v", err)
	}
	fromKey := fromPre.ChildString(id)
	toKey := toPre.ChildString(id)

	val, err := txn.Get(fromKey)
	if err != nil {
		return fmt.Errorf("getting key: %v", err)
	}
	if err := txn.Put(toKey, val); err != nil {
		return fmt.Errorf("putting key: %v", err)
	}
	if err := txn.Delete(fromKey); err != nil {
		return fmt.Errorf("deleting key: %v", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing txn: %v", err)
	}
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
	queue, err := q.ListRequests(openapi.QUEUED, time.Time{}, time.Time{}, MaxConcurrency)
	if err != nil {
		log.Errorf("listing requests: %v", err)
		return
	}
	if len(queue) > 0 {
		log.Debugf("enqueueing %d requests", len(queue))
	}
	for _, i := range queue {
		q.enqueue(i)
	}
}

func (q *Queue) enqueue(id string) {
	// Block while the request is removed from the main queue
	if err := q.moveRequest(id, openapi.QUEUED, openapi.PINNING); err != nil {
		log.Errorf("updating status (pinning): %v", err)
	}
	go func() {
		select {
		case q.jobCh <- id:
		default:
			// Workers are busy, put back in the main queue
			if err := q.moveRequest(id, openapi.PINNING, openapi.QUEUED); err != nil {
				log.Errorf("updating status (queued): %v", err)
			}
		}
	}()
}

func (q *Queue) worker() {
	for {
		select {
		case <-q.ctx.Done():
			return

		case id := <-q.jobCh:
			if q.ctx.Err() != nil {
				return
			}
			r, err := q.GetRequest(id, openapi.PINNING)
			if err != nil {
				log.Errorf("getting request: %v", err)
				break
			}

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

			// Finalize request by moving it to either "pinned" or "failed"
			if err := q.moveRequest(id, openapi.PINNING, status); err != nil {
				log.Errorf("updating status (%s): %v", status, err)
			}
		}
	}
}

func getKeyPrefix(status openapi.Status) (ds.Key, error) {
	switch status {
	case openapi.QUEUED:
		return dsQueuedPrefix, nil
	case openapi.PINNING:
		return dsPinningPrefix, nil
	case openapi.PINNED:
		return dsPinnedPrefix, nil
	case openapi.FAILED:
		return dsFailedPrefix, nil
	default:
		return ds.Key{}, fmt.Errorf("invalid status: %s", status)
	}
}

func decode(v []byte) (*Request, error) {
	var buf bytes.Buffer
	buf.Write(v)
	dec := gob.NewDecoder(&buf)
	var req Request
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("decoding key value: %v", err)
	}
	return &req, nil
}
