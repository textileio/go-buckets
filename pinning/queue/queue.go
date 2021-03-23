package queue

// @todo: Add doc strings
// @todo: Use badger v2

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/oklog/ulid/v2"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
	dsextensions "github.com/textileio/go-datastore-extensions"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	kt "github.com/textileio/go-threads/db/keytransform"
)

var (
	log = logging.Logger("buckets/ps-queue")

	// StartDelay is the time delay before the queue will process queued request on start.
	StartDelay = time.Second * 10

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

func NewID() string {
	return NewIDFromTime(time.Now())
}

func NewIDFromTime(t time.Time) string {
	return strings.ToLower(ulid.MustNew(ulid.Timestamp(t.UTC()), rand.Reader).String())
}

type Request struct {
	openapi.PinStatus

	Thread   core.ID
	Key      string
	Identity did.Token
}

type Query struct {
	Cid    []string                     // @todo
	Name   string                       // @todo
	Match  openapi.TextMatchingStrategy // @todo
	Status []openapi.Status
	Before string // ulid.ULID string
	After  string // ulid.ULID string
	Limit  int
	Meta   map[string]string // @todo
}

func (q Query) setDefaults() Query {
	if len(q.Status) == 0 {
		q.Status = []openapi.Status{openapi.PINNED}
	}
	if q.Limit == -1 {
		q.Limit = maxListLimit
	} else if q.Limit <= 0 {
		q.Limit = defaultListLimit
	} else if q.Limit > maxListLimit {
		q.Limit = maxListLimit
	}
	return q
}

type Handler func(ctx context.Context, request Request) error

type Queue struct {
	store kt.TxnDatastoreExtended

	handler Handler

	jobCh  chan Request
	doneCh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

func NewQueue(store kt.TxnDatastoreExtended, handler Handler) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		store:   store,
		handler: handler,
		jobCh:   make(chan Request, MaxConcurrency),
		doneCh:  make(chan struct{}, MaxConcurrency),
		ctx:     ctx,
		cancel:  cancel,
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

// ListRequests lists request for key by applying a Query.
// This is optimize for single-status queries (the default query is for only openapi.PINNED requests).
func (q *Queue) ListRequests(key string, query Query) ([]openapi.PinStatus, error) {
	query = query.setDefaults()
	if len(query.Before) != 0 && len(query.After) != 0 {
		return nil, fmt.Errorf("before and after cannot be used together")
	}
	var order dsq.Order = dsq.OrderByKey{}
	if len(query.Before) != 0 {
		order = dsq.OrderByKeyDescending{}
	}

	var all []openapi.PinStatus
	for _, s := range query.Status {
		var reqs []openapi.PinStatus

		pre, err := getStatusKeyPrefix(s)
		if err != nil {
			return nil, fmt.Errorf("getting status prefix: %v", err)
		}

		var (
			seek, seekKey string
			limit         = query.Limit
		)
		if len(query.Before) != 0 {
			seek = query.Before
		} else if len(query.After) != 0 {
			seek = query.After
		}
		if len(seek) != 0 {
			seekKey = getStatusKey(pre, key, seek).String()
			limit++ // Bump limit in case seek matches an element
		}

		results, err := q.store.QueryExtended(dsextensions.QueryExt{
			Query: dsq.Query{
				Prefix: pre.ChildString(key).String(),
				Orders: []dsq.Order{order},
				Limit:  limit,
			},
			SeekPrefix: seekKey,
		})
		if err != nil {
			return nil, fmt.Errorf("querying requests: %v", err)
		}

		for res := range results.Next() {
			if res.Error != nil {
				results.Close()
				return nil, fmt.Errorf("getting next result: %v", res.Error)
			}
			r, err := decode(res.Value)
			if err != nil {
				return nil, fmt.Errorf("decoding request: %v", err)
			}
			reqs = append(reqs, r.PinStatus)
		}
		results.Close()

		if len(seek) != 0 && len(reqs) > 0 {
			// Remove seek from tip if it matches the first record
			if seek == reqs[0].Requestid {
				reqs = reqs[1:]
			} else if len(reqs) == limit { // All elements are valid, remove the extra element
				reqs = reqs[:len(reqs)-1]
			}
		}

		all = append(all, reqs...)
	}

	// If the query contains multiple statuses, we have to sort and limit globally
	if len(query.Status) > 1 {
		switch order.(type) {
		case dsq.OrderByKeyDescending:
			sort.Slice(all, func(i, j int) bool {
				return all[i].Requestid > all[j].Requestid
			})
			if len(all) > query.Limit {
				all = all[len(all)-query.Limit:]
			}
		default:
			sort.Slice(all, func(i, j int) bool {
				return all[i].Requestid < all[j].Requestid
			})
			if len(all) > query.Limit {
				all = all[:query.Limit]
			}
		}
	}

	return all, nil
}

func (q *Queue) AddRequest(r Request) error {
	return q.enqueue(r, true)
}

func (q *Queue) GetRequest(key, id string) (*openapi.PinStatus, error) {
	statuses := []openapi.Status{
		openapi.QUEUED,
		openapi.PINNING,
		openapi.PINNED,
		openapi.FAILED,
	}
	for _, s := range statuses {
		pre, err := getStatusKeyPrefix(s)
		if err != nil {
			return nil, fmt.Errorf("getting status prefix: %v", err)
		}
		val, err := q.store.Get(getStatusKey(pre, key, id))
		if errors.Is(err, ds.ErrNotFound) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("getting status key: %v", err)
		}
		r, err := decode(val)
		if err != nil {
			return nil, fmt.Errorf("decoding request: %v", err)
		}
		return &r.PinStatus, nil
	}
	return nil, ds.ErrNotFound
}

func (q *Queue) RemoveRequest(key, id string) error {
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	// Delete from global queue
	if err := txn.Delete(dsQueuePrefix.ChildString(id)); err != nil {
		return fmt.Errorf("deleting key: %v", err)
	}

	// Remove from all possible status queues
	if err := txn.Delete(getStatusKey(dsQueuedStatusPrefix, key, id)); err != nil {
		return fmt.Errorf("deleting status key: %v", err)
	}
	if err := txn.Delete(getStatusKey(dsPinnedStatusPrefix, key, id)); err != nil {
		return fmt.Errorf("deleting status key: %v", err)
	}
	if err := txn.Delete(getStatusKey(dsFailedStatusPrefix, key, id)); err != nil {
		return fmt.Errorf("deleting status key: %v", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing txn: %v", err)
	}
	return nil
}

func (q *Queue) enqueue(r Request, isNew bool) error {
	// Block while the request is placed in a queue
	if isNew {
		r.Status = openapi.PINNING
		val, err := encode(r)
		if err != nil {
			return fmt.Errorf("encoding request: %v", err)
		}

		// Put the new request directly in the "pinning" queue in case a worker is available now
		if err := q.store.Put(getStatusKey(dsPinningStatusPrefix, r.Key, r.Requestid), val); err != nil {
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
			log.Debugf("workers are busy; queueing %s", r.Requestid)
			// Workers are busy, put back in the "queued" queue
			if err := q.moveRequest(r, openapi.PINNING, openapi.QUEUED); err != nil {
				log.Debugf("error updating status (queued): %v", err)
			}
		}
	}()
	return nil
}

func (q *Queue) start() {
	t := time.NewTimer(StartDelay)
	for {
		select {
		case <-q.ctx.Done():
			t.Stop()
			return
		case <-t.C:
			q.getNext()
		case <-q.doneCh:
			q.getNext()
		}
	}
}

func (q *Queue) getNext() {
	queue, err := q.getQueued()
	if err != nil {
		log.Errorf("listing requests: %v", err)
		return
	}
	if len(queue) > 0 {
		log.Debug("enqueueing job: %s", queue[0].Requestid)
	}
	for _, r := range queue {
		if err := q.enqueue(r, false); err != nil {
			log.Debugf("error enqueueing request: %v", err)
		}
	}
}

func (q *Queue) getQueued() ([]Request, error) {
	results, err := q.store.Query(dsq.Query{
		Prefix: dsQueuePrefix.String(),
		Orders: []dsq.Order{dsq.OrderByKey{}},
		Limit:  1,
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
		reqs = append(reqs, r)
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
			log.Debugf("worker %d got job %s", num, r.Requestid)

			// Handle the request with the handler func
			status := openapi.PINNED
			if err := q.handler(q.ctx, r); err != nil {
				status = openapi.FAILED
				log.Debugf("error handling request: %v", err)
			}

			// Finalize request by moving it to either the "pinned" or "failed" queue
			if err := q.moveRequest(r, openapi.PINNING, status); err != nil {
				log.Debugf("error updating status (%s): %v", status, err)
			}

			log.Debugf("worker %d finished job %s", num, r.Requestid)
			go func() {
				q.doneCh <- struct{}{}
			}()
		}
	}
}

func (q *Queue) moveRequest(r Request, from, to openapi.Status) error {
	fromPre, err := getStatusKeyPrefix(from)
	if err != nil {
		return fmt.Errorf("getting 'from' prefix: %v", err)
	}
	toPre, err := getStatusKeyPrefix(to)
	if err != nil {
		return fmt.Errorf("getting 'to' prefix: %v", err)
	}
	fromKey := getStatusKey(fromPre, r.Key, r.Requestid)
	toKey := getStatusKey(toPre, r.Key, r.Requestid)

	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	// Re-fetch in case it was removed by calling RemoveRequest
	val, err := q.store.Get(fromKey)
	if err != nil {
		return fmt.Errorf("getting status key: %v", err)
	}
	r, err = decode(val)
	if err != nil {
		return fmt.Errorf("decoding status key: %v", err)
	}
	r.Status = to
	val, err = encode(r)
	if err != nil {
		return fmt.Errorf("encoding status key: %v", err)
	}

	// Delete from global queue
	if from == openapi.QUEUED {
		if err := txn.Delete(dsQueuePrefix.ChildString(r.Requestid)); err != nil {
			return fmt.Errorf("deleting key: %v", err)
		}
	}
	// Add to global queue
	if to == openapi.QUEUED {
		if err := txn.Put(dsQueuePrefix.ChildString(r.Requestid), val); err != nil {
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

func encode(r Request) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return nil, fmt.Errorf("encoding request: %v", err)
	}
	return buf.Bytes(), nil
}

func decode(v []byte) (r Request, err error) {
	var buf bytes.Buffer
	if _, err := buf.Write(v); err != nil {
		return r, fmt.Errorf("writing key value: %v", err)
	}
	dec := gob.NewDecoder(&buf)
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Errorf("decoding key value: %v", err)
	}
	return r, nil
}
