package queue

// @todo: Add doc strings
// @todo: Use badger v2
// @todo: Batch jobs by key, then handler can directly fetch cids and use PushPaths to save bucket writes

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	c "github.com/ipfs/go-cid"
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

	// ErrNotFound indicates the requested request was not found.
	ErrNotFound = errors.New("request not found")

	// ErrInProgress indicates the request is in progress and cannot be altered.
	ErrInProgress = errors.New("request in progress")

	// dsQueuePrefix is the prefix for global time-ordered keys used internally for processing.
	// Structure: /queue/<requestid>
	dsQueuePrefix = ds.NewKey("/queue")

	// dsBucketPrefix is the prefix for bucket-grouped time-ordered keys used to list requests.
	// Structure: /bucket/<bucketkey>/<requestid>/<status>
	dsBucketPrefix = ds.NewKey("/bucket")
)

const (
	defaultListLimit = 10
	maxListLimit     = 1000
)

type Request struct {
	openapi.PinStatus

	Thread   core.ID
	Key      string
	Identity did.Token
}

type RequestParams struct {
	openapi.Pin
	Time time.Time

	Thread   core.ID
	Key      string
	Identity did.Token
}

type Query struct {
	Cid      []c.Cid                      // @todo
	Name     string                       // @todo
	Match    openapi.TextMatchingStrategy // @todo
	Statuses []openapi.Status
	Before   time.Time
	After    time.Time
	Limit    int
	Meta     map[string]string // @todo
}

func (q Query) setDefaults() Query {
	if len(q.Statuses) == 0 {
		q.Statuses = []openapi.Status{openapi.PINNED}
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
	jobCh   chan Request
	doneCh  chan struct{}
	entropy *ulid.MonotonicEntropy

	ctx    context.Context
	cancel context.CancelFunc

	lk sync.Mutex
}

func NewQueue(store kt.TxnDatastoreExtended, handler Handler) (*Queue, error) {
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
	return q, nil
}

func (q *Queue) Close() error {
	q.cancel()
	return nil
}

func (q *Queue) NewID(t time.Time) (string, error) {
	q.lk.Lock() // entropy is not safe for concurrent use

	if q.entropy == nil {
		q.entropy = ulid.Monotonic(rand.Reader, 0)
	}
	id, err := ulid.New(ulid.Timestamp(t.UTC()), q.entropy)
	if errors.Is(err, ulid.ErrMonotonicOverflow) {
		q.entropy = nil
		q.lk.Unlock()
		return q.NewID(t)
	} else if err != nil {
		q.lk.Unlock()
		return "", fmt.Errorf("generating requestid: %v", err)
	}
	q.lk.Unlock()
	return strings.ToLower(id.String()), nil
}

type statusFilter struct {
	valid []openapi.Status
}

func (f *statusFilter) Filter(e dsq.Entry) bool {
	for _, s := range f.valid {
		if strings.HasSuffix(e.Key, string(s)) {
			return true
		}
	}
	return false
}

// ListRequests lists requests for key by applying a Query.
func (q *Queue) ListRequests(key string, query Query) ([]openapi.PinStatus, error) {
	query = query.setDefaults()
	if !query.Before.IsZero() && !query.After.IsZero() {
		return nil, fmt.Errorf("before and after cannot be used together")
	}
	var order dsq.Order = dsq.OrderByKey{}
	if !query.Before.IsZero() {
		order = dsq.OrderByKeyDescending{}
	}

	var (
		seek, seekKey string
		filters       []dsq.Filter
		limit         = query.Limit
		err           error
	)

	if !query.Before.IsZero() {
		seek, err = q.NewID(query.Before)
		if err != nil {
			return nil, fmt.Errorf("getting 'before' id: %v", err)
		}
	} else if !query.After.IsZero() {
		// Bump up to next second since 'after' has been rounded down due to limited resolution
		seek, err = q.NewID(query.After.Add(time.Second))
		if err != nil {
			return nil, fmt.Errorf("getting 'before' id: %v", err)
		}
	}
	if len(seek) != 0 {
		seekKey = getBucketKey(key, seek).String()
	}
	if len(query.Statuses) > 0 && len(query.Statuses) < 4 {
		filters = append(filters, &statusFilter{
			valid: query.Statuses,
		})
	}

	results, err := q.store.QueryExtended(dsextensions.QueryExt{
		Query: dsq.Query{
			Prefix:  dsBucketPrefix.ChildString(key).String(),
			Filters: filters,
			Orders:  []dsq.Order{order},
			Limit:   limit,
		},
		SeekPrefix: seekKey,
	})
	if err != nil {
		return nil, fmt.Errorf("querying requests: %v", err)
	}
	defer results.Close()

	var reqs []openapi.PinStatus
	for res := range results.Next() {
		if res.Error != nil {
			return nil, fmt.Errorf("getting next result: %v", res.Error)
		}
		r, err := decode(res.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding request: %v", err)
		}
		reqs = append(reqs, r.PinStatus)
	}

	return reqs, nil
}

func (q *Queue) AddRequest(params RequestParams) (*openapi.PinStatus, error) {
	id, err := q.NewID(params.Time)
	if err != nil {
		return nil, fmt.Errorf("creating request id: %v", err)
	}
	r := Request{
		PinStatus: openapi.PinStatus{
			Requestid: id,
			Status:    openapi.QUEUED,
			Created:   params.Time,
			Pin:       params.Pin,
		},
		Thread:   params.Thread,
		Key:      params.Key,
		Identity: params.Identity,
	}

	if err := q.enqueue(r, true); err != nil {
		return nil, fmt.Errorf("enqueueing request: %v", err)
	}
	return &r.PinStatus, nil
}

func (q *Queue) GetRequest(key, id string) (*openapi.PinStatus, error) {
	results, err := q.store.Query(dsq.Query{
		Prefix: getBucketKey(key, id).String(),
		Limit:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("finding request: %v", err)
	}
	defer results.Close()
	for res := range results.Next() {
		if res.Error != nil {
			return nil, fmt.Errorf("getting next result: %v", res.Error)
		}
		r, err := decode(res.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding request: %v", err)
		}
		return &r.PinStatus, nil
	}
	return nil, ErrNotFound
}

func (q *Queue) RemoveRequest(key, id string) error {
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	// Check if pinning
	bk := getBucketKey(key, id)
	if _, err := txn.Get(bk.ChildString(string(openapi.PINNING))); err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return fmt.Errorf("getting bucket key: %v", err)
		}
	} else {
		return ErrInProgress
	}

	// Remove queue key
	if err := txn.Delete(dsQueuePrefix.ChildString(id)); err != nil {
		return fmt.Errorf("deleting queue key: %v", err)
	}

	// Remove all possible bucket keys
	if err := txn.Delete(bk.ChildString(string(openapi.QUEUED))); err != nil {
		return fmt.Errorf("deleting bucket key (queued): %v", err)
	}
	if err := txn.Delete(bk.ChildString(string(openapi.PINNED))); err != nil {
		return fmt.Errorf("deleting bucket key (pinned): %v", err)
	}
	if err := txn.Delete(bk.ChildString(string(openapi.FAILED))); err != nil {
		return fmt.Errorf("deleting bucket key (failed): %v", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing txn: %v", err)
	}
	return nil
}

func (q *Queue) enqueue(r Request, isNew bool) error {
	// Block while the request is placed in a queue
	if isNew {
		// Set to "pinning" in case a worker is available now
		r.Status = openapi.PINNING
		val, err := encode(r)
		if err != nil {
			return fmt.Errorf("encoding request: %v", err)
		}
		bk := getBucketKey(r.Key, r.Requestid)
		if err := q.store.Put(bk.ChildString(string(openapi.PINNING)), val); err != nil {
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
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	fromKey := getBucketKey(r.Key, r.Requestid).ChildString(string(from))
	toKey := getBucketKey(r.Key, r.Requestid).ChildString(string(to))

	// Re-fetch in case 'from' was removed by calling RemoveRequest
	val, err := txn.Get(fromKey)
	if err != nil {
		return fmt.Errorf("getting bucket key: %v", err)
	}
	r, err = decode(val)
	if err != nil {
		return fmt.Errorf("decoding bucket key: %v", err)
	}
	r.Status = to
	val, err = encode(r)
	if err != nil {
		return fmt.Errorf("encoding bucket key: %v", err)
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

	// Migrate key to new status
	if err := txn.Put(toKey, val); err != nil {
		return fmt.Errorf("putting bucket key: %v", err)
	}
	if err := txn.Delete(fromKey); err != nil {
		return fmt.Errorf("deleting bucket key: %v", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing txn: %v", err)
	}
	return nil
}

func getBucketKey(key, id string) ds.Key {
	return dsBucketPrefix.ChildString(key).ChildString(id)
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
