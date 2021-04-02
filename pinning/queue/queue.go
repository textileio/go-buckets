package queue

// @todo: Handle reload in-progress pins after shutdown
// @todo: Delete did.Token from request after success/fail
// @todo: Batch jobs by key, then handler can directly fetch cids with IPFS and use PushPaths to save bucket writes

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

	// dsGroupPrefix is the prefix for grouped time-ordered keys used to list requests.
	// Structure: /group/<groupkey>/<requestid>
	dsGroupPrefix = ds.NewKey("/group")
)

const (
	// defaultListLimit is the default request list page size.
	defaultListLimit = 10
	// maxListLimit is the max request list page size.
	maxListLimit = 1000
)

// Request is a wrapper for openapi.PinStatus that is persisted to the datastore.
type Request struct {
	openapi.PinStatus

	Thread   core.ID
	Key      string
	Identity did.Token

	// Replace indicates openapi.PinStatus.Pin.Cid is marked for replacement.
	Replace bool
	// Remove indicates the request is marked for removal.
	Remove bool
}

// RequestParams are used to create a new request.
type RequestParams struct {
	openapi.Pin

	// Time the request was received (used for openapi.PinStatus.Created).
	Time time.Time

	Thread   core.ID
	Key      string
	Identity did.Token
}

// Query is used to query for requests (a more typed version of openapi.Query).
type Query struct {
	Cids     []string
	Name     string
	Match    openapi.TextMatchingStrategy
	Statuses []openapi.Status
	Before   time.Time
	After    time.Time
	Limit    int
	Meta     map[string]string
}

func (q Query) setDefaults() Query {
	if q.Limit == -1 {
		q.Limit = maxListLimit
	} else if q.Limit <= 0 {
		q.Limit = defaultListLimit
	} else if q.Limit > maxListLimit {
		q.Limit = maxListLimit
	}
	if q.Meta == nil {
		q.Meta = make(map[string]string)
	}
	if len(q.Match) == 0 {
		q.Match = openapi.EXACT
	}
	return q
}

// Handler is called when a request moves from "queued" to "pinning".
// This separates the queue's job from the actual handling of a request, making the queue logic easier to test.
type Handler func(ctx context.Context, request Request) error

// Queue is a persistent worker-based task queue.
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

// NewQueue returns a new Queue using handler to process requests.
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

// Close the queue and cancel active "pinning" requests.
func (q *Queue) Close() error {
	q.cancel()
	return nil
}

// NewID returns new monotonically increasing request ids.
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

// cidFilter is used to query for one or more requests with a particular openapi.PinStatus.Pin.Cid.
type cidFilter struct {
	ok []string
}

func (f *cidFilter) Filter(e dsq.Entry) bool {
	r, err := decode(e.Value)
	if err != nil {
		log.Errorf("error decoding entry: %v", err)
		return false
	}
	for _, ok := range f.ok {
		if r.Pin.Cid == ok {
			return true
		}
	}
	return false
}

// nameFilter is used to query for one or more requests with a particular openapi.PinStatus.Pin.Name.
type nameFilter struct {
	name  string
	match openapi.TextMatchingStrategy
}

func (f *nameFilter) Filter(e dsq.Entry) bool {
	r, err := decode(e.Value)
	if err != nil {
		log.Errorf("error decoding entry: %v", err)
		return false
	}
	switch f.match {
	case openapi.EXACT:
		return r.Pin.Name == f.name
	case openapi.IEXACT:
		return strings.ToLower(r.Pin.Name) == strings.ToLower(f.name)
	case openapi.PARTIAL:
		return strings.Contains(r.Pin.Name, f.name)
	case openapi.IPARTIAL:
		return strings.Contains(strings.ToLower(r.Pin.Name), strings.ToLower(f.name))
	default:
		return false
	}
}

// statusFilter is used to query for one or more requests with a particular openapi.PinStatus.Status.
type statusFilter struct {
	ok []openapi.Status
}

func (f *statusFilter) Filter(e dsq.Entry) bool {
	r, err := decode(e.Value)
	if err != nil {
		log.Errorf("error decoding entry: %v", err)
		return false
	}
	for _, ok := range f.ok {
		if r.Status == ok {
			return true
		}
	}
	return false
}

// metaFilter is used to query for one or more requests with matching openapi.PinStatus.Pin.Meta.
type metaFilter struct {
	meta map[string]string
}

func (f *metaFilter) Filter(e dsq.Entry) bool {
	r, err := decode(e.Value)
	if err != nil {
		log.Errorf("error decoding entry: %v", err)
		return false
	}
	var match bool
loop:
	for fk, fv := range f.meta {
		for k, v := range r.Pin.Meta {
			if k == fk {
				if v == fv {
					match = true
					continue loop // So far so good, check next filter
				} else {
					return false // Values don't match, we're done
				}
			}
		}
		return false // Key not found, we're done
	}
	return match
}

// ListRequests lists requests for a group key by applying a Query.
func (q *Queue) ListRequests(group string, query Query) ([]openapi.PinStatus, error) {
	query = query.setDefaults()
	if !query.Before.IsZero() && !query.After.IsZero() {
		return nil, errors.New("before and after cannot be used together")
	}

	var (
		order         dsq.Order = dsq.OrderByKeyDescending{}
		seek, seekKey string
		filters       []dsq.Filter
		limit         = query.Limit
		err           error
	)

	if !query.After.IsZero() {
		order = dsq.OrderByKey{}
		seek, err = q.NewID(query.After.Add(time.Millisecond))
		if err != nil {
			return nil, fmt.Errorf("getting 'after' id: %v", err)
		}
	} else {
		if query.Before.IsZero() {
			seek = strings.ToLower(ulid.MustNew(ulid.MaxTime(), nil).String())
		} else {
			seek, err = q.NewID(query.Before.Add(-time.Millisecond))
			if err != nil {
				return nil, fmt.Errorf("getting 'before' id: %v", err)
			}
		}
	}
	seekKey = getGroupKey(group, seek).String()

	if len(query.Cids) > 0 {
		filters = append(filters, &cidFilter{
			ok: query.Cids,
		})
	}
	if len(query.Name) > 0 {
		filters = append(filters, &nameFilter{
			name:  query.Name,
			match: query.Match,
		})
	}
	if len(query.Statuses) > 0 {
		filters = append(filters, &statusFilter{
			ok: query.Statuses,
		})
	}
	if len(query.Meta) > 0 {
		filters = append(filters, &metaFilter{
			meta: query.Meta,
		})
	}

	results, err := q.store.QueryExtended(dsextensions.QueryExt{
		Query: dsq.Query{
			Prefix:  dsGroupPrefix.ChildString(group).String(),
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

// AddRequest adds a new request to the queue.
// The new request will be handled immediately if workers are not busy.
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

// GetRequest returns a request by group key and id.
func (q *Queue) GetRequest(group, id string) (*openapi.PinStatus, error) {
	r, err := q.getRequest(q.store, group, id)
	if err != nil {
		return nil, err
	}
	return &r.PinStatus, err
}

func (q *Queue) getRequest(reader ds.Read, group, id string) (*Request, error) {
	val, err := reader.Get(getGroupKey(group, id))
	if errors.Is(err, ds.ErrNotFound) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("getting group key: %v", err)
	}
	r, err := decode(val)
	if err != nil {
		return nil, fmt.Errorf("decoding request: %v", err)
	}
	return &r, nil
}

// ReplaceRequest replaces a request's openapi.PinStatus.Pin.
// Note: In-progress ("pinning") requests cannot be replaced.
func (q *Queue) ReplaceRequest(group, id string, pin openapi.Pin) (*openapi.PinStatus, error) {
	r, err := q.dequeue(group, id)
	if err != nil {
		return nil, fmt.Errorf("dequeueing request: %w", err)
	}

	// Mark for replacement
	r.Replace = true
	r.Pin = pin
	r.Status = openapi.QUEUED

	// Re-enqueue
	if err := q.enqueue(*r, true); err != nil {
		return nil, fmt.Errorf("re-enqueueing request: %v", err)
	}
	return &r.PinStatus, nil
}

// RemoveRequest removes a request.
// Note: In-progress ("pinning") requests cannot be removed.
func (q *Queue) RemoveRequest(group, id string) error {
	r, err := q.dequeue(group, id)
	if err != nil {
		return fmt.Errorf("dequeueing request: %w", err)
	}

	// Mark for removal
	r.Remove = true
	r.Status = openapi.QUEUED

	// Re-enqueue
	if err := q.enqueue(*r, true); err != nil {
		return fmt.Errorf("re-enqueueing request: %v", err)
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
		if err := q.store.Put(getGroupKey(r.Key, r.Requestid), val); err != nil {
			return fmt.Errorf("putting group key: %v", err)
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

func (q *Queue) dequeue(group, id string) (*Request, error) {
	txn, err := q.store.NewTransactionExtended(false)
	if err != nil {
		return nil, fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	// Check if pinning
	r, err := q.getRequest(txn, group, id)
	if errors.Is(err, ds.ErrNotFound) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("getting group key: %v", err)
	} else if r.Status == openapi.PINNING {
		return nil, ErrInProgress
	}

	// Remove queue key
	if err := txn.Delete(dsQueuePrefix.ChildString(id)); err != nil {
		return nil, fmt.Errorf("putting key: %v", err)
	}

	if err := txn.Commit(); err != nil {
		return nil, fmt.Errorf("committing txn: %v", err)
	}
	return r, nil
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

			if r.Remove {
				if err := q.removeRequest(r); err != nil {
					log.Debugf("error removing request: %v", err)
				}
			} else {
				// Finalize request by setting status to "pinned" or "failed"
				if err := q.moveRequest(r, openapi.PINNING, status); err != nil {
					log.Debugf("error updating status (%s): %v", status, err)
				}
			}

			log.Debugf("worker %d finished job %s", num, r.Requestid)
			go func() {
				q.doneCh <- struct{}{}
			}()
		}
	}
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

func (q *Queue) moveRequest(r Request, from, to openapi.Status) error {
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	// Update status
	r.Status = to

	// Reset 'replace' and 'remove' flags if the request is complete
	if to == openapi.PINNED || to == openapi.FAILED {
		r.Replace = false
		r.Remove = false // Only needed if the removal failed
	}

	val, err := encode(r)
	if err != nil {
		return fmt.Errorf("encoding group key: %v", err)
	}

	// Handle queue key
	if from == openapi.QUEUED {
		if err := txn.Delete(dsQueuePrefix.ChildString(r.Requestid)); err != nil {
			return fmt.Errorf("deleting key: %v", err)
		}
	}
	if to == openapi.QUEUED {
		if err := txn.Put(dsQueuePrefix.ChildString(r.Requestid), val); err != nil {
			return fmt.Errorf("putting key: %v", err)
		}
	}

	// Update group key
	if err := txn.Put(getGroupKey(r.Key, r.Requestid), val); err != nil {
		return fmt.Errorf("putting group key: %v", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing txn: %v", err)
	}
	return nil
}

func (q *Queue) removeRequest(r Request) error {
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return fmt.Errorf("creating txn: %v", err)
	}
	defer txn.Discard()

	// Remove queue key
	if err := txn.Delete(dsQueuePrefix.ChildString(r.Requestid)); err != nil {
		return fmt.Errorf("deleting queue key: %v", err)
	}

	// Remove group key
	if err := txn.Delete(getGroupKey(r.Key, r.Requestid)); err != nil {
		return fmt.Errorf("deleting group key: %v", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing txn: %v", err)
	}
	return nil
}

func getGroupKey(group, id string) ds.Key {
	return dsGroupPrefix.ChildString(group).ChildString(id)
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
