package queue

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	c "github.com/ipfs/go-cid"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/pinning/openapi"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/net/util"
)

var log = logging.Logger("buckets/ps-queue")

const (
	// StartAfter is the pause before queue ticks starts.
	StartAfter = time.Second

	// InitialInterval is the interval for the first iteration of queue ticks.
	InitialInterval = time.Second

	// Interval is the interval between automatic queue ticks.
	Interval = time.Second * 10
)

var (
	dsPrefix = ds.NewKey("/pins")
	//dsName   = dsPrefix.ChildString("name")
)

type request struct {
	ID  string
	Cid string

	Thread   core.ID
	Key      string
	Identity did.Token
}

type queueLock string

func (l queueLock) Key() string {
	return string(l)
}

type Queue struct {
	lib   *buckets.Buckets
	store ds.TxnDatastore
	locks *util.SemaphorePool

	ctx    context.Context
	cancel context.CancelFunc
}

func NewQueue(store ds.TxnDatastore) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		store:  store,
		locks:  util.NewSemaphorePool(1),
		ctx:    ctx,
		cancel: cancel,
	}
	//go q.start()
	return q
}

func (q *Queue) Close() error {
	q.locks.Stop()
	q.cancel()
	return nil
}

func (q *Queue) AddRequest(thread core.ID, key string, status *openapi.PinStatus, identity did.Token) error {
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return err
	}
	defer txn.Discard()

	req := request{
		ID:       status.Requestid,
		Cid:      status.Pin.Cid,
		Thread:   thread,
		Key:      key,
		Identity: identity,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(req); err != nil {
		return err
	}

	// Add key value
	if err := txn.Put(dsPrefix.ChildString(req.ID), buf.Bytes()); err != nil {
		return err
	}

	// Add name "index"
	//if len(req.Pin.Name) != 0 {
	//	if err := txn.Put(dsName.ChildString(req.Pin.Name), []byte(req.Pin.Name)); err != nil {
	//		return err
	//	}
	//}

	return txn.Commit()
}

func (q *Queue) GetRequest(id string) (*request, error) {
	val, err := q.store.Get(dsPrefix.ChildString(id))
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.Write(val)
	dec := gob.NewDecoder(&buf)
	var req request
	if err := dec.Decode(&req); err != nil {
		return nil, err
	}
	return &req, nil
}

func (q *Queue) start() {
	select {
	case <-time.After(StartAfter):
	case <-q.ctx.Done():
		return
	}

	// Set interval into initial value,
	// it will be redefined on the next iteration
	var interval = InitialInterval

loop:
	for {
		queue, err := q.getQueuedRequests()
		if err != nil {
			log.Errorf("listing queued pins: %s", err)
			return
		}

		if len(queue) == 0 {
			// Wait and retry
			select {
			case <-time.After(interval):
				interval = Interval
				continue loop
			case <-q.ctx.Done():
				return
			}
		}

		var (
			period = interval / time.Duration(len(queue))
			ticker = time.NewTicker(period)
			idx    = 0
		)

		for {
			select {
			case <-ticker.C:
				go func(id string) {
					if err := q.pinRequest(id); err != nil {
						log.Errorf("pinning request %s: %s", id, err)
					}
				}(queue[idx])
				idx++
				if idx >= len(queue) {
					ticker.Stop()
					interval = Interval
					continue loop
				}

			case <-q.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}
}

func (q *Queue) getQueuedRequests() ([]string, error) {
	results, err := q.store.Query(query.Query{Prefix: dsPrefix.String(), KeysOnly: true})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var reqs []string
	for r := range results.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		reqs = append(reqs, strings.TrimPrefix(r.Key, dsPrefix.String()))
	}
	return reqs, nil
}

func (q *Queue) pinRequest(id string) error {
	lk := q.locks.Get(queueLock(id))
	if !lk.TryAcquire() {
		return nil
	}
	defer lk.Release()

	req, err := q.GetRequest(id)
	if err != nil {
		return err
	}
	cid, err := c.Decode(req.Cid)
	if err != nil {
		return err
	}

	log.Debugf("processing pin request: %s", id)

	ctx, cancel := context.WithTimeout(q.ctx, time.Hour)
	defer cancel()
	if _, _, err = q.lib.SetPath(ctx, req.Thread, req.Key, req.ID, cid, req.Identity); err != nil {
		return fmt.Errorf("setting path %s: %v", req.ID, err)
	}

	if err := q.delete(req.ID); err != nil {
		return fmt.Errorf("deleting request %s: %v", req.ID, err)
	}

	log.Debugf("pin request completed: %s", id)
	return nil
}

func (q *Queue) delete(id string) error {
	txn, err := q.store.NewTransaction(false)
	if err != nil {
		return err
	}
	defer txn.Discard()

	req, err := q.GetRequest(id)
	if err != nil {
		return err
	}

	// Delete "indexes"
	//if err := txn.Delete(dsName.ChildString(req.name)); err != nil {
	//	return err
	//}

	// Delete key value
	if err := txn.Delete(dsPrefix.ChildString(req.ID)); err != nil {
		return err
	}

	return txn.Commit()
}
