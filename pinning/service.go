package pinning

// @todo: Add delegates field to responses
// @todo: Leverage origins when setting path
// @todo: Handle remaining query types

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	c "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/textileio/go-buckets"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
	q "github.com/textileio/go-buckets/pinning/queue"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	kt "github.com/textileio/go-threads/db/keytransform"
)

var (
	log = logging.Logger("buckets/ps")

	// ErrPinNotFound a pin was not found.
	ErrPinNotFound = errors.New("pin not found")

	// PinTimeout is the max time taken to pin a Cid.
	PinTimeout = time.Hour

	statusTimeout = time.Minute
)

// Service provides a bucket-based IPFS Pinning Service based on the OpenAPI spec:
// https://github.com/ipfs/pinning-services-api-spec
type Service struct {
	lib   *buckets.Buckets
	queue *q.Queue
}

// NewService returns a new pinning Service.
func NewService(lib *buckets.Buckets, store kt.TxnDatastoreExtended) (*Service, error) {
	s := &Service{lib: lib}
	queue, err := q.NewQueue(store, s.handleRequest)
	if err != nil {
		return nil, fmt.Errorf("creating queue: %v", err)
	}
	s.queue = queue
	return s, nil
}

// Close the Service.
func (s *Service) Close() error {
	return s.queue.Close()
}

// ListPins returns a list of openapi.PinStatus matching the Query.
func (s *Service) ListPins(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	query q.Query,
) ([]openapi.PinStatus, error) {
	// Ensure bucket is readable by identity
	if _, err := s.lib.Get(ctx, thread, key, identity); err != nil {
		return nil, err
	}

	list, err := s.queue.ListRequests(key, query)
	if err != nil {
		return nil, fmt.Errorf("listing requests: %v", err)
	}

	log.Debugf("listed %d requests in %s", len(list), key)
	return list, nil
}

// AddPin adds an openapi.Pin to a bucket.
func (s *Service) AddPin(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pin openapi.Pin,
) (*openapi.PinStatus, error) {
	// Verify pin cid
	if _, err := c.Decode(pin.Cid); err != nil {
		return nil, fmt.Errorf("decoding pin cid: %v", err)
	}

	// Enqueue request
	r, err := s.queue.AddRequest(q.RequestParams{
		Pin:      pin,
		Time:     time.Now(),
		Thread:   thread,
		Key:      key,
		Identity: identity,
	})
	if err != nil {
		return nil, fmt.Errorf("adding request: %v", err)
	}

	log.Debugf("added request %s in %s", r.Requestid, key)
	return r, nil
}

// handleRequest attempts to pin the request Cid to the bucket path (request ID).
// Note: The openapi.PinStatus held in the bucket won't be set to "pinning"
// because we have that info in the embedded queue and can therefore avoid an extra bucket write.
// In other words, the bucket state will show "queued", "pinned", or "failed", but not "pinning".
func (s *Service) handleRequest(ctx context.Context, r q.Request) error {
	log.Debugf("handling request: %s", r.Requestid)

	// Open a transaction that we can use to control blocking since we may need
	// to push a failure to the bucket if SetPath fails.
	txn := s.lib.NewTxn(r.Thread, r.Key, r.Identity)
	defer txn.Close()

	fail := func(reason error) error {
		if err := s.failRequest(ctx, txn, r, reason); err != nil {
			log.Debugf("failing request: %v", err)
		}
		return reason
	}

	cid, err := c.Decode(r.Pin.Cid)
	if err != nil {
		return fail(fmt.Errorf("decoding cid: %v", err))
	}

	// Replace placeholder with requested Cid and set status to "pinned"
	pctx, pcancel := context.WithTimeout(ctx, PinTimeout)
	defer pcancel()
	if _, _, err := txn.SetPath(
		pctx,
		nil,
		r.Requestid,
		cid,
		map[string]interface{}{
			"pin": map[string]interface{}{
				"status": openapi.PINNED,
			},
		},
	); err != nil {
		// @todo: Skip fail handler if path not found
		return fail(fmt.Errorf("setting path %s: %v", r.Requestid, err))
	}

	log.Debugf("request completed: %s", r.Requestid)
	return nil
}

// failRequest updates the bucket path (request ID) with an error.
func (s *Service) failRequest(ctx context.Context, txn *buckets.Txn, r q.Request, reason error) error {
	log.Debugf("failing request %s with reason: %v", r.Requestid, reason)

	// Update placeholder with status "failed"
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	if _, err := txn.PushPath(
		ctx,
		nil,
		buckets.PushPathsInput{
			Path:   r.Requestid,
			Reader: strings.NewReader(fmt.Sprintf("pin %s failed: %v", r.Pin.Cid, reason)),
			Meta: map[string]interface{}{
				"pin": map[string]interface{}{
					"status": openapi.FAILED,
				},
			},
		},
	); err != nil {
		return fmt.Errorf("pushing status to bucket: %v", err)
	}

	log.Debugf("request failed: %s", r.Requestid)
	return nil
}

// GetPin returns an openapi.PinStatus.
func (s *Service) GetPin(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	id string,
) (*openapi.PinStatus, error) {
	// Ensure bucket is readable by identity
	if _, err := s.lib.Get(ctx, thread, key, identity); err != nil {
		return nil, err
	}

	r, err := s.queue.GetRequest(key, id)
	if errors.Is(err, ds.ErrNotFound) {
		return nil, ErrPinNotFound
	} else if err != nil {
		return nil, err
	}

	log.Debugf("got request %s in %s", id, key)
	return r, nil
}

// ReplacePin replaces an openapi.PinStatus with another.
func (s *Service) ReplacePin(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	id string,
	pin openapi.Pin,
) (*openapi.PinStatus, error) {
	// Verify pin cid
	if _, err := c.Decode(pin.Cid); err != nil {
		return nil, fmt.Errorf("decoding pin cid: %v", err)
	}

	//status := openapi.PinStatus{
	//	Requestid: id,
	//	Status:    openapi.QUEUED,
	//	Pin:       pin,
	//}

	// Remove from queues.
	if err := s.queue.RemoveRequest(key, id); err != nil {
		return nil, fmt.Errorf("removing request: %v", err)
	}

	//// Re-enqueue request
	//r, err := s.queue.AddRequest(q.RequestParams{
	//	Pin: pin,
	//	Time: time.Now(),
	//}thread, key, identity, pin, time.Now())
	//if err != nil {
	//	return nil, fmt.Errorf("adding request: %v", err)
	//}

	log.Debugf("replaced request %s in %s", id, key)
	return nil, nil
}

// RemovePin removes an openapi.PinStatus from a bucket.
func (s *Service) RemovePin(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	id string,
) error {
	// Remove from bucket. This will block if the request is pinning.
	if _, _, err := s.lib.RemovePath(
		ctx,
		thread,
		key,
		identity,
		nil,
		id,
	); err != nil {
		return fmt.Errorf("removing path %s: %v", id, err)
	}

	// Remove from queues
	if err := s.queue.RemoveRequest(key, id); err != nil {
		return fmt.Errorf("removing request: %v", err)
	}

	log.Debugf("removed request %s in %s", id, key)
	return nil
}
