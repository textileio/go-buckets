package pinning

// @todo: Add delegates field to responses
// @todo: Leverage origins when setting path
// @todo: Handle remaining query types
// @todo: Cleanup up embedded queue when bucket is mutated out-of-band?

import (
	"context"
	"encoding/json"
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

	statusTimeout = time.Minute
	pinTimeout    = time.Hour
)

// Service provides a bucket-based IPFS Pinning Service based on the OpenAPI spec:
// https://github.com/ipfs/pinning-services-api-spec
type Service struct {
	lib   *buckets.Buckets
	queue *q.Queue
}

// NewService returns a new Service.
func NewService(lib *buckets.Buckets, store kt.TxnDatastoreExtended) *Service {
	s := &Service{lib: lib}
	s.queue = q.NewQueue(store, s.handleRequest)
	return s
}

// Close the Service.
func (s *Service) Close() error {
	return s.queue.Close()
}

// ListPins returns a list of openapi.PinStatus matching the Query.
func (s *Service) ListPins(
	thread core.ID,
	key string,
	query q.Query,
	identity did.Token,
) ([]openapi.PinStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	buck, err := s.lib.Get(ctx, thread, key, identity)
	if err != nil {
		return nil, err
	}

	list, err := s.queue.ListRequests(key, query)
	if err != nil {
		return nil, fmt.Errorf("listing requests: %v", err)
	}

	var stats []openapi.PinStatus
	for _, r := range list {
		status, err := getBucketStatus(buck, &r)
		if err != nil {
			return nil, err
		}
		stats = append(stats, *status)
	}

	log.Debugf("listed %d requests in %s", len(stats), key)
	return stats, nil
}

// AddPin adds an openapi.Pin to a bucket.
func (s *Service) AddPin(
	thread core.ID,
	key string,
	pin openapi.Pin,
	identity did.Token,
) (*openapi.PinStatus, error) {
	// Verify pin cid
	if _, err := c.Decode(pin.Cid); err != nil {
		return nil, fmt.Errorf("decoding pin cid: %v", err)
	}

	status := &openapi.PinStatus{
		Requestid: q.NewID(),
		Status:    openapi.QUEUED,
		Created:   time.Now(),
		Pin:       pin,
	}

	// Push a placeholder to the bucket
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	if _, err := s.lib.PushPath(
		ctx,
		thread,
		key,
		identity,
		nil,
		buckets.PushPathsInput{
			Path:   status.Requestid,
			Reader: strings.NewReader(fmt.Sprintf("pin %s in progress", pin.Cid)),
			Meta: map[string]interface{}{
				"pin": status,
			},
		},
	); err != nil {
		return nil, fmt.Errorf("pushing status to bucket: %v", err)
	}

	// Enqueue request
	if err := s.queue.AddRequest(q.Request{
		ID:       status.Requestid,
		Cid:      status.Pin.Cid,
		Thread:   thread,
		Key:      key,
		Identity: identity,
	}); err != nil {
		return nil, fmt.Errorf("adding request: %v", err)
	}

	log.Debugf("added request %s in %s", status.Requestid, key)
	return status, nil
}

// handleRequest attempts to pin the request Cid to the bucket path (request ID).
// Note: The openapi.PinStatus held in the bucket won't be set to "pinning"
// because we have that info in the embedded queue and can therefore avoid an extra bucket write.
// In other words, the bucket state will show "queued", "pinned", or "failed", but not "pinning".
func (s *Service) handleRequest(ctx context.Context, r q.Request) error {
	log.Debugf("processing request: %s", r.ID)

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

	cid, err := c.Decode(r.Cid)
	if err != nil {
		return fail(fmt.Errorf("decoding cid: %v", err))
	}

	// Replace placeholder with requested Cid and set status to "pinned"
	pctx, pcancel := context.WithTimeout(ctx, pinTimeout)
	defer pcancel()
	if _, _, err := s.lib.SetPath(
		pctx,
		r.Thread,
		r.Key,
		r.Identity,
		nil,
		r.ID,
		cid,
		map[string]interface{}{
			"pin": map[string]interface{}{
				"status": openapi.PINNED,
			},
		},
	); err != nil {
		return fail(fmt.Errorf("setting path %s: %v", r.ID, err))
	}

	log.Debugf("request completed: %s", r.ID)
	return nil
}

// failRequest updates the bucket path (request ID) with an error.
func (s *Service) failRequest(ctx context.Context, txn *buckets.Txn, r q.Request, reason error) error {
	log.Debugf("failing request: %s", r.ID)

	// Update placeholder with status "failed"
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	if _, err := txn.PushPath(
		ctx,
		nil,
		buckets.PushPathsInput{
			Path:   r.ID,
			Reader: strings.NewReader(fmt.Sprintf("pin %s failed: %v", r.Cid, reason)),
			Meta: map[string]interface{}{
				"pin": map[string]interface{}{
					"status": openapi.FAILED,
				},
			},
		},
	); err != nil {
		return fmt.Errorf("pushing status to bucket: %v", err)
	}

	log.Debugf("request failed: %s", r.ID)
	return nil
}

// GetPin returns an openapi.PinStatus.
func (s *Service) GetPin(
	thread core.ID,
	key string,
	id string,
	identity did.Token,
) (*openapi.PinStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	status, err := s.getPin(ctx, thread, key, id, identity)
	if err != nil {
		return nil, err
	}

	log.Debugf("got request %s in %s", id, key)
	return status, nil
}

func (s *Service) getPin(
	ctx context.Context,
	thread core.ID,
	key string,
	id string,
	identity did.Token,
) (*openapi.PinStatus, error) {
	buck, err := s.lib.Get(ctx, thread, key, identity)
	if err != nil {
		return nil, err
	}

	r, err := s.queue.GetRequest(key, id)
	if errors.Is(err, ds.ErrNotFound) {
		return nil, ErrPinNotFound
	} else if err != nil {
		return nil, err
	}

	return getBucketStatus(buck, r)
}

// ReplacePin replaces an openapi.PinStatus with another.
func (s *Service) ReplacePin(
	thread core.ID,
	key string,
	id string,
	pin openapi.Pin,
	identity did.Token,
) (*openapi.PinStatus, error) {
	// Verify pin cid
	if _, err := c.Decode(pin.Cid); err != nil {
		return nil, fmt.Errorf("decoding pin cid: %v", err)
	}

	// Ensure bucket is readable
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	if _, err := s.getPin(ctx, thread, key, id, identity); err != nil {
		return nil, err
	}

	status := &openapi.PinStatus{
		Requestid: id,
		Status:    openapi.QUEUED,
		Created:   time.Now(),
		Pin:       pin,
	}

	// Push a new placeholder to the bucket. This will block if the request is pinning.
	if _, err := s.lib.PushPath(
		ctx,
		thread,
		key,
		identity,
		nil,
		buckets.PushPathsInput{
			Path:   status.Requestid,
			Reader: strings.NewReader(fmt.Sprintf("pin %s in progress", pin.Cid)),
			Meta: map[string]interface{}{
				"pin": status,
			},
		},
	); err != nil {
		return nil, fmt.Errorf("pushing new status to bucket: %v", err)
	}

	// Remove from queues.
	// Note: If the request was pinning when ReplacePin was called, it has now completed
	// and the embedded queue moved it to its final home (pinned/failed).
	// Technically, there's now a race between that final move and the following
	// RemoveRequest. In practice, PushPath will "always" take longer then
	// that final move operation, meaning the final move operation will "always" win.
	// In any case, if RemoveRequest wins, the final move operation fails and carries on
	// without issue. Mentioning here for clarity.
	if err := s.queue.RemoveRequest(key, id); err != nil {
		return nil, fmt.Errorf("removing request: %v", err)
	}

	// Re-enqueue request
	if err := s.queue.AddRequest(q.Request{
		ID:       status.Requestid,
		Cid:      status.Pin.Cid,
		Thread:   thread,
		Key:      key,
		Identity: identity,
	}); err != nil {
		return nil, fmt.Errorf("adding request: %v", err)
	}

	log.Debugf("replaced request %s in %s", id, key)
	return nil, nil
}

// RemovePin removes an openapi.PinStatus from a bucket.
func (s *Service) RemovePin(
	thread core.ID,
	key string,
	id string,
	identity did.Token,
) error {
	// Remove from bucket. This will block if the request is pinning.
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
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

func getBucketStatus(b *buckets.Bucket, s *q.Status) (*openapi.PinStatus, error) {
	// Cross-check results from the embedded queue with bucket state,
	// since it may have been mutated out-of-band.
	md, ok := b.Metadata[s.ID].Info["pin"]
	if ok {
		status, err := statusFromMeta(md)
		if err != nil {
			return nil, err
		}
		// The request may be pinning but this won't be reflected in the bucket state.
		// Ssee s.handleRequest for details.
		status.Status = s.Status
		return status, nil
	}
	return nil, ErrPinNotFound
}

func statusFromMeta(m interface{}) (*openapi.PinStatus, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshalling request: %v", err)
	}
	var status openapi.PinStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("unmarshalling request: %v", err)
	}
	return &status, nil
}
