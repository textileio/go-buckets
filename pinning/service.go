package pinning

// @todo: Handle remaining query types
// @todo: Add delegates field to responses
// @todo: Leverage origins when setting path

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	c "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/dag"
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
	s.queue = q.NewQueue(store, s.handleRequest, s.failRequest)
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

	ids, err := s.queue.ListRequests(key, query)
	if err != nil {
		return nil, fmt.Errorf("listing requests: %v", err)
	}

	var stats []openapi.PinStatus
	for _, r := range ids {
		md, ok := buck.Metadata[r].Info["pin"]
		if ok {
			status, err := statusFromMeta(md)
			if err != nil {
				return nil, err
			}
			stats = append(stats, *status)
		}
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

	// Fetch latest bucket root.
	// @todo: Cache bucket roots to avoid this lookup when possible
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	buck, err := s.lib.Get(ctx, thread, key, identity)
	if err != nil {
		return nil, err
	}
	root, err := dag.NewResolvedPath(buck.Path)
	if err != nil {
		return nil, fmt.Errorf("resolving bucket path: %v", err)
	}

	// Push a placeholder with status "queued"
	sctx, scancel := context.WithTimeout(context.Background(), statusTimeout)
	defer scancel()
	res, err := s.lib.PushPath(
		sctx,
		thread,
		key,
		root,
		buckets.PushPathsInput{
			Path:   status.Requestid,
			Reader: strings.NewReader(string(status.Status)),
			Meta: map[string]interface{}{
				"pin": status,
			},
		},
		identity,
	)
	if err != nil {
		return nil, fmt.Errorf("pushing status to bucket: %v", err)
	}
	root = res.Path

	// Enqueue request
	if err := s.queue.AddRequest(q.Request{
		ID:       status.Requestid,
		Cid:      status.Pin.Cid,
		Thread:   thread,
		Key:      key,
		Root:     root,
		Identity: identity,
	}); err != nil {
		return nil, fmt.Errorf("adding request: %v", err)
	}

	log.Debugf("added request %s in %s", status.Requestid, key)
	return status, nil
}

func (s *Service) handleRequest(ctx context.Context, r q.Request) error {
	log.Debugf("processing request: %s", r.ID)

	cid, err := c.Decode(r.Cid)
	if err != nil {
		return fmt.Errorf("decoding cid: %v", err)
	}

	// Update placeholder with status "pinning"
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	res, err := s.lib.PushPath(
		ctx,
		r.Thread,
		r.Key,
		r.Root,
		buckets.PushPathsInput{
			Path:   r.ID,
			Reader: strings.NewReader(string(openapi.PINNING)),
			Meta: map[string]interface{}{
				"pin": map[string]interface{}{"status": openapi.PINNING},
			},
		},
		r.Identity,
	)
	if err != nil {
		return fmt.Errorf("pushing status to bucket: %v", err)
	}
	r.Root = res.Path

	// Replace placeholder with requested Cid and set status to "pinned"
	pctx, pcancel := context.WithTimeout(ctx, pinTimeout)
	defer pcancel()
	if _, _, err := s.lib.SetPath(
		pctx,
		r.Thread,
		r.Key,
		r.Root,
		r.ID,
		cid,
		map[string]interface{}{
			"pin": map[string]interface{}{"status": openapi.PINNED},
		},
		r.Identity,
	); err != nil {
		return fmt.Errorf("setting path %s: %v", r.ID, err)
	}

	log.Debugf("request completed: %s", r.ID)
	return nil
}

func (s *Service) failRequest(ctx context.Context, r q.Request) error {
	log.Debugf("failing request: %s", r.ID)

	// Update placeholder with status "failed"
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	if _, err := s.lib.PushPath(
		ctx,
		r.Thread,
		r.Key,
		r.Root,
		buckets.PushPathsInput{
			Path:   r.ID,
			Reader: strings.NewReader(string(openapi.FAILED)),
			Meta: map[string]interface{}{
				"pin": map[string]interface{}{"status": openapi.FAILED},
			},
		},
		r.Identity,
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
	buck, err := s.lib.Get(ctx, thread, key, identity)
	if err != nil {
		return nil, err
	}

	var status *openapi.PinStatus
	md, ok := buck.Metadata[id].Info["pin"]
	if ok {
		status, err = statusFromMeta(md)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrPinNotFound
	}

	log.Debugf("got request %s in %s", id, key)
	return status, nil
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

	// Fetch latest bucket root.
	// @todo: Cache bucket roots to avoid this lookup when possible
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	buck, err := s.lib.Get(ctx, thread, key, identity)
	if err != nil {
		return nil, err
	}
	root, err := dag.NewResolvedPath(buck.Path)
	if err != nil {
		return nil, fmt.Errorf("resolving bucket path: %v", err)
	}

	if _, ok := buck.Metadata[id].Info["pin"]; !ok {
		return nil, ErrPinNotFound
	}

	// Update pin object in bucket
	buck, err = s.lib.PushPathInfo(
		ctx,
		thread,
		key,
		root,
		id,
		map[string]interface{}{
			"pin": map[string]interface{}{
				"status": openapi.QUEUED,
				"pin":    pin,
			},
		},
		identity,
	)
	if err != nil {
		return nil, fmt.Errorf("pushing new status to bucket: %v", err)
	}

	var status *openapi.PinStatus
	md, ok := buck.Metadata[id].Info["pin"]
	if ok {
		status, err = statusFromMeta(md)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrPinNotFound
	}

	// Remove from queues
	if err := s.queue.RemoveRequest(key, id); err != nil {
		return nil, fmt.Errorf("removing request: %v", err)
	}

	// Re-enqueue request
	if err := s.queue.AddRequest(q.Request{
		ID:       status.Requestid,
		Cid:      status.Pin.Cid,
		Thread:   thread,
		Key:      key,
		Root:     root,
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
	// Fetch latest bucket root.
	// @todo: Cache bucket roots to avoid this lookup when possible
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	buck, err := s.lib.Get(ctx, thread, key, identity)
	if err != nil {
		return err
	}
	root, err := dag.NewResolvedPath(buck.Path)
	if err != nil {
		return fmt.Errorf("resolving bucket path: %v", err)
	}

	if _, ok := buck.Metadata[id].Info["pin"]; !ok {
		return ErrPinNotFound
	}

	// Remove from queues
	if err := s.queue.RemoveRequest(key, id); err != nil {
		return fmt.Errorf("removing request: %v", err)
	}

	// Remove from bucket
	if _, _, err := s.lib.RemovePath(
		ctx,
		thread,
		key,
		root,
		id,
		identity,
	); err != nil {
		return fmt.Errorf("removing path %s: %v", id, err)
	}

	log.Debugf("removed request %s in %s", id, key)
	return nil
}
