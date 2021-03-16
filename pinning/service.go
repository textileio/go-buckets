package pinning

import (
	"context"
	"fmt"
	"strings"
	"time"

	c "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/pinning/openapi"
	q "github.com/textileio/go-buckets/pinning/queue"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

var (
	log = logging.Logger("buckets/ps")

	statusTimeout = time.Minute
	pinTimeout    = time.Hour
)

type Service struct {
	lib   *buckets.Buckets
	queue *q.Queue
}

func NewService(lib *buckets.Buckets, store ds.TxnDatastore) *Service {
	s := &Service{lib: lib}
	s.queue = q.NewQueue(store, s.handleRequest, s.failRequest)
	return s
}

func (s *Service) Close() error {
	return s.queue.Close()
}

func (s *Service) AddPin(
	thread core.ID,
	key string,
	pin openapi.Pin,
	identity did.Token,
) (*openapi.PinStatus, error) {
	status := &openapi.PinStatus{
		Requestid: s.queue.NewID(),
		Created:   time.Now(),
		Status:    openapi.QUEUED,
		Pin:       pin,
	}

	// Push a placeholder with status "queued"
	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()
	if _, err := s.lib.PushPath(
		ctx,
		thread,
		key,
		nil,
		buckets.PushPathsInput{
			Path:   status.Requestid,
			Reader: strings.NewReader(string(status.Status)),
			Meta: map[string]interface{}{
				"pin": status,
			},
		},
		identity,
	); err != nil {
		return nil, fmt.Errorf("pushing status to bucket: %v", err)
	}

	if err := s.queue.AddRequest(thread, key, status, identity); err != nil {
		return nil, fmt.Errorf("adding request: %v", err)
	}

	log.Debugf("added request: %s", status.Requestid)
	return status, nil
}

func (s *Service) handleRequest(ctx context.Context, r *q.Request) error {
	log.Debugf("processing request: %s", r.ID)

	cid, err := c.Decode(r.Cid)
	if err != nil {
		return fmt.Errorf("decoding cid: %v", err)
	}

	// Update placeholder with status "pinning"
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	if _, err := s.lib.PushPath(
		ctx,
		r.Thread,
		r.Key,
		nil,
		buckets.PushPathsInput{
			Path:   r.ID,
			Reader: strings.NewReader(string(openapi.PINNING)),
			Meta: map[string]interface{}{
				"pin": map[string]interface{}{"status": openapi.PINNING},
			},
		},
		r.Identity,
	); err != nil {
		return fmt.Errorf("pushing status to bucket: %v", err)
	}

	// Replace placeholder with requested Cid and set status to "pinned"
	pctx, pcancel := context.WithTimeout(ctx, pinTimeout)
	defer pcancel()
	if _, _, err := s.lib.SetPath(
		pctx,
		r.Thread,
		r.Key,
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

func (s *Service) failRequest(ctx context.Context, r *q.Request) error {
	log.Debugf("failing request: %s", r.ID)

	// Update placeholder with status "failed"
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	if _, err := s.lib.PushPath(
		ctx,
		r.Thread,
		r.Key,
		nil,
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

//// GetPinsQuery represents Pin query parameters.
//type GetPinsQuery struct {
//	// Cid can be used to filter by one or more Pin Cids.
//	Cid []string `form:"cid" json:"cid,omitempty"`
//	// Name can be used to filer by Pin name (by default case-sensitive, exact match).
//	Name string `form:"name" json:"name,omitempty"`
//	// Match can be used to customize the text matching strategy applied when Name is present.
//	Match string `form:"match" json:"match,omitempty"`
//	// Status can be used to filter by Pin status.
//	Status []string `form:"status" json:"status,omitempty"`
//	// Before can by used to filter by before creation (queued) time.
//	Before *time.Time `form:"before" json:"before,omitempty"`
//	// After can by used to filter by after creation (queued) time.
//	After *time.Time `form:"after" json:"after,omitempty"`
//	// Limit specifies the max number of Pins to return.
//	Limit *int32 `form:"limit" json:"limit,omitempty"`
//	// Meta can be used to filter results by Pin metadata.
//	Meta *map[string]string `form:"meta" json:"meta,omitempty"`
//}
