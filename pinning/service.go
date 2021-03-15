package pinning

import (
	"context"
	"fmt"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/pinning/openapi"
	q "github.com/textileio/go-buckets/pinning/queue"
	"github.com/textileio/go-buckets/util"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

var log = logging.Logger("buckets/ps")

type Service struct {
	lib   *buckets.Buckets
	queue *q.Queue
}

func NewService(lib *buckets.Buckets, store ds.TxnDatastore) *Service {
	return &Service{
		lib:   lib,
		queue: q.NewQueue(store),
	}
}

func (s *Service) Close() error {
	return s.queue.Close()
}

func (s *Service) AddPin(
	ctx context.Context,
	thread core.ID,
	key string,
	pin openapi.Pin,
	identity did.Token,
) (*openapi.PinStatus, error) {
	status := &openapi.PinStatus{
		Requestid: util.MakeToken(20),
		Created:   time.Now(),
		Status:    openapi.QUEUED,
		Pin:       pin,
	}

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
		return nil, fmt.Errorf("pushing pin status to bucket: %v", err)
	}

	if err := s.queue.AddRequest(thread, key, status, identity); err != nil {
		return nil, fmt.Errorf("enqueueing pin request: %v", err)
	}

	log.Debugf("added pin request: %s", status.Requestid)
	return status, nil
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
