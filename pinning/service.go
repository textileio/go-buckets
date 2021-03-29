package pinning

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	c "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/libp2p/go-libp2p-core/peer"
	maddr "github.com/multiformats/go-multiaddr"
	"github.com/textileio/go-buckets"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
	q "github.com/textileio/go-buckets/pinning/queue"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	kt "github.com/textileio/go-threads/db/keytransform"
)

var (
	log = logging.Logger("buckets/ps")

	// ErrPermissionDenied indicates an identity does not have the required athorization.
	ErrPermissionDenied = errors.New("permission denied")

	// PinTimeout is the max time taken to pin a Cid.
	PinTimeout = time.Hour

	statusTimeout = time.Minute

	connectTimeout = time.Second * 10
)

// Service provides a bucket-based IPFS Pinning Service based on the OpenAPI spec:
// https://github.com/ipfs/pinning-services-api-spec
type Service struct {
	lib   *buckets.Buckets
	queue *q.Queue
	addrs []string
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
	if ok, err := s.lib.IsReadablePath(ctx, thread, key, identity, ""); err != nil {
		return nil, fmt.Errorf("authenticating read: %v", err)
	} else if !ok {
		return nil, ErrPermissionDenied
	}

	list, err := s.queue.ListRequests(key, query)
	if err != nil {
		return nil, fmt.Errorf("listing requests: %v", err)
	}

	delegates, err := s.getDelegates()
	if err != nil {
		return nil, fmt.Errorf("getting delegates: %v", err)
	}
	for _, r := range list {
		r.Delegates = delegates
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
	// Ensure bucket is writable by identity
	if ok, err := s.lib.IsWritablePath(ctx, thread, key, identity, ""); err != nil {
		return nil, fmt.Errorf("authenticating write: %v", err)
	} else if !ok {
		return nil, ErrPermissionDenied
	}

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

	r.Delegates, err = s.getDelegates()
	if err != nil {
		return nil, fmt.Errorf("getting delegates: %v", err)
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
	txn, err := s.lib.NewTxn(r.Thread, r.Key, r.Identity)
	if err != nil {
		return err
	}
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

	if r.Remove {
		// Remove path from the bucket
		ctx, cancel := context.WithTimeout(ctx, statusTimeout)
		defer cancel()
		if _, _, err := txn.RemovePath(ctx, nil, r.Requestid); err != nil {
			return fail(fmt.Errorf("removing path %s: %v", r.Requestid, err))
		}
	} else {
		pctx, pcancel := context.WithTimeout(ctx, PinTimeout)
		defer pcancel()

		// Connect to Pin.Origins
		go s.connectOrigins(pctx, r.Pin.Origins)

		// Replace placeholder at path with openapi.Pin.Cid
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
		); buckets.IsPathNotFoundErr(err) {
			return fmt.Errorf("setting path %s: %v", r.Requestid, err)
		} else if err != nil {
			return fail(fmt.Errorf("setting path %s: %v", r.Requestid, err))
		}
	}

	log.Debugf("request completed: %s", r.Requestid)
	return nil
}

// failRequest updates the bucket path (request ID) with an error.
func (s *Service) failRequest(ctx context.Context, txn *buckets.Txn, r q.Request, reason error) error {
	log.Debugf("failing request %s with reason: %v", r.Requestid, reason)

	// Update placeholder with failure reason
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
	if ok, err := s.lib.IsReadablePath(ctx, thread, key, identity, ""); err != nil {
		return nil, fmt.Errorf("authenticating read: %v", err)
	} else if !ok {
		return nil, ErrPermissionDenied
	}

	r, err := s.queue.GetRequest(key, id)
	if err != nil {
		return nil, fmt.Errorf("getting request: %w", err)
	}

	r.Delegates, err = s.getDelegates()
	if err != nil {
		return nil, fmt.Errorf("getting delegates: %v", err)
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
	// Ensure bucket is writable by identity
	if ok, err := s.lib.IsWritablePath(ctx, thread, key, identity, ""); err != nil {
		return nil, fmt.Errorf("authenticating write: %v", err)
	} else if !ok {
		return nil, ErrPermissionDenied
	}

	// Verify pin cid
	if _, err := c.Decode(pin.Cid); err != nil {
		return nil, fmt.Errorf("decoding pin cid: %v", err)
	}

	r, err := s.queue.ReplaceRequest(key, id, pin)
	if err != nil {
		return nil, fmt.Errorf("replacing request: %w", err)
	}

	r.Delegates, err = s.getDelegates()
	if err != nil {
		return nil, fmt.Errorf("getting delegates: %v", err)
	}

	log.Debugf("replaced request %s in %s", id, key)
	return r, nil
}

// RemovePin removes an openapi.PinStatus from a bucket.
func (s *Service) RemovePin(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	id string,
) error {
	// Ensure bucket is writable by identity
	if ok, err := s.lib.IsWritablePath(ctx, thread, key, identity, ""); err != nil {
		return fmt.Errorf("authenticating write: %v", err)
	} else if !ok {
		return ErrPermissionDenied
	}

	if err := s.queue.RemoveRequest(key, id); err != nil {
		return fmt.Errorf("removing request: %w", err)
	}

	log.Debugf("removed request %s in %s", id, key)
	return nil
}

func (s *Service) connectOrigins(ctx context.Context, origins []string) {
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, o := range origins {
		wg.Add(1)
		go func(o string) {
			defer wg.Done()
			pinfo, err := peerInfoFromOrigin(o)
			if err != nil {
				log.Errorf("error %v", err)
				return
			}
			if err := s.lib.Ipfs().Swarm().Connect(ctx, *pinfo); err != nil {
				log.Debugf("error connecting to %s: %v", pinfo.ID, err)
			} else {
				log.Debugf("connected to %s", pinfo.ID)
			}
		}(o)
	}
	wg.Wait()
}

func peerInfoFromOrigin(origin string) (*peer.AddrInfo, error) {
	addr, err := maddr.NewMultiaddr(origin)
	if err != nil {
		return nil, fmt.Errorf("decoding origin: %v", err)
	}
	parts := maddr.Split(addr)
	if len(parts) == 0 {
		return nil, errors.New("origin has zero components")
	}
	var p2p maddr.Multiaddr
	p2p, parts = parts[len(parts)-1], parts[:len(parts)-1]
	p2pv, err := p2p.ValueForProtocol(maddr.P_P2P)
	if err != nil {
		return nil, fmt.Errorf("origin missing p2p component: %v", err)
	}
	pid, err := peer.Decode(p2pv)
	if err != nil {
		return nil, fmt.Errorf("getting peer id: %v", err)
	}
	return &peer.AddrInfo{
		ID: pid,
		Addrs: []maddr.Multiaddr{
			maddr.Join(parts...),
		},
	}, nil
}

func (s *Service) getDelegates() ([]string, error) {
	if len(s.addrs) != 0 {
		return s.addrs, nil
	}
	addrs, err := GetLocalAddrs(s.lib.Ipfs())
	if err != nil {
		return nil, fmt.Errorf("getting ipfs local addrs: %v", err)
	}
	for _, a := range addrs {
		addr := a.String()
		// if !strings.Contains(addr, "localhost") && !strings.Contains(addr, "127.0.0.1") {
		s.addrs = append(s.addrs, addr)
		// }
	}
	return s.addrs, nil
}

func GetLocalAddrs(ipfs iface.CoreAPI) ([]maddr.Multiaddr, error) {
	key, err := ipfs.Key().Self(context.Background())
	if err != nil {
		return nil, err
	}
	paddr, err := maddr.NewMultiaddr("/p2p/" + key.ID().String())
	if err != nil {
		return nil, err
	}
	addrs, err := ipfs.Swarm().LocalAddrs(context.Background())
	if err != nil {
		return nil, err
	}
	paddrs := make([]maddr.Multiaddr, len(addrs))
	for i, a := range addrs {
		paddrs[i] = a.Encapsulate(paddr)
	}
	return paddrs, nil
}
