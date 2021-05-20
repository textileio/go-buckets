package ipns

import (
	"context"
	"errors"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p-core/peer"
	mbase "github.com/multiformats/go-multibase"
	s "github.com/textileio/go-buckets/ipns/store"
	"github.com/textileio/go-threads/core/thread"
	tutil "github.com/textileio/go-threads/util"
)

var log = logging.Logger("buckets/ipns")

const (
	// publishTimeout
	publishTimeout = time.Minute * 2
	// maxCancelPublishTries is the number of time cancelling a publish is allowed to fail.
	maxCancelPublishTries = 10
)

// Manager handles bucket name publishing to IPNS.
type Manager struct {
	store   *s.Store
	keyAPI  iface.KeyAPI
	nameAPI iface.NameAPI

	keyLocks map[string]chan struct{}
	ctxsLock sync.Mutex
	ctxs     map[string]context.CancelFunc
	sync.Mutex
}

// NewManager returns a new IPNS manager.
func NewManager(store ds.TxnDatastore, ipfs iface.CoreAPI) (*Manager, error) {
	return &Manager{
		store:    s.NewStore(store),
		keyAPI:   ipfs.Key(),
		nameAPI:  ipfs.Name(),
		ctxs:     make(map[string]context.CancelFunc),
		keyLocks: make(map[string]chan struct{}),
	}, nil
}

// Store returns the key store.
func (m *Manager) Store() *s.Store {
	return m.store
}

// CreateKey generates and saves a new IPNS key.
func (m *Manager) CreateKey(ctx context.Context, dbID thread.ID) (keyID string, err error) {
	key, err := m.keyAPI.Generate(ctx, tutil.MakeToken(), options.Key.Type(options.Ed25519Key))
	if err != nil {
		return
	}
	keyID, err = peer.ToCid(key.ID()).StringOfBase(mbase.Base36)
	if err != nil {
		return
	}
	if err = m.store.Create(key.Name(), keyID, dbID); err != nil {
		return
	}
	return keyID, nil
}

// RemoveKey removes an IPNS key.
func (m *Manager) RemoveKey(ctx context.Context, keyID string) error {
	key, err := m.store.GetByCid(keyID)
	if err != nil {
		return err
	}
	if _, err = m.keyAPI.Remove(ctx, key.Name); err != nil {
		return err
	}
	return m.store.Delete(key.Name)
}

// Publish publishes a path to IPNS with key ID.
// Publishing can take up to a minute. Pending publishes are cancelled by consecutive
// calls with the same key ID, which results in only the most recent publish succeeding.
func (m *Manager) Publish(pth path.Path, keyID string) {
	ptl := m.getSemaphore(keyID)
	try := 0
	for {
		select {
		case ptl <- struct{}{}:
			pctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
			m.ctxsLock.Lock()
			m.ctxs[keyID] = cancel
			m.ctxsLock.Unlock()
			if err := m.publishUnsafe(pctx, pth, keyID); err != nil {
				if !errors.Is(err, context.Canceled) {
					// Logging as a warning because this often fails with "context deadline exceeded",
					// even if the entry can be found on the network (not fully saturated).
					log.Warnf("error publishing path %s: %v", pth, err)
				} else {
					log.Debugf("publishing path %s was cancelled: %v", pth, err)
				}
			}
			cancel()
			m.ctxsLock.Lock()
			delete(m.ctxs, keyID)
			m.ctxsLock.Unlock()
			<-ptl
			return
		default:
			m.ctxsLock.Lock()
			cancel, ok := m.ctxs[keyID]
			m.ctxsLock.Unlock()
			if ok {
				cancel()
			} else {
				try++
				if try > maxCancelPublishTries {
					log.Warnf("failed to publish path %s: max tries exceeded", pth)
					return
				} else {
					log.Debugf("failed to cancel publish (%v tries remaining)", maxCancelPublishTries-try)
				}
			}
		}
	}
}

// Close all pending publishes.
func (m *Manager) Close() error {
	m.Lock()
	defer m.Unlock()
	m.ctxsLock.Lock()
	defer m.ctxsLock.Unlock()
	for _, cancel := range m.ctxs {
		cancel()
	}
	return nil
}

func (m *Manager) publishUnsafe(ctx context.Context, pth path.Path, keyID string) error {
	key, err := m.store.GetByCid(keyID)
	if err != nil {
		return err
	}
	entry, err := m.nameAPI.Publish(ctx, pth, options.Name.Key(key.Name))
	if err != nil {
		return err
	}
	log.Debugf("published %s => %s", entry.Value(), entry.Name())
	return nil
}

func (m *Manager) getSemaphore(key string) chan struct{} {
	var ptl chan struct{}
	var ok bool
	m.Lock()
	defer m.Unlock()
	if ptl, ok = m.keyLocks[key]; !ok {
		ptl = make(chan struct{}, 1)
		m.keyLocks[key] = ptl
	}
	return ptl
}
