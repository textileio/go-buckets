package buckets

// @todo: Clean up error messages

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	c "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-buckets/dns"
	"github.com/textileio/go-buckets/ipns"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	nc "github.com/textileio/go-threads/net/api/client"
	"github.com/textileio/go-threads/net/util"
	nutil "github.com/textileio/go-threads/net/util"
)

var (
	log = logging.Logger("buckets")

	// GatewayURL is used to construct externally facing bucket links.
	GatewayURL string

	// ThreadsGatewayURL is used to construct externally facing bucket links.
	ThreadsGatewayURL string

	// WWWDomain can be set to specify the domain to use for bucket website hosting, e.g.,
	// if this is set to mydomain.com, buckets can be rendered as a website at the following URL:
	//   https://<bucket_key>.mydomain.com
	WWWDomain string

	// ErrNonFastForward is returned when an update in non-fast-forward.
	ErrNonFastForward = errors.New("update is non-fast-forward")

	movePathRegexp = regexp.MustCompile("/ipfs/([^/]+)/")
)

// IsPathNotFoundErr returns whether or not an error indicates a bucket path was not found.
// This is needed because public and private (encrypted) buckets can return different
// errors for the same operation.
func IsPathNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "could not resolve path") ||
		strings.Contains(msg, "no link named")
}

// Bucket adds thread ID to collection.Bucket.
type Bucket struct {
	Thread core.ID `json:"thread"`
	collection.Bucket
}

// PathItem describes a file or directory in a bucket.
type PathItem struct {
	Cid        string              `json:"cid"`
	Name       string              `json:"name"`
	Path       string              `json:"path"`
	Size       int64               `json:"size"`
	IsDir      bool                `json:"is_dir"`
	Items      []PathItem          `json:"items"`
	ItemsCount int32               `json:"items_count"`
	Metadata   collection.Metadata `json:"metadata"`
}

// Links wraps links for resolving a bucket with various protocols.
type Links struct {
	// URL is the thread URL, which maps to a ThreadDB collection instance.
	URL string `json:"url"`
	// WWW is the URL at which the bucket will be rendered as a website (requires remote DNS configuration).
	WWW string `json:"www"`
	// IPNS is the bucket IPNS address.
	IPNS string `json:"ipns"`
	// BPS is the bucket pinning service URL.
	BPS string `json:"bps"`
}

// Seed describes a bucket seed file.
type Seed struct {
	Cid  c.Cid
	Data []byte
}

// Buckets is an object storage library built on Threads, IPFS, and IPNS.
type Buckets struct {
	net *nc.Client
	db  *dbc.Client
	c   *collection.Buckets

	ipfs iface.CoreAPI
	ipns *ipns.Manager
	dns  *dns.Manager

	locks *nutil.SemaphorePool
}

var _ nutil.SemaphoreKey = (*lock)(nil)

type lock string

func (l lock) Key() string {
	return string(l)
}

// Txn allows for holding a bucket lock while performing multiple write operations.
type Txn struct {
	b        *Buckets
	thread   core.ID
	key      string
	identity did.Token
	lock     *util.Semaphore
}

// Close the Txn, releasing the bucket for additional writes.
func (t *Txn) Close() error {
	t.lock.Release()
	return nil
}

// NewBuckets returns a new buckets library.
func NewBuckets(
	net *nc.Client,
	db *dbc.Client,
	ipfs iface.CoreAPI,
	ipns *ipns.Manager,
	dns *dns.Manager,
) (*Buckets, error) {
	bc, err := collection.NewBuckets(db)
	if err != nil {
		return nil, fmt.Errorf("getting buckets collection: %v", err)
	}
	return &Buckets{
		net:   net,
		db:    db,
		c:     bc,
		ipfs:  ipfs,
		ipns:  ipns,
		dns:   dns,
		locks: nutil.NewSemaphorePool(1),
	}, nil
}

// Close it down.
func (b *Buckets) Close() error {
	b.locks.Stop()
	return nil
}

// Net returns the underlying thread net client.
func (b *Buckets) Net() *nc.Client {
	return b.net
}

// DB returns the underlying thread db client.
func (b *Buckets) DB() *dbc.Client {
	return b.db
}

// Ipfs returns the underlying IPFS client.
func (b *Buckets) Ipfs() iface.CoreAPI {
	return b.ipfs
}

// NewTxn returns a new Txn for bucket key.
func (b *Buckets) NewTxn(thread core.ID, key string, identity did.Token) (*Txn, error) {
	if err := thread.Validate(); err != nil {
		return nil, fmt.Errorf("invalid thread id: %v", err)
	}
	lk := b.locks.Get(lock(key))
	lk.Acquire()
	return &Txn{
		b:        b,
		thread:   thread,
		key:      key,
		identity: identity,
		lock:     lk,
	}, nil
}

// Get returns a bucket by thread id and key.
func (b *Buckets) Get(ctx context.Context, thread core.ID, key string, identity did.Token) (*Bucket, error) {
	if err := thread.Validate(); err != nil {
		return nil, fmt.Errorf("invalid thread id: %v", err)
	}
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return nil, err
	}
	log.Debugf("got %s", key)
	return instanceToBucket(thread, instance), nil
}

// GetLinks returns a Links object containing the bucket thread, IPNS, and WWW links by thread and bucket key.
func (b *Buckets) GetLinks(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pth string,
) (links Links, err error) {
	if err := thread.Validate(); err != nil {
		return links, fmt.Errorf("invalid thread id: %v", err)
	}
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return links, err
	}
	log.Debugf("got %s links", key)
	return b.GetLinksForBucket(ctx, instanceToBucket(thread, instance), identity, pth)
}

// GetLinksForBucket returns a Links object containing the bucket thread, IPNS, and WWW links.
func (b *Buckets) GetLinksForBucket(
	ctx context.Context,
	bucket *Bucket,
	identity did.Token,
	pth string,
) (links Links, err error) {
	links.URL = fmt.Sprintf("%s/thread/%s/%s/%s", ThreadsGatewayURL, bucket.Thread, collection.Name, bucket.Key)
	if len(WWWDomain) != 0 {
		parts := strings.Split(GatewayURL, "://")
		if len(parts) < 2 {
			return links, fmt.Errorf("failed to parse gateway URL: %s", GatewayURL)
		}
		links.WWW = fmt.Sprintf("%s://%s.%s", parts[0], bucket.Key, WWWDomain)
	}
	links.IPNS = fmt.Sprintf("%s/ipns/%s", GatewayURL, bucket.Key)

	pth = trimSlash(pth)
	if _, _, ok := bucket.GetMetadataForPath(pth, false); !ok {
		return links, fmt.Errorf("could not resolve path: %s", pth)
	}
	if len(pth) != 0 {
		npth, err := getBucketPath(&bucket.Bucket, pth)
		if err != nil {
			return links, err
		}
		linkKey := bucket.GetLinkEncryptionKey()
		if _, err := dag.GetNodeAtPath(ctx, b.ipfs, npth, linkKey); err != nil {
			return links, fmt.Errorf("could not resolve path: %s", pth)
		}
		pth = "/" + pth
		links.URL += pth
		if len(links.WWW) != 0 {
			links.WWW += pth
		}
		links.IPNS += pth
	} else {
		links.BPS = fmt.Sprintf("%s/bps/%s", GatewayURL, bucket.Key)
	}

	query := "?token=" + string(identity)
	links.URL += query
	if len(links.WWW) != 0 {
		links.WWW += query
	}
	links.IPNS += query

	return links, nil
}

// List returns all buckets under a thread.
func (b *Buckets) List(ctx context.Context, thread core.ID, identity did.Token) ([]Bucket, error) {
	if err := thread.Validate(); err != nil {
		return nil, fmt.Errorf("invalid thread id: %v", err)
	}
	list, err := b.c.List(ctx, thread, &db.Query{}, &collection.Bucket{}, collection.WithIdentity(identity))
	if err != nil {
		return nil, fmt.Errorf("listing buckets: %v", err)
	}
	instances := list.([]*collection.Bucket)
	bucks := make([]Bucket, len(instances))
	for i, in := range instances {
		bucket := instanceToBucket(thread, in)
		bucks[i] = *bucket
	}

	log.Debugf("listed all in %s", thread)
	return bucks, nil
}

// Remove removes an entire bucket, unpinning all data.
func (b *Buckets) Remove(ctx context.Context, thread core.ID, key string, identity did.Token) (int64, error) {
	txn, err := b.NewTxn(thread, key, identity)
	if err != nil {
		return 0, err
	}
	defer txn.Close()
	return txn.Remove(ctx)
}

// Remove is Txn based Remove.
func (t *Txn) Remove(ctx context.Context) (int64, error) {
	instance, err := t.b.c.GetSafe(ctx, t.thread, t.key, collection.WithIdentity(t.identity))
	if err != nil {
		return 0, err
	}
	if err := t.b.c.Delete(ctx, t.thread, t.key, collection.WithIdentity(t.identity)); err != nil {
		return 0, fmt.Errorf("deleting bucket: %v", err)
	}

	buckPath, err := dag.NewResolvedPath(instance.Path)
	if err != nil {
		return 0, fmt.Errorf("resolving path: %v", err)
	}
	linkKey := instance.GetLinkEncryptionKey()
	if linkKey != nil {
		ctx, err = dag.UnpinNodeAndBranch(ctx, t.b.ipfs, buckPath, linkKey)
		if err != nil {
			return 0, err
		}
	} else {
		ctx, err = dag.UnpinPath(ctx, t.b.ipfs, buckPath)
		if err != nil {
			return 0, err
		}
	}
	if err := t.b.ipns.RemoveKey(ctx, t.key); err != nil {
		return 0, err
	}

	log.Debugf("removed %s", t.key)
	return dag.GetPinnedBytes(ctx), nil
}

func (b *Buckets) saveAndPublish(
	ctx context.Context,
	thread core.ID,
	identity did.Token,
	instance *collection.Bucket,
) error {
	if err := b.c.Save(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
		return fmt.Errorf("saving bucket: %v", err)
	}
	go b.ipns.Publish(path.New(instance.Path), instance.Key)
	return nil
}

func instanceToBucket(thread core.ID, instance *collection.Bucket) *Bucket {
	return &Bucket{
		Thread: thread,
		Bucket: *instance,
	}
}
