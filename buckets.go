package buckets

// @todo: Validate all thread IDs
// @todo: Validate all identities
// @todo: Clean up error messages
// @todo: Enforce fast-forward-only in SetPath and MovePath, PushPathAccessRoles

import (
	"context"
	"errors"
	"fmt"
	"io"
	gopath "path"
	"regexp"
	"strings"
	"sync"
	"time"

	c "github.com/ipfs/go-cid"
	ipfsfiles "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/ipfs/interface-go-ipfs-core"
	ifaceopts "github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-buckets/dns"
	"github.com/textileio/go-buckets/ipns"
	"github.com/textileio/go-buckets/util"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
	nc "github.com/textileio/go-threads/net/api/client"
	nutil "github.com/textileio/go-threads/net/util"
)

var (
	log = logging.Logger("buckets")

	// GatewayURL is used to construct externally facing bucket links.
	GatewayURL string

	// WWWDomain can be set to specify the domain to use for bucket website hosting, e.g.,
	// if this is set to mydomain.com, buckets can be rendered as a website at the following URL:
	//   https://<bucket_key>.mydomain.com
	WWWDomain string

	// ErrNonFastForward is returned when an update in non-fast-forward.
	ErrNonFastForward = errors.New("update is non-fast-forward")

	movePathRegexp = regexp.MustCompile("/ipfs/([^/]+)/")
)

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

func (b *Buckets) Net() *nc.Client {
	return b.net
}

func (b *Buckets) DB() *dbc.Client {
	return b.db
}

func (b *Buckets) Create(
	ctx context.Context,
	identity did.Token,
	opts ...CreateOption,
) (*Bucket, *Seed, int64, error) {
	args := &CreateOptions{}
	for _, opt := range opts {
		opt(args)
	}

	if args.Thread.Defined() {
		if err := args.Thread.Validate(); err != nil {
			return nil, nil, 0, fmt.Errorf("invalid thread id: %v", err)
		}
	} else {
		args.Thread = core.NewRandomIDV1()
		if err := b.db.NewDB(ctx, args.Thread, db.WithNewManagedName(args.Name)); err != nil {
			return nil, nil, 0, fmt.Errorf("creating new thread: %v", err)
		}
	}

	_, owner, err := b.net.ValidateIdentity(ctx, identity)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("validating identity: %v", err)
	}

	// Create bucket keys if private
	var linkKey, fileKey []byte
	if args.Private {
		var err error
		linkKey, err = dcrypto.NewKey()
		if err != nil {
			return nil, nil, 0, err
		}
		fileKey, err = dcrypto.NewKey()
		if err != nil {
			return nil, nil, 0, err
		}
	}

	// Make a random seed, which ensures a bucket's uniqueness
	seed, err := dag.MakeBucketSeed(fileKey)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("making bucket seed: %v", err)
	}

	// Create the bucket directory
	var pth path.Resolved
	if args.Cid.Defined() {
		ctx, pth, err = dag.CreateBucketPathWithCid(
			ctx,
			b.ipfs,
			"",
			args.Cid,
			linkKey,
			fileKey,
			seed,
		)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("creating bucket with cid: %v", err)
		}
	} else {
		ctx, pth, err = dag.CreateBucketPath(ctx, b.ipfs, seed, linkKey)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("creating bucket: %v", err)
		}
	}

	// Create top-level metadata
	now := time.Now()
	md := map[string]collection.Metadata{
		"": collection.NewDefaultMetadata(owner, fileKey, now),
		collection.SeedName: {
			Roles:     make(map[did.DID]collection.Role),
			UpdatedAt: now.UnixNano(),
		},
	}

	// Create a new IPNS key
	key, err := b.ipns.CreateKey(ctx, args.Thread)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("creating IPNS key: %v", err)
	}

	// Create the bucket using the IPNS key as instance ID
	instance, err := b.c.New(
		ctx,
		args.Thread,
		key,
		owner,
		pth,
		now,
		md,
		identity,
		collection.WithBucketName(args.Name),
		collection.WithBucketKey(linkKey),
	)
	if err != nil {
		return nil, nil, 0, err
	}

	seedInfo := &Seed{
		Cid: seed.Cid(),
	}
	if instance.IsPrivate() {
		fileKey, err := instance.GetFileEncryptionKeyForPath("")
		if err != nil {
			return nil, nil, 0, err
		}
		data, err := dag.DecryptData(seed.RawData(), fileKey)
		if err != nil {
			return nil, nil, 0, err
		}
		seedInfo.Data = data
	} else {
		seedInfo.Data = seed.RawData()
	}

	// Publish the new bucket's address to the name system
	go b.ipns.Publish(pth, instance.Key)

	log.Debugf("created %s", key)
	return instanceToBucket(args.Thread, instance), seedInfo, dag.GetPinnedBytes(ctx), nil
}

func (b *Buckets) Get(ctx context.Context, thread core.ID, key string, identity did.Token) (*Bucket, error) {
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return nil, err
	}
	log.Debugf("got %s", key)
	return instanceToBucket(thread, instance), nil
}

func (b *Buckets) GetLinks(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	identity did.Token,
) (links Links, err error) {
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return links, err
	}
	log.Debugf("got %s links", key)
	return b.GetLinksForBucket(ctx, instanceToBucket(thread, instance), pth, identity)
}

func (b *Buckets) GetLinksForBucket(
	ctx context.Context,
	bucket *Bucket,
	pth string,
	identity did.Token,
) (links Links, err error) {
	links.URL = fmt.Sprintf("%s/thread/%s/%s/%s", GatewayURL, bucket.Thread, collection.Name, bucket.Key)
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
			return links, err
		}
		pth = "/" + pth
		links.URL += pth
		if len(links.WWW) != 0 {
			links.WWW += pth
		}
		links.IPNS += pth
	}
	if bucket.IsPrivate() {
		query := "?token=" + string(identity)
		links.URL += query
		if len(links.WWW) != 0 {
			links.WWW += query
		}
		links.IPNS += query
	}
	return links, nil
}

func (b *Buckets) List(ctx context.Context, thread core.ID, identity did.Token) ([]Bucket, error) {
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

func (b *Buckets) Remove(ctx context.Context, thread core.ID, key string, identity did.Token) (int64, error) {
	lk := b.locks.Get(lock(key))
	lk.Acquire()
	defer lk.Release()

	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return 0, err
	}
	if err := b.c.Delete(ctx, thread, key, collection.WithIdentity(identity)); err != nil {
		return 0, fmt.Errorf("deleting bucket: %v", err)
	}

	buckPath, err := util.NewResolvedPath(instance.Path)
	if err != nil {
		return 0, fmt.Errorf("resolving path: %v", err)
	}
	linkKey := instance.GetLinkEncryptionKey()
	if linkKey != nil {
		ctx, err = dag.UnpinNodeAndBranch(ctx, b.ipfs, buckPath, linkKey)
		if err != nil {
			return 0, err
		}
	} else {
		ctx, err = dag.UnpinPath(ctx, b.ipfs, buckPath)
		if err != nil {
			return 0, err
		}
	}
	if err := b.ipns.RemoveKey(ctx, key); err != nil {
		return 0, err
	}

	log.Debugf("removed %s", key)
	return dag.GetPinnedBytes(ctx), nil
}

func (b *Buckets) ListPath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	identity did.Token,
) (*PathItem, *Bucket, error) {
	pth = trimSlash(pth)
	instance, bpth, err := b.getBucketAndPath(ctx, thread, key, pth, identity)
	if err != nil {
		return nil, nil, err
	}
	item, err := b.listPath(ctx, instance, bpth)
	if err != nil {
		return nil, nil, err
	}
	log.Debugf("listed %s in %s", pth, key)
	return item, instanceToBucket(thread, instance), nil
}

func (b *Buckets) ListIPFSPath(ctx context.Context, pth string) (*PathItem, error) {
	log.Debugf("listed ipfs path %s", pth)
	return b.pathToItem(ctx, nil, path.New(pth), true)
}

type PushPathsChunk struct {
	Path string
	Data []byte
}

type PushPathsResult struct {
	Path   string
	Cid    c.Cid
	Size   int64
	Pinned int64
	Bucket *Bucket
}

func (b *Buckets) PushPaths(
	ctx context.Context,
	thread core.ID,
	key string,
	root path.Resolved,
	identity did.Token,
) (chan<- PushPathsChunk, <-chan PushPathsResult, <-chan error) {
	lk := b.locks.Get(lock(key))
	lk.Acquire()

	in := make(chan PushPathsChunk)
	out := make(chan PushPathsResult)
	errs := make(chan error, 1)

	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		errs <- err
		lk.Release()
		return in, out, errs
	}
	if root != nil && root.String() != instance.Path {
		errs <- ErrNonFastForward
		lk.Release()
		return in, out, errs
	}
	readOnlyInstance := instance.Copy()

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var ctxLock sync.RWMutex
	addedCh := make(chan addedFile)
	doneCh := make(chan struct{})
	errCh := make(chan error)
	go func() {
		queue := newFileQueue()
		for {
			select {
			case chunk, ok := <-in:
				if !ok {
					wg.Wait() // Request ended normally, wait for pending jobs
					close(doneCh)
					return
				}
				pth, err := parsePath(chunk.Path)
				if err != nil {
					errCh <- fmt.Errorf("parsing path: %v", err)
					return
				}
				ctxLock.RLock()
				ctx := ctx
				ctxLock.RUnlock()
				fa, err := queue.add(ctx, b.ipfs.Unixfs(), pth, func() ([]byte, error) {
					wg.Add(1)
					readOnlyInstance.UpdatedAt = time.Now().UnixNano()
					readOnlyInstance.SetMetadataAtPath(pth, collection.Metadata{
						UpdatedAt: readOnlyInstance.UpdatedAt,
					})
					readOnlyInstance.UnsetMetadataWithPrefix(pth + "/")
					if err := b.c.Verify(ctx, thread, readOnlyInstance, collection.WithIdentity(identity)); err != nil {
						return nil, fmt.Errorf("verifying bucket update: %v", err)
					}
					key, err := readOnlyInstance.GetFileEncryptionKeyForPath(pth)
					if err != nil {
						return nil, fmt.Errorf("getting bucket key: %v", err)
					}
					return key, nil
				}, addedCh, errCh)
				if err != nil {
					errCh <- fmt.Errorf("enqueueing file: %v", err)
					return
				}

				if len(chunk.Data) > 0 {
					if _, err := fa.writer.Write(chunk.Data); err != nil {
						errCh <- fmt.Errorf("writing chunk: %v", err)
						return
					}
				} else {
					if err := fa.writer.Close(); err != nil {
						errCh <- fmt.Errorf("closing writer: %v", err)
						return
					}
				}
			}
		}
	}()

	var changed bool
	sctx := util.NewClonedContext(ctx)
	saveWithErr := func(err error) error {
		cancel()
		if !changed {
			return err
		}
		if serr := b.saveAndPublish(sctx, thread, instance, identity); serr != nil {
			if err != nil {
				return err
			}
			return serr
		} else {
			log.Debugf("saved bucket %s with path: %s", instance.Key, instance.Path)
		}
		return err
	}

	go func() {
		defer lk.Release()
		for {
			select {
			case res := <-addedCh:
				ctxLock.RLock()
				ctx2 := ctx
				ctxLock.RUnlock()

				fn, err := b.ipfs.ResolveNode(ctx2, res.resolved)
				if err != nil {
					errs <- saveWithErr(fmt.Errorf("resolving added node: %v", err))
					return
				}

				var dir path.Resolved
				if instance.IsPrivate() {
					ctx2, dir, err = dag.InsertNodeAtPath(
						ctx2,
						b.ipfs,
						fn,
						path.Join(path.New(instance.Path), res.path),
						instance.GetLinkEncryptionKey(),
					)
					if err != nil {
						errs <- saveWithErr(fmt.Errorf("inserting added node: %v", err))
						return
					}
				} else {
					dir, err = b.ipfs.Object().AddLink(
						ctx2,
						path.New(instance.Path),
						res.path,
						res.resolved,
						ifaceopts.Object.Create(true),
					)
					if err != nil {
						errs <- saveWithErr(fmt.Errorf("adding bucket link: %v", err))
						return
					}
					ctx2, err = dag.UpdateOrAddPin(ctx2, b.ipfs, path.New(instance.Path), dir)
					if err != nil {
						errs <- saveWithErr(fmt.Errorf("updating bucket pin: %v", err))
						return
					}
				}
				instance.Path = dir.String()
				instance.UpdatedAt = time.Now().UnixNano()
				instance.SetMetadataAtPath(res.path, collection.Metadata{
					UpdatedAt: instance.UpdatedAt,
				})
				instance.UnsetMetadataWithPrefix(res.path + "/")

				out <- PushPathsResult{
					Path:   res.path,
					Cid:    res.resolved.Cid(),
					Size:   res.size,
					Pinned: dag.GetPinnedBytes(ctx2),
					Bucket: instanceToBucket(thread, instance),
				}

				ctxLock.Lock()
				ctx = ctx2
				ctxLock.Unlock()

				log.Debugf("pushed %s to %s", res.path, instance.Key)
				changed = true // Save is needed
				wg.Done()

			case <-doneCh:
				errs <- saveWithErr(nil)
				return

			case err := <-errCh:
				errs <- saveWithErr(err)
				return
			}
		}
	}()
	return in, out, errs
}

type pathReader struct {
	r       io.Reader
	closers []io.Closer
}

func (r *pathReader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *pathReader) Close() error {
	// Close in reverse.
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i].Close(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Buckets) PullPath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	identity did.Token,
) (io.ReadCloser, error) {
	pth = trimSlash(pth)
	instance, bpth, err := b.getBucketAndPath(ctx, thread, key, pth, identity)
	if err != nil {
		return nil, err
	}
	fileKey, err := instance.GetFileEncryptionKeyForPath(pth)
	if err != nil {
		return nil, err
	}

	var filePath path.Resolved
	if instance.IsPrivate() {
		buckPath, err := util.NewResolvedPath(instance.Path)
		if err != nil {
			return nil, err
		}
		np, isDir, r, err := dag.GetNodesToPath(ctx, b.ipfs, buckPath, pth, instance.GetLinkEncryptionKey())
		if err != nil {
			return nil, err
		}
		if r != "" {
			return nil, fmt.Errorf("could not resolve path: %s", bpth)
		}
		if isDir {
			return nil, fmt.Errorf("node is a directory")
		}
		fn := np[len(np)-1]
		filePath = path.IpfsPath(fn.New.Cid())
	} else {
		filePath, err = b.ipfs.ResolvePath(ctx, bpth)
		if err != nil {
			return nil, err
		}
	}

	r := &pathReader{}
	node, err := b.ipfs.Unixfs().Get(ctx, filePath)
	if err != nil {
		return nil, err
	}
	r.closers = append(r.closers, node)

	file := ipfsfiles.ToFile(node)
	if file == nil {
		_ = r.Close()
		return nil, fmt.Errorf("node is a directory")
	}
	if fileKey != nil {
		dr, err := dcrypto.NewDecrypter(file, fileKey)
		if err != nil {
			_ = r.Close()
			return nil, err
		}
		r.closers = append(r.closers, dr)
		r.r = dr
	} else {
		r.r = file
	}

	log.Debugf("pulled %s from %s", pth, instance.Key)
	return r, nil
}

func (b *Buckets) PullIPFSPath(ctx context.Context, pth string) (io.ReadCloser, error) {
	node, err := b.ipfs.Unixfs().Get(ctx, path.New(pth))
	if err != nil {
		return nil, err
	}
	file := ipfsfiles.ToFile(node)
	if file == nil {
		return nil, fmt.Errorf("node is a directory")
	}
	return file, nil
}

func (b *Buckets) SetPath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	cid c.Cid,
	identity did.Token,
) (int64, *Bucket, error) {
	lck := b.locks.Get(lock(key))
	lck.Acquire()
	defer lck.Release()

	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return 0, nil, err
	}

	pth = trimSlash(pth)
	instance.UpdatedAt = time.Now().UnixNano()
	instance.SetMetadataAtPath(pth, collection.Metadata{
		UpdatedAt: instance.UpdatedAt,
	})
	instance.UnsetMetadataWithPrefix(pth + "/")

	if err := b.c.Verify(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
		return 0, nil, err
	}

	var linkKey, fileKey []byte
	if instance.IsPrivate() {
		linkKey = instance.GetLinkEncryptionKey()
		var err error
		fileKey, err = instance.GetFileEncryptionKeyForPath(pth)
		if err != nil {
			return 0, nil, err
		}
	}

	buckPath := path.New(instance.Path)
	ctx, dirPath, err := b.setPathFromExistingCid(ctx, instance, buckPath, pth, cid, linkKey, fileKey)
	if err != nil {
		return 0, nil, err
	}
	instance.Path = dirPath.String()
	if err := b.c.Save(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
		return 0, nil, err
	}

	log.Debugf("set %s to %s", pth, cid)
	return dag.GetPinnedBytes(ctx), instanceToBucket(thread, instance), nil
}

func (b *Buckets) MovePath(
	ctx context.Context,
	thread core.ID,
	key, fpth, tpth string,
	identity did.Token,
) (int64, *Bucket, error) {
	lk := b.locks.Get(lock(key))
	lk.Acquire()
	defer lk.Release()

	fpth, err := parsePath(fpth)
	if err != nil {
		return 0, nil, err
	}
	if fpth == "" {
		// @todo: enable move of root directory
		return 0, nil, fmt.Errorf("root cannot be moved")
	}
	tpth, err = parsePath(tpth)
	if err != nil {
		return 0, nil, err
	}
	// Paths are the same, nothing to do
	if fpth == tpth {
		return 0, nil, fmt.Errorf("path is destination")
	}

	instance, pth, err := b.getBucketAndPath(ctx, thread, key, fpth, identity)
	if err != nil {
		return 0, nil, fmt.Errorf("getting path: %v", err)
	}

	instance.UpdatedAt = time.Now().UnixNano()
	instance.SetMetadataAtPath(tpth, collection.Metadata{
		UpdatedAt: instance.UpdatedAt,
	})
	instance.UnsetMetadataWithPrefix(fpth + "/")
	if err := b.c.Verify(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
		return 0, nil, fmt.Errorf("verifying bucket update: %v", err)
	}

	fbpth, err := getBucketPath(instance, fpth)
	if err != nil {
		return 0, nil, err
	}
	fitem, err := b.pathToItem(ctx, instance, fbpth, false)
	if err != nil {
		return 0, nil, err
	}
	tbpth, err := getBucketPath(instance, tpth)
	if err != nil {
		return 0, nil, err
	}
	titem, err := b.pathToItem(ctx, instance, tbpth, false)
	if err == nil {
		if fitem.IsDir && !titem.IsDir {
			return 0, nil, fmt.Errorf("destination is not a directory")
		}
		if titem.IsDir {
			// from => to becomes new dir:
			//   - "c" => "b" becomes "b/c"
			//   - "a.jpg" => "b" becomes "b/a.jpg"
			tpth = gopath.Join(tpth, fitem.Name)
		}
	}

	pnode, err := dag.GetNodeAtPath(ctx, b.ipfs, pth, instance.GetLinkEncryptionKey())
	if err != nil {
		return 0, nil, fmt.Errorf("getting node: %v", err)
	}

	var dirPath path.Resolved
	if instance.IsPrivate() {
		ctx, dirPath, err = dag.CopyDag(ctx, b.ipfs, instance, pnode, fpth, tpth)
		if err != nil {
			return 0, nil, fmt.Errorf("copying node: %v", err)
		}
	} else {
		ctx, dirPath, err = b.setPathFromExistingCid(
			ctx,
			instance,
			path.New(instance.Path),
			tpth,
			pnode.Cid(),
			nil,
			nil,
		)
		if err != nil {
			return 0, nil, fmt.Errorf("copying path: %v", err)
		}
	}
	instance.Path = dirPath.String()

	// If "a/b" => "a/", cleanup only needed for priv
	if strings.HasPrefix(fpth, tpth) {
		if instance.IsPrivate() {
			ctx, dirPath, err = b.removePath(ctx, instance, fpth)
			if err != nil {
				return 0, nil, fmt.Errorf("removing path: %v", err)
			}
			instance.Path = dirPath.String()
		}

		if err := b.saveAndPublish(ctx, thread, instance, identity); err != nil {
			return 0, nil, err
		}

		log.Debugf("moved %s to %s", fpth, tpth)
		return dag.GetPinnedBytes(ctx), instanceToBucket(thread, instance), nil
	}

	if strings.HasPrefix(tpth, fpth) {
		// If "a/" => "a/b" cleanup each leaf in "a" that isn't "b" (skipping .textileseed)
		ppth := path.Join(path.New(instance.Path), fpth)
		item, err := b.listPath(ctx, instance, ppth)
		if err != nil {
			return 0, nil, fmt.Errorf("listing path: %v", err)
		}
		for _, chld := range item.Items {
			sp := trimSlash(movePathRegexp.ReplaceAllString(chld.Path, ""))
			if strings.Compare(chld.Name, collection.SeedName) == 0 || sp == tpth {
				continue
			}
			ctx, dirPath, err = b.removePath(ctx, instance, trimSlash(sp))
			if err != nil {
				return 0, nil, fmt.Errorf("removing path: %v", err)
			}
			instance.Path = dirPath.String()
		}
	} else {
		// if a/ => b/ remove a
		ctx, dirPath, err = b.removePath(ctx, instance, fpth)
		if err != nil {
			return 0, nil, fmt.Errorf("removing path: %v", err)
		}
		instance.Path = dirPath.String()
	}

	if err := b.saveAndPublish(ctx, thread, instance, identity); err != nil {
		return 0, nil, err
	}

	log.Debugf("moved %s to %s", fpth, tpth)
	return dag.GetPinnedBytes(ctx), instanceToBucket(thread, instance), nil
}

func (b *Buckets) RemovePath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	root path.Resolved,
	identity did.Token,
) (int64, *Bucket, error) {
	lk := b.locks.Get(lock(key))
	lk.Acquire()
	defer lk.Release()

	pth, err := parsePath(pth)
	if err != nil {
		return 0, nil, err
	}

	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return 0, nil, err
	}

	if root != nil && root.String() != instance.Path {
		return 0, nil, ErrNonFastForward
	}

	instance.UpdatedAt = time.Now().UnixNano()
	instance.UnsetMetadataWithPrefix(pth)
	if err := b.c.Verify(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
		return 0, nil, err
	}

	ctx, dirPath, err := b.removePath(ctx, instance, pth)
	if err != nil {
		return 0, nil, err
	}

	instance.Path = dirPath.String()
	if err := b.saveAndPublish(ctx, thread, instance, identity); err != nil {
		return 0, nil, err
	}

	log.Debugf("removed %s from %s", pth, key)
	return dag.GetPinnedBytes(ctx), instanceToBucket(thread, instance), nil
}

func (b *Buckets) PushPathAccessRoles(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	roles map[did.DID]collection.Role,
	identity did.Token,
) (int64, *Bucket, error) {
	lk := b.locks.Get(lock(key))
	lk.Acquire()
	defer lk.Release()

	pth, err := parsePath(pth)
	if err != nil {
		return 0, nil, err
	}

	instance, bpth, err := b.getBucketAndPath(ctx, thread, key, pth, identity)
	if err != nil {
		return 0, nil, err
	}
	linkKey := instance.GetLinkEncryptionKey()
	pathNode, err := dag.GetNodeAtPath(ctx, b.ipfs, bpth, linkKey)
	if err != nil {
		return 0, nil, err
	}

	md, mdPath, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return 0, nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	var target collection.Metadata
	if mdPath != pth { // If the metadata is inherited from a parent, create a new entry
		target = collection.Metadata{
			Roles: make(map[did.DID]collection.Role),
		}
	} else {
		target = md
	}
	var currentFileKeys map[string][]byte
	if instance.IsPrivate() {
		currentFileKeys, err = instance.GetFileEncryptionKeysForPrefix(pth)
		if err != nil {
			return 0, nil, err
		}
		newFileKey, err := dcrypto.NewKey()
		if err != nil {
			return 0, nil, err
		}
		target.SetFileEncryptionKey(newFileKey) // Create or refresh the file key
	}

	var changed bool
	for k, r := range roles {
		if x, ok := target.Roles[k]; ok && x == r {
			continue
		}
		if r > collection.NoneRole {
			target.Roles[k] = r
		} else {
			delete(target.Roles, k)
		}
		changed = true
	}
	if changed {
		instance.UpdatedAt = time.Now().UnixNano()
		target.UpdatedAt = instance.UpdatedAt
		instance.Metadata[pth] = target
		if instance.IsPrivate() {
			if err := instance.RotateFileEncryptionKeysForPrefix(pth); err != nil {
				return 0, nil, err
			}
		}
		if err := b.c.Verify(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
			return 0, nil, err
		}

		if instance.IsPrivate() {
			newFileKeys, err := instance.GetFileEncryptionKeysForPrefix(pth)
			if err != nil {
				return 0, nil, err
			}
			nmap, err := dag.EncryptDag(
				ctx,
				b.ipfs,
				pathNode,
				pth,
				linkKey,
				currentFileKeys,
				newFileKeys,
				nil,
				nil,
			)
			if err != nil {
				return 0, nil, err
			}
			nodes := make([]ipld.Node, len(nmap))
			i := 0
			for _, tn := range nmap {
				nodes[i] = tn.Node
				i++
			}
			pn := nmap[pathNode.Cid()].Node
			var dirPath path.Resolved
			ctx, dirPath, err = dag.InsertNodeAtPath(ctx, b.ipfs, pn, path.Join(path.New(instance.Path), pth), linkKey)
			if err != nil {
				return 0, nil, err
			}
			ctx, err = dag.AddAndPinNodes(ctx, b.ipfs, nodes)
			if err != nil {
				return 0, nil, err
			}
			instance.Path = dirPath.String()
		}

		if err := b.c.Save(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
			return 0, nil, err
		}
	}

	log.Debugf("pushed access roles for %s in %s", pth, key)
	return dag.GetPinnedBytes(ctx), instanceToBucket(thread, instance), nil
}

func (b *Buckets) PullPathAccessRoles(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	identity did.Token,
) (map[did.DID]collection.Role, error) {
	pth, err := parsePath(pth)
	if err != nil {
		return nil, err
	}
	instance, bpth, err := b.getBucketAndPath(ctx, thread, key, pth, identity)
	if err != nil {
		return nil, err
	}
	if _, err := dag.GetNodeAtPath(ctx, b.ipfs, bpth, instance.GetLinkEncryptionKey()); err != nil {
		return nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	md, _, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return nil, fmt.Errorf("could not resolve path: %s", pth)
	}

	log.Debugf("pulled access roles for %s in %s", pth, key)
	return md.Roles, nil
}

func (b *Buckets) saveAndPublish(
	ctx context.Context,
	thread core.ID,
	instance *collection.Bucket,
	identity did.Token,
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
