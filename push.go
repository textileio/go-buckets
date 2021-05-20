package buckets

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	c "github.com/ipfs/go-cid"
	ipfsfiles "github.com/ipfs/go-ipfs-files"
	iface "github.com/ipfs/interface-go-ipfs-core"
	ifaceopts "github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

// PushPathsInput is used to with PushPath and PushPaths to specify a reader or chunked data to push to a path.
type PushPathsInput struct {
	// Path is a bucket relative path at which to insert data.
	Path string
	// Reader from which to write data into Path.
	Reader io.Reader
	// Chunk should be used to add chunked data when a plain io.Reader is not available.
	Chunk []byte
	// Meta is optional metadata the will be persisted under Path.
	Meta map[string]interface{}
}

// PushPathsResult contains the result of a single push result from PuthPath or PushPaths.
type PushPathsResult struct {
	Path   string
	Cid    c.Cid
	Size   int64
	Pinned int64
	Bucket *Bucket
}

// PushPath pushes data to a single bucket path.
func (b *Buckets) PushPath(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	root path.Resolved,
	input PushPathsInput,
) (*PushPathsResult, error) {
	txn, err := b.NewTxn(thread, key, identity)
	if err != nil {
		return nil, err
	}
	defer txn.Close()
	return txn.PushPath(ctx, root, input)
}

// PushPath is Txn based PushPath.
func (t *Txn) PushPath(
	ctx context.Context,
	root path.Resolved,
	input PushPathsInput,
) (*PushPathsResult, error) {
	in, out, errs := t.PushPaths(ctx, root)
	if len(errs) != 0 {
		err := <-errs
		return nil, err
	}

	go func() {
		in <- input
		close(in)
	}()

	result := &PushPathsResult{}
	for {
		select {
		case res := <-out:
			result = &res
		case err := <-errs:
			return result, err
		}
	}
}

// PushPaths pushes data to one or more bucket paths.
// The returned channels are used to push one or more PushPathsInput.
// Each input will result in a PushPathsResult.
// Use this method to bulk write data to a bucket.
func (b *Buckets) PushPaths(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	root path.Resolved,
) (chan<- PushPathsInput, <-chan PushPathsResult, <-chan error) {
	errs := make(chan error, 1)
	txn, err := b.NewTxn(thread, key, identity)
	if err != nil {
		errs <- err
		return nil, nil, errs
	}

	in, out, perrs := txn.PushPaths(ctx, root)
	go func() {
		defer txn.Close()
		for err := range perrs {
			errs <- err
			return
		}
	}()
	return in, out, errs
}

// PushPaths is Txn based PushPaths.
func (t *Txn) PushPaths(
	ctx context.Context,
	root path.Resolved,
) (chan<- PushPathsInput, <-chan PushPathsResult, <-chan error) {
	in := make(chan PushPathsInput)
	out := make(chan PushPathsResult)
	errs := make(chan error, 1)

	instance, err := t.b.c.GetSafe(ctx, t.thread, t.key, collection.WithIdentity(t.identity))
	if err != nil {
		errs <- err
		return in, out, errs
	}
	if root != nil && root.String() != instance.Path {
		errs <- ErrNonFastForward
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
			case input, ok := <-in:
				if !ok {
					wg.Wait() // Request ended normally, wait for pending jobs
					close(doneCh)
					return
				}
				pth, err := parsePath(input.Path)
				if err != nil {
					errCh <- fmt.Errorf("parsing path: %v", err)
					return
				}
				ctxLock.RLock()
				ctx := ctx
				ctxLock.RUnlock()
				fa, err := queue.add(ctx, t.b.ipfs.Unixfs(), pth, input.Meta, func() ([]byte, error) {
					wg.Add(1)
					readOnlyInstance.UpdatedAt = time.Now().UnixNano()
					readOnlyInstance.SetMetadataAtPath(pth, collection.Metadata{
						UpdatedAt: readOnlyInstance.UpdatedAt,
						Info:      input.Meta,
					})
					readOnlyInstance.UnsetMetadataWithPrefix(pth + "/")
					if err := t.b.c.Verify(
						ctx,
						t.thread,
						readOnlyInstance,
						collection.WithIdentity(t.identity),
					); err != nil {
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

				if input.Reader != nil {
					if _, err := io.Copy(fa.writer, input.Reader); err != nil {
						errCh <- fmt.Errorf("piping reader: %v", err)
						return
					}
					if err := fa.writer.Close(); err != nil {
						errCh <- fmt.Errorf("closing writer: %v", err)
						return
					}
				} else if len(input.Chunk) > 0 {
					if _, err := fa.writer.Write(input.Chunk); err != nil {
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
	sctx := newClonedContext(ctx)
	saveWithErr := func(err error) error {
		cancel()
		if !changed {
			return err
		}
		if serr := t.b.saveAndPublish(sctx, t.thread, t.identity, instance); serr != nil {
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
		for {
			select {
			case res := <-addedCh:
				ctxLock.RLock()
				ctx2 := ctx
				ctxLock.RUnlock()

				fn, err := t.b.ipfs.ResolveNode(ctx2, res.resolved)
				if err != nil {
					errs <- saveWithErr(fmt.Errorf("resolving added node: %v", err))
					return
				}

				var dir path.Resolved
				if instance.IsPrivate() {
					ctx2, dir, err = dag.InsertNodeAtPath(
						ctx2,
						t.b.ipfs,
						fn,
						path.Join(path.New(instance.Path), res.path),
						instance.GetLinkEncryptionKey(),
					)
					if err != nil {
						errs <- saveWithErr(fmt.Errorf("inserting added node: %v", err))
						return
					}
				} else {
					dir, err = t.b.ipfs.Object().AddLink(
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
					ctx2, err = dag.UpdateOrAddPin(ctx2, t.b.ipfs, path.New(instance.Path), dir)
					if err != nil {
						errs <- saveWithErr(fmt.Errorf("updating bucket pin: %v", err))
						return
					}
				}
				instance.Path = dir.String()
				instance.UpdatedAt = time.Now().UnixNano()
				instance.SetMetadataAtPath(res.path, collection.Metadata{
					UpdatedAt: instance.UpdatedAt,
					Info:      res.meta,
				})
				instance.UnsetMetadataWithPrefix(res.path + "/")

				out <- PushPathsResult{
					Path:   res.path,
					Cid:    res.resolved.Cid(),
					Size:   res.size,
					Pinned: dag.GetPinnedBytes(ctx2),
					Bucket: instanceToBucket(t.thread, instance),
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

type fileAdder struct {
	reader io.ReadCloser
	writer io.WriteCloser
}

type addedFile struct {
	path     string
	resolved path.Resolved
	size     int64
	meta     map[string]interface{}
}

type fileQueue struct {
	q    map[string]*fileAdder
	lock sync.Mutex
}

func newFileQueue() *fileQueue {
	return &fileQueue{q: make(map[string]*fileAdder)}
}

func (q *fileQueue) add(
	ctx context.Context,
	ufs iface.UnixfsAPI,
	pth string,
	meta map[string]interface{},
	addFunc func() ([]byte, error),
	doneCh chan<- addedFile,
	errCh chan<- error,
) (*fileAdder, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	fa, ok := q.q[pth]
	if ok {
		return fa, nil
	}

	key, err := addFunc()
	if err != nil {
		return nil, err
	}

	reader, writer := io.Pipe()
	fa = &fileAdder{
		reader: reader,
		writer: writer,
	}
	q.q[pth] = fa

	eventCh := make(chan interface{})
	chSize := make(chan string)
	go func() {
		for e := range eventCh {
			event, ok := e.(*iface.AddEvent)
			if !ok {
				log.Error("unexpected event type")
				continue
			}
			if event.Path != nil {
				chSize <- event.Size // Save size for use in the final response
			}
		}
	}()

	var r io.Reader
	if key != nil {
		r, err = dcrypto.NewEncrypter(reader, key)
		if err != nil {
			return nil, fmt.Errorf("creating decrypter: %v", err)
		}
	} else {
		r = reader
	}

	go func() {
		defer close(eventCh)
		res, err := ufs.Add(
			ctx,
			ipfsfiles.NewReaderFile(r),
			ifaceopts.Unixfs.CidVersion(1),
			ifaceopts.Unixfs.Pin(false),
			ifaceopts.Unixfs.Progress(true),
			ifaceopts.Unixfs.Events(eventCh),
		)
		if err != nil {
			errCh <- fmt.Errorf("adding file: %v", err)
			return
		}
		size := <-chSize
		added, err := strconv.Atoi(size)
		if err != nil {
			errCh <- fmt.Errorf("getting file size: %v", err)
			return
		}
		doneCh <- addedFile{
			path:     pth,
			resolved: res,
			size:     int64(added),
			meta:     meta,
		}
	}()

	return fa, nil
}

// newClonedContext returns a context with the same Values but not inherited cancelation.
func newClonedContext(ctx context.Context) context.Context {
	return valueOnlyContext{Context: ctx}
}

type valueOnlyContext struct{ context.Context }

func (valueOnlyContext) Deadline() (deadline time.Time, ok bool) { return }
func (valueOnlyContext) Done() <-chan struct{}                   { return nil }
func (valueOnlyContext) Err() error                              { return nil }
