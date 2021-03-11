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
	"github.com/textileio/go-buckets/util"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

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

type fileAdder struct {
	reader io.ReadCloser
	writer io.WriteCloser
}

type addedFile struct {
	path     string
	resolved path.Resolved
	size     int64
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
		doneCh <- addedFile{path: pth, resolved: res, size: int64(added)}
	}()

	return fa, nil
}
