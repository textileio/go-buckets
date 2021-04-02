package client

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/cast"
	pb "github.com/textileio/go-buckets/api/pb/buckets"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	"google.golang.org/grpc"
)

const (
	// chunkSize for add file requests.
	chunkSize = 1024 * 32
)

var (
	// ErrPushPathQueueClosed indicates the push path is or was closed.
	ErrPushPathQueueClosed = errors.New("push path queue is closed")
)

// Client provides the client api.
type Client struct {
	c      pb.APIServiceClient
	conn   *grpc.ClientConn
	target did.DID
}

// NewClient starts the client.
func NewClient(addr string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		c:      pb.NewAPIServiceClient(conn),
		conn:   conn,
		target: "did:key:foo", // @todo: Get target from thread services
	}, nil
}

// Close closes the client's grpc connection and cancels any active requests.
func (c *Client) Close() error {
	return c.conn.Close()
}

// NewTokenContext adds an identity token targeting the client's service.
func (c *Client) NewTokenContext(
	ctx context.Context,
	identity core.Identity,
	duration time.Duration,
) (context.Context, error) {
	token, err := identity.Token(c.target, duration)
	if err != nil {
		return nil, err
	}
	return did.NewTokenContext(ctx, token), nil
}

// Create initializes a new bucket.
// The bucket name is only meant to help identify a bucket in a UI and is not unique.
func (c *Client) Create(ctx context.Context, opts ...buckets.CreateOption) (*pb.CreateResponse, error) {
	args := &buckets.CreateOptions{}
	for _, opt := range opts {
		opt(args)
	}
	var threadstr string
	if args.Thread.Defined() {
		threadstr = args.Thread.String()
	}
	var cidstr string
	if args.Cid.Defined() {
		cidstr = args.Cid.String()
	}
	return c.c.Create(ctx, &pb.CreateRequest{
		Thread:  threadstr,
		Name:    args.Name,
		Private: args.Private,
		Cid:     cidstr,
	})
}

// Get returns the bucket root.
func (c *Client) Get(ctx context.Context, thread core.ID, key string) (*pb.GetResponse, error) {
	return c.c.Get(ctx, &pb.GetRequest{
		Thread: thread.String(),
		Key:    key,
	})
}

// GetLinks returns lists for resolving a bucket with various protocols.
func (c *Client) GetLinks(ctx context.Context, thread core.ID, key, pth string) (*pb.GetLinksResponse, error) {
	return c.c.GetLinks(ctx, &pb.GetLinksRequest{
		Thread: thread.String(),
		Key:    key,
		Path:   filepath.ToSlash(pth),
	})
}

// List returns a list of all bucket roots.
func (c *Client) List(ctx context.Context, thread core.ID) (*pb.ListResponse, error) {
	return c.c.List(ctx, &pb.ListRequest{
		Thread: thread.String(),
	})
}

// Remove removes an entire bucket.
// Files and directories will be unpinned.
func (c *Client) Remove(ctx context.Context, thread core.ID, key string) error {
	_, err := c.c.Remove(ctx, &pb.RemoveRequest{
		Thread: thread.String(),
		Key:    key,
	})
	return err
}

// ListPath returns information about a bucket path.
func (c *Client) ListPath(ctx context.Context, thread core.ID, key, pth string) (*pb.ListPathResponse, error) {
	return c.c.ListPath(ctx, &pb.ListPathRequest{
		Thread: thread.String(),
		Key:    key,
		Path:   filepath.ToSlash(pth),
	})
}

// ListIpfsPath returns items at a particular path in a UnixFS path living in the IPFS network.
func (c *Client) ListIpfsPath(ctx context.Context, pth path.Path) (*pb.ListIpfsPathResponse, error) {
	return c.c.ListIpfsPath(ctx, &pb.ListIpfsPathRequest{
		Path: pth.String(),
	})
}

// PushPathsResult contains the result of a Push.
type PushPathsResult struct {
	Path   string
	Cid    cid.Cid
	Size   int64
	Pinned int64
	Root   path.Resolved

	err error
}

// PushPathsQueue handles PushPath input and output.
type PushPathsQueue struct {
	// Current contains the current push result.
	Current PushPathsResult

	q           []pushPath
	len         int
	inCh        chan pushPath
	inWaitCh    chan struct{}
	outCh       chan PushPathsResult
	started     bool
	stopped     bool
	closeFunc   func() error
	closed      bool
	closeWaitCh chan struct{}

	size     int64
	complete int64

	lk sync.Mutex
	wg sync.WaitGroup
}

type pushPath struct {
	path string
	r    func() (io.ReadCloser, error)
}

// AddFile adds a file to the queue.
// pth is the location relative to the bucket root at which to insert the file, e.g., "/path/to/mybone.jpg".
// name is the location of the file on the local filesystem, e.g., "/Users/clyde/Downloads/mybone.jpg".
func (c *PushPathsQueue) AddFile(pth, name string) error {
	c.lk.Lock()
	defer c.lk.Unlock()

	if c.closed {
		return ErrPushPathQueueClosed
	}
	if c.started {
		return errors.New("cannot call AddFile after Next")
	}

	f, err := os.Open(name)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	atomic.AddInt64(&c.size, info.Size())
	f.Close()
	c.q = append(c.q, pushPath{
		path: filepath.ToSlash(pth),
		r: func() (io.ReadCloser, error) {
			return os.Open(name)
		},
	})
	return nil
}

// AddReader adds a reader to the queue.
// pth is the location relative to the bucket root at which to insert the file, e.g., "/path/to/mybone.jpg".
// r is the reader to read from.
// size is the size of the reader. Use of the WithProgress option is not recommended if the reader size is unknown.
func (c *PushPathsQueue) AddReader(pth string, r io.Reader, size int64) error {
	c.lk.Lock()
	defer c.lk.Unlock()

	if c.closed {
		return ErrPushPathQueueClosed
	}
	if c.started {
		return errors.New("cannot call AddReader after Next")
	}

	atomic.AddInt64(&c.size, size)
	c.q = append(c.q, pushPath{
		path: filepath.ToSlash(pth),
		r: func() (io.ReadCloser, error) {
			return ioutil.NopCloser(r), nil
		},
	})
	return nil
}

// Size returns the queue size in bytes.
func (c *PushPathsQueue) Size() int64 {
	return atomic.LoadInt64(&c.size)
}

// Complete returns the portion of the queue size that has been pushed.
func (c *PushPathsQueue) Complete() int64 {
	return atomic.LoadInt64(&c.complete)
}

// Next blocks while the queue is open, returning true when a result is ready.
// Use Current to access the result.
func (c *PushPathsQueue) Next() (ok bool) {
	c.lk.Lock()
	if c.closed {
		c.lk.Unlock()
		return false
	}

	if !c.started {
		c.started = true
		c.len = len(c.q)
		c.start()
	}
	if c.len == 0 {
		c.lk.Unlock()
		return false
	}

	c.lk.Unlock()
	select {
	case r, k := <-c.outCh:
		if !k {
			return false
		}
		c.lk.Lock()
		c.len--
		c.Current = r
		c.lk.Unlock()
		return true
	}
}

func (c *PushPathsQueue) start() {
	go func() {
		defer close(c.inWaitCh)
		for {
			c.lk.Lock()
			if c.closed {
				c.q = nil
				c.lk.Unlock()
				return
			}
			if len(c.q) == 0 {
				c.lk.Unlock()
				return
			}
			var p pushPath
			p, c.q = c.q[0], c.q[1:]
			c.lk.Unlock()
			c.inCh <- p
		}
	}()
}

func (c *PushPathsQueue) stop() {
	c.lk.Lock()
	defer c.lk.Unlock()
	c.stopped = true
}

// Err returns the current queue error.
// Call this method before checking the value of Current.
func (c *PushPathsQueue) Err() error {
	c.lk.Lock()
	defer c.lk.Unlock()
	return c.Current.err
}

// Close the queue.
// Failure to close may lead to unpredictable bucket state.
func (c *PushPathsQueue) Close() error {
	c.lk.Lock()
	if c.closed {
		c.lk.Unlock()
		return nil
	}
	c.closed = true
	c.lk.Unlock()

	<-c.inWaitCh
	close(c.inCh)

	c.lk.Lock()
	wait := !c.stopped
	c.lk.Unlock()
	if err := c.closeFunc(); err != nil {
		return err
	}
	if wait {
		<-c.closeWaitCh
	}
	return nil
}

// PushPaths returns a queue that can be used to push multiple files and readers to bucket paths.
// See PushPathQueue.AddFile and PushPathsQueue.AddReader for more.
func (c *Client) PushPaths(
	ctx context.Context,
	thread core.ID,
	key string,
	opts ...buckets.Option,
) (*PushPathsQueue, error) {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	stream, err := c.c.PushPaths(ctx)
	if err != nil {
		return nil, err
	}
	var xr string
	if args.Root != nil {
		xr = args.Root.String()
	}

	if err := stream.Send(&pb.PushPathsRequest{
		Payload: &pb.PushPathsRequest_Header_{
			Header: &pb.PushPathsRequest_Header{
				Thread: thread.String(),
				Key:    key,
				Root:   xr,
			},
		},
	}); err != nil {
		return nil, err
	}

	q := &PushPathsQueue{
		inCh:     make(chan pushPath),
		inWaitCh: make(chan struct{}),
		outCh:    make(chan PushPathsResult),
		closeFunc: func() error {
			return stream.CloseSend()
		},
		closeWaitCh: make(chan struct{}),
	}

	go func() {
		defer q.stop()
		for {
			rep, err := stream.Recv()
			if err == io.EOF {
				select {
				case q.closeWaitCh <- struct{}{}:
				default:
				}
				return
			} else if err != nil {
				if strings.Contains(err.Error(), "STREAM_CLOSED") {
					err = ErrPushPathQueueClosed
				}
				q.outCh <- PushPathsResult{err: err}
				return
			}

			id, err := cid.Parse(rep.Cid)
			if err != nil {
				q.outCh <- PushPathsResult{err: err}
				return
			}
			root, err := dag.NewResolvedPath(rep.Bucket.Path)
			if err != nil {
				q.outCh <- PushPathsResult{err: err}
				return
			}
			q.outCh <- PushPathsResult{
				Path:   rep.Path,
				Cid:    id,
				Size:   rep.Size,
				Pinned: rep.Pinned,
				Root:   root,
			}
		}
	}()

	sendChunk := func(c *pb.PushPathsRequest_Chunk) bool {
		q.lk.Lock()
		if q.closed {
			q.lk.Unlock()
			return false
		}
		q.lk.Unlock()

		if err := stream.Send(&pb.PushPathsRequest{
			Payload: &pb.PushPathsRequest_Chunk_{
				Chunk: c,
			},
		}); err == io.EOF {
			return false // error is waiting to be received with stream.Recv above
		} else if err != nil {
			q.outCh <- PushPathsResult{err: err}
			return false
		}
		atomic.AddInt64(&q.complete, int64(len(c.Data)))

		q.lk.Lock()
		if !q.closed && args.Progress != nil {
			args.Progress <- atomic.LoadInt64(&q.complete)
		}
		q.lk.Unlock()
		return true
	}

	go func() {
		for p := range q.inCh {
			r, err := p.r()
			if err != nil {
				q.outCh <- PushPathsResult{err: err}
				break
			}
			buf := make([]byte, chunkSize)
			for {
				n, err := r.Read(buf)
				c := &pb.PushPathsRequest_Chunk{
					Path: p.path,
				}
				if n > 0 {
					c.Data = make([]byte, n)
					copy(c.Data, buf[:n])
					if ok := sendChunk(c); !ok {
						break
					}
				} else if err == io.EOF {
					sendChunk(c)
					break
				} else if err != nil {
					q.outCh <- PushPathsResult{err: err}
					break
				}
			}
			r.Close()
		}
	}()

	return q, nil
}

// PullPath pulls the bucket path, writing it to writer if it's a file.
func (c *Client) PullPath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	writer io.Writer,
	opts ...buckets.Option,
) error {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	pth = filepath.ToSlash(pth)
	stream, err := c.c.PullPath(ctx, &pb.PullPathRequest{
		Thread: thread.String(),
		Key:    key,
		Path:   pth,
	})
	if err != nil {
		return err
	}

	var written int64
	for {
		rep, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		n, err := writer.Write(rep.Chunk)
		if err != nil {
			return err
		}
		written += int64(n)
		if args.Progress != nil {
			args.Progress <- written
		}
	}
}

// PullIpfsPath pulls the path from a remote UnixFS dag, writing it to writer if it's a file.
func (c *Client) PullIpfsPath(ctx context.Context, pth path.Path, writer io.Writer, opts ...buckets.Option) error {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	stream, err := c.c.PullIpfsPath(ctx, &pb.PullIpfsPathRequest{
		Path: pth.String(),
	})
	if err != nil {
		return err
	}

	var written int64
	for {
		rep, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		n, err := writer.Write(rep.Chunk)
		if err != nil {
			return err
		}
		written += int64(n)
		if args.Progress != nil {
			args.Progress <- written
		}
	}
}

// SetPath set a particular path to an existing IPFS UnixFS DAG.
func (c *Client) SetPath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	remoteCid cid.Cid,
	opts ...buckets.Option,
) (*pb.SetPathResponse, error) {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	var xr string
	if args.Root != nil {
		xr = args.Root.String()
	}
	return c.c.SetPath(ctx, &pb.SetPathRequest{
		Thread: thread.String(),
		Key:    key,
		Root:   xr,
		Path:   filepath.ToSlash(pth),
		Cid:    remoteCid.String(),
	})
}

// MovePath moves a particular path to another path in the existing IPFS UnixFS DAG.
func (c *Client) MovePath(
	ctx context.Context,
	thread core.ID,
	key, pth string, dest string,
	opts ...buckets.Option,
) error {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	var xr string
	if args.Root != nil {
		xr = args.Root.String()
	}
	_, err := c.c.MovePath(ctx, &pb.MovePathRequest{
		Thread:   thread.String(),
		Key:      key,
		Root:     xr,
		FromPath: filepath.ToSlash(pth),
		ToPath:   filepath.ToSlash(dest),
	})
	return err
}

// RemovePath removes the file or directory at path.
// Files and directories will be unpinned.
func (c *Client) RemovePath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	opts ...buckets.Option,
) (path.Resolved, error) {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	var xr string
	if args.Root != nil {
		xr = args.Root.String()
	}
	res, err := c.c.RemovePath(ctx, &pb.RemovePathRequest{
		Thread: thread.String(),
		Key:    key,
		Root:   xr,
		Path:   filepath.ToSlash(pth),
	})
	if err != nil {
		return nil, err
	}
	return dag.NewResolvedPath(res.Bucket.Path)
}

// PushPathAccessRoles updates path access roles by merging the pushed roles with existing roles.
// roles is a map of string marshaled public keys to path roles. A non-nil error is returned
// if the map keys are not unmarshalable to public keys.
// To delete a role for a public key, set its value to buckets.None.
func (c *Client) PushPathAccessRoles(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	roles map[did.DID]collection.Role,
	opts ...buckets.Option,
) error {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	var xr string
	if args.Root != nil {
		xr = args.Root.String()
	}
	_, err := c.c.PushPathAccessRoles(ctx, &pb.PushPathAccessRolesRequest{
		Thread: thread.String(),
		Key:    key,
		Root:   xr,
		Path:   filepath.ToSlash(pth),
		Roles:  cast.RolesToPb(roles),
	})
	return err
}

// PullPathAccessRoles returns access roles for a path.
func (c *Client) PullPathAccessRoles(
	ctx context.Context,
	thread core.ID,
	key, pth string,
) (map[did.DID]collection.Role, error) {
	res, err := c.c.PullPathAccessRoles(ctx, &pb.PullPathAccessRolesRequest{
		Thread: thread.String(),
		Key:    key,
		Path:   filepath.ToSlash(pth),
	})
	if err != nil {
		return nil, err
	}
	return cast.RolesFromPb(res.Roles), nil
}

// PushPathInfo updates path info by merging the pushed info with existing info.
func (c *Client) PushPathInfo(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	info map[string]interface{},
	opts ...buckets.Option,
) error {
	args := &buckets.Options{}
	for _, opt := range opts {
		opt(args)
	}

	var xr string
	if args.Root != nil {
		xr = args.Root.String()
	}

	data, err := cast.InfoToPb(info)
	if err != nil {
		return err
	}
	_, err = c.c.PushPathInfo(ctx, &pb.PushPathInfoRequest{
		Thread: thread.String(),
		Key:    key,
		Root:   xr,
		Path:   filepath.ToSlash(pth),
		Info:   data,
	})
	return err
}

// PullPathInfo returns info for a path.
func (c *Client) PullPathInfo(
	ctx context.Context,
	thread core.ID,
	key, pth string,
) (map[string]interface{}, error) {
	res, err := c.c.PullPathInfo(ctx, &pb.PullPathInfoRequest{
		Thread: thread.String(),
		Key:    key,
		Path:   filepath.ToSlash(pth),
	})
	if err != nil {
		return nil, err
	}
	return cast.InfoFromPb(res.Info)
}
