package api

import (
	"context"
	"errors"
	"fmt"
	"io"

	c "github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/cast"
	pb "github.com/textileio/go-buckets/api/pb/buckets"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

// chunkSize for get file requests.
const chunkSize = 1024 * 32 // 32 KiB

// Service is a gRPC service for buckets.
type Service struct {
	lib *buckets.Buckets
}

var _ pb.APIServiceServer = (*Service)(nil)

// NewService returns a new buckets gRPC service.
func NewService(lib *buckets.Buckets) *Service {
	return &Service{
		lib: lib,
	}
}

func (s *Service) Create(ctx context.Context, req *pb.CreateRequest) (*pb.CreateResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}
	var cid c.Cid
	if len(req.Cid) != 0 {
		cid, err = c.Decode(req.Cid)
		if err != nil {
			return nil, fmt.Errorf("decoding cid: %v", err)
		}
	}

	bucket, seed, pinned, err := s.lib.Create(
		ctx,
		identity,
		buckets.WithThread(thread),
		buckets.WithName(req.Name),
		buckets.WithPrivate(req.Private),
		buckets.WithCid(cid),
	)
	if err != nil {
		return nil, err
	}
	links, err := s.lib.GetLinksForBucket(ctx, bucket, identity, "")
	if err != nil {
		return nil, err
	}
	return &pb.CreateResponse{
		Bucket: cast.BucketToPb(bucket),
		Links:  cast.LinksToPb(links),
		Pinned: pinned,
		Seed: &pb.Seed{
			Cid:  seed.Cid.String(),
			Data: seed.Data,
		},
	}, nil
}

func (s *Service) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}

	bucket, err := s.lib.Get(ctx, thread, req.Key, identity)
	if err != nil {
		return nil, err
	}
	links, err := s.lib.GetLinksForBucket(ctx, bucket, identity, "")
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{
		Bucket: cast.BucketToPb(bucket),
		Links:  cast.LinksToPb(links),
	}, nil
}

func (s *Service) GetLinks(ctx context.Context, req *pb.GetLinksRequest) (*pb.GetLinksResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}

	links, err := s.lib.GetLinks(ctx, thread, req.Key, identity, req.Path)
	if err != nil {
		return nil, err
	}
	return &pb.GetLinksResponse{
		Links: cast.LinksToPb(links),
	}, nil
}

func (s *Service) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}

	list, err := s.lib.List(ctx, thread, identity)
	if err != nil {
		return nil, err
	}
	pblist := make([]*pb.Bucket, len(list))
	for i, b := range list {
		pblist[i] = cast.BucketToPb(&b)
	}
	return &pb.ListResponse{
		Buckets: pblist,
	}, nil
}

func (s *Service) Remove(ctx context.Context, req *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}

	pinned, err := s.lib.Remove(ctx, thread, req.Key, identity)
	if err != nil {
		return nil, err
	}
	return &pb.RemoveResponse{
		Pinned: pinned,
	}, nil
}

func (s *Service) ListPath(ctx context.Context, req *pb.ListPathRequest) (*pb.ListPathResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}

	item, bucket, err := s.lib.ListPath(ctx, thread, req.Key, identity, req.Path)
	if err != nil {
		return nil, err
	}
	links, err := s.lib.GetLinksForBucket(ctx, bucket, identity, req.Path)
	if err != nil {
		return nil, err
	}
	return &pb.ListPathResponse{
		Item:   cast.ItemToPb(item),
		Bucket: cast.BucketToPb(bucket),
		Links:  cast.LinksToPb(links),
	}, nil
}

func (s *Service) ListIpfsPath(ctx context.Context, req *pb.ListIpfsPathRequest) (*pb.ListIpfsPathResponse, error) {
	item, err := s.lib.ListIPFSPath(ctx, req.Path)
	if err != nil {
		return nil, err
	}
	return &pb.ListIpfsPathResponse{
		Item: cast.ItemToPb(item),
	}, nil
}

func (s *Service) PushPaths(server pb.APIService_PushPathsServer) error {
	identity, err := did.NewTokenFromMD(server.Context())
	if err != nil {
		return fmt.Errorf("getting identity token: %v", err)
	}

	req, err := server.Recv()
	if err != nil {
		return fmt.Errorf("on receive: %v", err)
	}
	var (
		thread core.ID
		key    string
		root   path.Resolved
	)
	switch payload := req.Payload.(type) {
	case *pb.PushPathsRequest_Header_:
		thread, err = core.Decode(payload.Header.Thread)
		if err != nil {
			return fmt.Errorf("decoding thread: %v", err)
		}
		key = payload.Header.Key
		root, err = rootFromString(payload.Header.Root)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("push bucket path header is required")
	}

	in, out, errs := s.lib.PushPaths(server.Context(), thread, key, identity, root)
	if len(errs) != 0 {
		return <-errs
	}
	errCh := make(chan error)
	go func() {
		defer close(in)
		for {
			req, err := server.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				errCh <- fmt.Errorf("on receive: %v", err)
				return
			}
			switch payload := req.Payload.(type) {
			case *pb.PushPathsRequest_Chunk_:
				in <- buckets.PushPathsInput{
					Path:  payload.Chunk.Path,
					Chunk: payload.Chunk.Data,
				}
			default:
				errCh <- fmt.Errorf("invalid request")
				return
			}
		}
	}()

	for {
		select {
		case res := <-out:
			if err := server.Send(&pb.PushPathsResponse{
				Path:   res.Path,
				Cid:    res.Cid.String(),
				Size:   res.Size,
				Pinned: res.Pinned,
				Bucket: cast.BucketToPb(res.Bucket),
			}); err != nil {
				return fmt.Errorf("sending event: %v", err)
			}
		case err := <-errs:
			return err
		case err := <-errCh:
			return err
		}
	}
}

func (s *Service) PullPath(req *pb.PullPathRequest, server pb.APIService_PullPathServer) error {
	thread, identity, err := getThreadAndIdentity(server.Context(), req.Thread)
	if err != nil {
		return err
	}

	reader, err := s.lib.PullPath(server.Context(), thread, req.Key, identity, req.Path)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := make([]byte, chunkSize)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := server.Send(&pb.PullPathResponse{
				Chunk: buf[:n],
			}); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
	}
}

func (s *Service) PullIpfsPath(req *pb.PullIpfsPathRequest, server pb.APIService_PullIpfsPathServer) error {
	reader, err := s.lib.PullIPFSPath(server.Context(), req.Path)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := make([]byte, chunkSize)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := server.Send(&pb.PullIpfsPathResponse{
				Chunk: buf[:n],
			}); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
	}
}

func (s *Service) SetPath(ctx context.Context, req *pb.SetPathRequest) (*pb.SetPathResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}
	root, err := rootFromString(req.Root)
	if err != nil {
		return nil, err
	}
	cid, err := c.Decode(req.Cid)
	if err != nil {
		return nil, fmt.Errorf("decoding cid: %v", err)
	}

	pinned, bucket, err := s.lib.SetPath(ctx, thread, req.Key, identity, root, req.Path, cid, nil)
	if err != nil {
		return nil, err
	}
	return &pb.SetPathResponse{
		Bucket: cast.BucketToPb(bucket),
		Pinned: pinned,
	}, nil
}

// MovePath moves source path to destination path and cleans up afterward
func (s *Service) MovePath(ctx context.Context, req *pb.MovePathRequest) (res *pb.MovePathResponse, err error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}
	root, err := rootFromString(req.Root)
	if err != nil {
		return nil, err
	}

	pinned, bucket, err := s.lib.MovePath(ctx, thread, req.Key, identity, root, req.FromPath, req.ToPath)
	if err != nil {
		return nil, err
	}
	return &pb.MovePathResponse{
		Bucket: cast.BucketToPb(bucket),
		Pinned: pinned,
	}, nil
}

func (s *Service) RemovePath(ctx context.Context, req *pb.RemovePathRequest) (res *pb.RemovePathResponse, err error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}
	root, err := rootFromString(req.Root)
	if err != nil {
		return nil, err
	}

	pinned, bucket, err := s.lib.RemovePath(ctx, thread, req.Key, identity, root, req.Path)
	if err != nil {
		return nil, err
	}
	return &pb.RemovePathResponse{
		Bucket: cast.BucketToPb(bucket),
		Pinned: pinned,
	}, nil
}

func (s *Service) PushPathAccessRoles(
	ctx context.Context,
	req *pb.PushPathAccessRolesRequest,
) (res *pb.PushPathAccessRolesResponse, err error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}
	root, err := rootFromString(req.Root)
	if err != nil {
		return nil, err
	}
	roles := cast.RolesFromPb(req.Roles)

	pinned, bucket, err := s.lib.PushPathAccessRoles(ctx, thread, req.Key, identity, root, req.Path, roles)
	if err != nil {
		return nil, err
	}
	return &pb.PushPathAccessRolesResponse{
		Bucket: cast.BucketToPb(bucket),
		Pinned: pinned,
	}, nil
}

func (s *Service) PullPathAccessRoles(
	ctx context.Context,
	req *pb.PullPathAccessRolesRequest,
) (*pb.PullPathAccessRolesResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}

	roles, err := s.lib.PullPathAccessRoles(ctx, thread, req.Key, identity, req.Path)
	if err != nil {
		return nil, err
	}
	return &pb.PullPathAccessRolesResponse{
		Roles: cast.RolesToPb(roles),
	}, nil
}

func (s *Service) PushPathInfo(
	ctx context.Context,
	req *pb.PushPathInfoRequest,
) (res *pb.PushPathInfoResponse, err error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}
	root, err := rootFromString(req.Root)
	if err != nil {
		return nil, err
	}
	info, err := cast.InfoFromPb(req.Info)
	if err != nil {
		return nil, err
	}

	bucket, err := s.lib.PushPathInfo(ctx, thread, req.Key, identity, root, req.Path, info)
	if err != nil {
		return nil, err
	}
	return &pb.PushPathInfoResponse{
		Bucket: cast.BucketToPb(bucket),
	}, nil
}

func (s *Service) PullPathInfo(
	ctx context.Context,
	req *pb.PullPathInfoRequest,
) (*pb.PullPathInfoResponse, error) {
	thread, identity, err := getThreadAndIdentity(ctx, req.Thread)
	if err != nil {
		return nil, err
	}

	info, err := s.lib.PullPathInfo(ctx, thread, req.Key, identity, req.Path)
	if err != nil {
		return nil, err
	}
	data, err := cast.InfoToPb(info)
	if err != nil {
		return nil, err
	}

	return &pb.PullPathInfoResponse{
		Info: data,
	}, nil
}

func getThreadAndIdentity(ctx context.Context, threadStr string) (thread core.ID, identity did.Token, err error) {
	if len(threadStr) != 0 {
		thread, err = core.Decode(threadStr)
		if err != nil {
			return "", "", fmt.Errorf("decoding thread: %v", err)
		}
	}
	identity, err = did.NewTokenFromMD(ctx)
	if err != nil {
		return "", "", fmt.Errorf("getting identity token: %v", err)
	}
	return thread, identity, nil
}

func rootFromString(root string) (r path.Resolved, err error) {
	if len(root) != 0 {
		r, err = dag.NewResolvedPath(root)
		if err != nil {
			return nil, fmt.Errorf("resolving root path: %v", err)
		}
	}
	return r, nil
}
