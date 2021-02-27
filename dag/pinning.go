package dag

import (
	"context"
	"errors"
	"fmt"

	ipld "github.com/ipfs/go-ipld-format"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

const (
	// pinNotRecursiveMsg is used to match an IPFS "recursively pinned already" error.
	pinNotRecursiveMsg = "'from' cid was not recursively pinned already"
)

var (
	// ErrStorageQuotaExhausted indicates the requested operation exceeds the storage allowance.
	ErrStorageQuotaExhausted = errors.New("storage quota exhausted")
)

type ctxKey string

// BucketOwner provides owner context to the bucket service.
type BucketOwner struct {
	StorageUsed      int64
	StorageAvailable int64
	StorageDelta     int64
}

// NewOwnerContext returns a new bucket owner context.
func NewOwnerContext(ctx context.Context, owner *BucketOwner) context.Context {
	return context.WithValue(ctx, ctxKey("bucketOwner"), owner)
}

// OwnerFromContext returns a bucket owner from the context if available.
func OwnerFromContext(ctx context.Context) (*BucketOwner, bool) {
	owner, ok := ctx.Value(ctxKey("bucketOwner")).(*BucketOwner)
	return owner, ok
}

// AddPinnedBytes adds the provided delta to a running total for context.
func AddPinnedBytes(ctx context.Context, delta int64) context.Context {
	total, _ := ctx.Value(ctxKey("pinnedBytes")).(int64)
	ctx = context.WithValue(ctx, ctxKey("pinnedBytes"), total+delta)
	owner, ok := OwnerFromContext(ctx)
	if ok {
		owner.StorageUsed += delta
		owner.StorageAvailable -= delta
		owner.StorageDelta += delta
		ctx = NewOwnerContext(ctx, owner)
	}
	return ctx
}

// GetPinnedBytes returns the total pinned bytes for context.
func GetPinnedBytes(ctx context.Context) int64 {
	pinned, _ := ctx.Value(ctxKey("pinnedBytes")).(int64)
	return pinned
}

// PinBlocks pins blocks, accounting for sum bytes pinned for context.
func PinBlocks(ctx context.Context, ipfs iface.CoreAPI, nodes []ipld.Node) (context.Context, error) {
	var totalAddedSize int64
	for _, n := range nodes {
		s, err := n.Stat()
		if err != nil {
			return ctx, fmt.Errorf("getting size of node: %v", err)
		}
		totalAddedSize += int64(s.CumulativeSize)
	}

	// Check context owner's storage allowance
	owner, ok := OwnerFromContext(ctx)
	if ok && totalAddedSize > owner.StorageAvailable {
		return ctx, ErrStorageQuotaExhausted
	}

	if err := ipfs.Dag().Pinning().AddMany(ctx, nodes); err != nil {
		return ctx, fmt.Errorf("pinning set of nodes: %v", err)
	}
	return AddPinnedBytes(ctx, totalAddedSize), nil
}

// AddAndPinNodes adds and pins nodes, accounting for sum bytes pinned for context.
func AddAndPinNodes(ctx context.Context, ipfs iface.CoreAPI, nodes []ipld.Node) (context.Context, error) {
	if err := ipfs.Dag().AddMany(ctx, nodes); err != nil {
		return ctx, err
	}
	return PinBlocks(ctx, ipfs, nodes)
}

// UpdateOrAddPin moves the pin at from to to.
// If from is nil, a new pin as placed at to.
func UpdateOrAddPin(ctx context.Context, ipfs iface.CoreAPI, from, to path.Path) (context.Context, error) {
	toSize, err := GetPathSize(ctx, ipfs, to)
	if err != nil {
		return ctx, fmt.Errorf("getting size of destination dag: %v", err)
	}

	fromSize, err := GetPathSize(ctx, ipfs, from)
	if err != nil {
		return ctx, fmt.Errorf("getting size of current dag: %v", err)
	}
	deltaSize := -fromSize + toSize

	// Check context owner's storage allowance
	owner, ok := OwnerFromContext(ctx)
	if ok && deltaSize > owner.StorageAvailable {
		return ctx, ErrStorageQuotaExhausted
	}

	if from == nil {
		if err := ipfs.Pin().Add(ctx, to); err != nil {
			return ctx, err
		}
	} else {
		if err := ipfs.Pin().Update(ctx, from, to); err != nil {
			if err.Error() == pinNotRecursiveMsg {
				return ctx, ipfs.Pin().Add(ctx, to)
			}
			return ctx, err
		}
	}
	return AddPinnedBytes(ctx, deltaSize), nil
}

// UnpinPath unpins path and accounts for sum bytes pinned for context.
func UnpinPath(ctx context.Context, ipfs iface.CoreAPI, path path.Path) (context.Context, error) {
	if err := ipfs.Pin().Rm(ctx, path); err != nil {
		return ctx, err
	}
	size, err := GetPathSize(ctx, ipfs, path)
	if err != nil {
		return ctx, fmt.Errorf("getting size of removed node: %v", err)
	}
	return AddPinnedBytes(ctx, -size), nil
}

// UnpinBranch walks a the node at path, decrypting (if needed) and unpinning all nodes
func UnpinBranch(ctx context.Context, ipfs iface.CoreAPI, p path.Resolved, key []byte) (context.Context, error) {
	n, _, err := ResolveNodeAtPath(ctx, ipfs, p, key)
	if err != nil {
		return ctx, err
	}
	for _, l := range n.Links() {
		if l.Name == "" {
			continue // Data nodes will never be pinned directly
		}
		lp := path.IpfsPath(l.Cid)
		ctx, err = UnpinPath(ctx, ipfs, lp)
		if err != nil {
			return ctx, err
		}
		ctx, err = UnpinBranch(ctx, ipfs, lp, key)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// UnpinNodeAndBranch unpins a node and its entire branch, accounting for sum bytes pinned for context.
func UnpinNodeAndBranch(
	ctx context.Context,
	ipfs iface.CoreAPI,
	pth path.Resolved,
	key []byte,
) (context.Context, error) {
	ctx, err := UnpinBranch(ctx, ipfs, pth, key)
	if err != nil {
		return ctx, err
	}
	return UnpinPath(ctx, ipfs, pth)
}
