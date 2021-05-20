package buckets

import (
	"context"
	"fmt"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

// ListPath lists all paths under a path.
func (b *Buckets) ListPath(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pth string,
) (*PathItem, *Bucket, error) {
	if err := thread.Validate(); err != nil {
		return nil, nil, fmt.Errorf("invalid thread id: %v", err)
	}
	pth = trimSlash(pth)
	instance, bpth, err := b.getBucketAndPath(ctx, thread, key, identity, pth)
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

// ListIPFSPath lists all paths under a path.
func (b *Buckets) ListIPFSPath(ctx context.Context, pth string) (*PathItem, error) {
	log.Debugf("listed ipfs path %s", pth)
	return b.pathToItem(ctx, nil, path.New(pth), true)
}
