package buckets

import (
	"context"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

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
