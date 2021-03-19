package buckets

import (
	"context"
	"time"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

func (b *Buckets) RemovePath(
	ctx context.Context,
	thread core.ID,
	key string,
	root path.Resolved,
	pth string,
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
