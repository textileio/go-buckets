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

// RemovePath removed a path from a bucket.
// All children under the path will be removed and unpinned.
func (b *Buckets) RemovePath(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	root path.Resolved,
	pth string,
) (int64, *Bucket, error) {
	txn, err := b.NewTxn(thread, key, identity)
	if err != nil {
		return 0, nil, err
	}
	defer txn.Close()
	return txn.RemovePath(ctx, root, pth)
}

// RemovePath is Txn based RemovePath.
func (t *Txn) RemovePath(ctx context.Context, root path.Resolved, pth string) (int64, *Bucket, error) {
	pth, err := parsePath(pth)
	if err != nil {
		return 0, nil, err
	}

	instance, err := t.b.c.GetSafe(ctx, t.thread, t.key, collection.WithIdentity(t.identity))
	if err != nil {
		return 0, nil, err
	}
	if root != nil && root.String() != instance.Path {
		return 0, nil, ErrNonFastForward
	}

	instance.UpdatedAt = time.Now().UnixNano()
	instance.UnsetMetadataWithPrefix(pth)
	if err := t.b.c.Verify(ctx, t.thread, instance, collection.WithIdentity(t.identity)); err != nil {
		return 0, nil, err
	}

	ctx, dirPath, err := t.b.removePath(ctx, instance, pth)
	if err != nil {
		return 0, nil, err
	}

	instance.Path = dirPath.String()
	if err := t.b.saveAndPublish(ctx, t.thread, t.identity, instance); err != nil {
		return 0, nil, err
	}

	log.Debugf("removed %s from %s", pth, t.key)
	return dag.GetPinnedBytes(ctx), instanceToBucket(t.thread, instance), nil
}
