package buckets

import (
	"context"
	"time"

	c "github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

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
