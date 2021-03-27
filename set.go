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
	key string,
	identity did.Token,
	root path.Resolved,
	pth string,
	cid c.Cid,
	meta map[string]interface{},
) (int64, *Bucket, error) {
	txn, err := b.NewTxn(thread, key, identity)
	if err != nil {
		return 0, nil, err
	}
	defer txn.Close()
	return txn.SetPath(ctx, root, pth, cid, meta)
}

func (t *Txn) SetPath(
	ctx context.Context,
	root path.Resolved,
	pth string,
	cid c.Cid,
	meta map[string]interface{},
) (int64, *Bucket, error) {
	instance, err := t.b.c.GetSafe(ctx, t.thread, t.key, collection.WithIdentity(t.identity))
	if err != nil {
		return 0, nil, err
	}
	if root != nil && root.String() != instance.Path {
		return 0, nil, ErrNonFastForward
	}

	pth = trimSlash(pth)
	instance.UpdatedAt = time.Now().UnixNano()
	instance.SetMetadataAtPath(pth, collection.Metadata{
		UpdatedAt: instance.UpdatedAt,
		Info:      meta,
	})
	instance.UnsetMetadataWithPrefix(pth + "/")

	if err := t.b.c.Verify(ctx, t.thread, instance, collection.WithIdentity(t.identity)); err != nil {
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
	ctx, dirPath, err := t.b.setPathFromExistingCid(ctx, instance, buckPath, pth, cid, linkKey, fileKey)
	if err != nil {
		return 0, nil, err
	}
	instance.Path = dirPath.String()
	if err := t.b.c.Save(ctx, t.thread, instance, collection.WithIdentity(t.identity)); err != nil {
		return 0, nil, err
	}

	log.Debugf("set %s to %s", pth, cid)
	return dag.GetPinnedBytes(ctx), instanceToBucket(t.thread, instance), nil
}
