package buckets

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

// PushPathInfo pushes arbitrary info/metadata to a path,
// allowing users to add application specific path metadata.
func (b *Buckets) PushPathInfo(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	root path.Resolved,
	pth string,
	info map[string]interface{},
) (*Bucket, error) {
	txn, err := b.NewTxn(thread, key, identity)
	if err != nil {
		return nil, err
	}
	defer txn.Close()
	return txn.PushPathInfo(ctx, root, pth, info)
}

// PushPathInfo is Txn based PushPathInfo.
func (t *Txn) PushPathInfo(
	ctx context.Context,
	root path.Resolved,
	pth string,
	info map[string]interface{},
) (*Bucket, error) {
	pth, err := parsePath(pth)
	if err != nil {
		return nil, err
	}

	instance, err := t.b.c.GetSafe(ctx, t.thread, t.key, collection.WithIdentity(t.identity))
	if err != nil {
		return nil, err
	}
	if root != nil && root.String() != instance.Path {
		return nil, ErrNonFastForward
	}

	md, mdPath, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	var target collection.Metadata
	if mdPath != pth { // If the metadata is inherited from a parent, create a new entry
		target = collection.Metadata{
			Info: make(map[string]interface{}),
		}
	} else {
		target = md
	}
	if target.Info == nil {
		target.Info = make(map[string]interface{})
	}

	var changed bool
	for k, v := range info {
		if x, ok := target.Info[k]; ok && x == v {
			continue
		}
		target.Info[k] = v
		changed = true
	}
	if changed {
		instance.UpdatedAt = time.Now().UnixNano()
		target.UpdatedAt = instance.UpdatedAt
		instance.Metadata[pth] = target
		if err := t.b.c.Save(ctx, t.thread, instance, collection.WithIdentity(t.identity)); err != nil {
			return nil, err
		}
	}

	log.Debugf("pushed info for %s in %s", pth, t.key)
	return instanceToBucket(t.thread, instance), nil
}

// PullPathInfo pulls all info at a path.
func (b *Buckets) PullPathInfo(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pth string,
) (map[string]interface{}, error) {
	if err := thread.Validate(); err != nil {
		return nil, fmt.Errorf("invalid thread id: %v", err)
	}
	pth, err := parsePath(pth)
	if err != nil {
		return nil, err
	}
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return nil, err
	}
	md, _, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	if md.Info == nil {
		md.Info = make(map[string]interface{})
	}

	log.Debugf("pulled info for %s in %s", pth, key)
	return md.Info, nil
}
