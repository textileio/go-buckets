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

func (b *Buckets) PushPathInfo(
	ctx context.Context,
	thread core.ID,
	key string,
	root path.Resolved,
	pth string,
	info map[string]interface{},
	identity did.Token,
) (*Bucket, error) {
	lk := b.locks.Get(lock(key))
	lk.Acquire()
	defer lk.Release()

	pth, err := parsePath(pth)
	if err != nil {
		return nil, err
	}

	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
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
		if err := b.c.Save(ctx, thread, instance, collection.WithIdentity(identity)); err != nil {
			return nil, err
		}
	}

	log.Debugf("pushed info for %s in %s", pth, key)
	return instanceToBucket(thread, instance), nil
}

func (b *Buckets) PullPathInfo(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	identity did.Token,
) (map[string]interface{}, error) {
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
