package buckets

import (
	"context"
	"fmt"
	"time"

	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

func (b *Buckets) PushPathMetadata(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	info map[string]interface{},
	identity did.Token,
) error {
	lk := b.locks.Get(lock(key))
	lk.Acquire()
	defer lk.Release()

	pth, err := parsePath(pth)
	if err != nil {
		return err
	}

	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return err
	}

	md, mdPath, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return fmt.Errorf("could not resolve path: %s", pth)
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
			return err
		}
	}

	log.Debugf("pushed metadata for %s in %s", pth, key)
	return nil
}

func (b *Buckets) PullPathMetadata(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	identity did.Token,
) (md collection.Metadata, err error) {
	pth, err = parsePath(pth)
	if err != nil {
		return md, err
	}
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return md, err
	}
	md, _, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return md, fmt.Errorf("could not resolve path: %s", pth)
	}

	log.Debugf("pulled metadata for %s in %s", pth, key)
	return md, nil
}
