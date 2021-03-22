package buckets

import (
	"context"
	"fmt"
	gopath "path"
	"strings"
	"time"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

func (b *Buckets) MovePath(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	root path.Resolved,
	fpth, tpth string,
) (int64, *Bucket, error) {
	txn := b.NewTxn(thread, key, identity)
	defer txn.Close()
	return txn.MovePath(ctx, root, fpth, tpth)
}

func (t *Txn) MovePath(
	ctx context.Context,
	root path.Resolved,
	fpth, tpth string,
) (int64, *Bucket, error) {
	fpth, err := parsePath(fpth)
	if err != nil {
		return 0, nil, err
	}
	if fpth == "" {
		// @todo: enable move of root directory
		return 0, nil, fmt.Errorf("root cannot be moved")
	}
	tpth, err = parsePath(tpth)
	if err != nil {
		return 0, nil, err
	}
	// Paths are the same, nothing to do
	if fpth == tpth {
		return 0, nil, fmt.Errorf("path is destination")
	}

	instance, pth, err := t.b.getBucketAndPath(ctx, t.thread, t.key, t.identity, fpth)
	if err != nil {
		return 0, nil, fmt.Errorf("getting path: %v", err)
	}
	if root != nil && root.String() != instance.Path {
		return 0, nil, ErrNonFastForward
	}

	instance.UpdatedAt = time.Now().UnixNano()
	instance.SetMetadataAtPath(tpth, collection.Metadata{
		UpdatedAt: instance.UpdatedAt,
	})
	instance.UnsetMetadataWithPrefix(fpth + "/")
	if err := t.b.c.Verify(ctx, t.thread, instance, collection.WithIdentity(t.identity)); err != nil {
		return 0, nil, fmt.Errorf("verifying bucket update: %v", err)
	}

	fbpth, err := getBucketPath(instance, fpth)
	if err != nil {
		return 0, nil, err
	}
	fitem, err := t.b.pathToItem(ctx, instance, fbpth, false)
	if err != nil {
		return 0, nil, err
	}
	tbpth, err := getBucketPath(instance, tpth)
	if err != nil {
		return 0, nil, err
	}
	titem, err := t.b.pathToItem(ctx, instance, tbpth, false)
	if err == nil {
		if fitem.IsDir && !titem.IsDir {
			return 0, nil, fmt.Errorf("destination is not a directory")
		}
		if titem.IsDir {
			// from => to becomes new dir:
			//   - "c" => "b" becomes "b/c"
			//   - "a.jpg" => "b" becomes "b/a.jpg"
			tpth = gopath.Join(tpth, fitem.Name)
		}
	}

	pnode, err := dag.GetNodeAtPath(ctx, t.b.ipfs, pth, instance.GetLinkEncryptionKey())
	if err != nil {
		return 0, nil, fmt.Errorf("getting node: %v", err)
	}

	var dirPath path.Resolved
	if instance.IsPrivate() {
		ctx, dirPath, err = dag.CopyDag(ctx, t.b.ipfs, instance, pnode, fpth, tpth)
		if err != nil {
			return 0, nil, fmt.Errorf("copying node: %v", err)
		}
	} else {
		ctx, dirPath, err = t.b.setPathFromExistingCid(
			ctx,
			instance,
			path.New(instance.Path),
			tpth,
			pnode.Cid(),
			nil,
			nil,
		)
		if err != nil {
			return 0, nil, fmt.Errorf("copying path: %v", err)
		}
	}
	instance.Path = dirPath.String()

	// If "a/b" => "a/", cleanup only needed for priv
	if strings.HasPrefix(fpth, tpth) {
		if instance.IsPrivate() {
			ctx, dirPath, err = t.b.removePath(ctx, instance, fpth)
			if err != nil {
				return 0, nil, fmt.Errorf("removing path: %v", err)
			}
			instance.Path = dirPath.String()
		}

		if err := t.b.saveAndPublish(ctx, t.thread, t.identity, instance); err != nil {
			return 0, nil, err
		}

		log.Debugf("moved %s to %s", fpth, tpth)
		return dag.GetPinnedBytes(ctx), instanceToBucket(t.thread, instance), nil
	}

	if strings.HasPrefix(tpth, fpth) {
		// If "a/" => "a/b" cleanup each leaf in "a" that isn't "b" (skipping .textileseed)
		ppth := path.Join(path.New(instance.Path), fpth)
		item, err := t.b.listPath(ctx, instance, ppth)
		if err != nil {
			return 0, nil, fmt.Errorf("listing path: %v", err)
		}
		for _, chld := range item.Items {
			sp := trimSlash(movePathRegexp.ReplaceAllString(chld.Path, ""))
			if strings.Compare(chld.Name, collection.SeedName) == 0 || sp == tpth {
				continue
			}
			ctx, dirPath, err = t.b.removePath(ctx, instance, trimSlash(sp))
			if err != nil {
				return 0, nil, fmt.Errorf("removing path: %v", err)
			}
			instance.Path = dirPath.String()
		}
	} else {
		// if a/ => b/ remove a
		ctx, dirPath, err = t.b.removePath(ctx, instance, fpth)
		if err != nil {
			return 0, nil, fmt.Errorf("removing path: %v", err)
		}
		instance.Path = dirPath.String()
	}

	if err := t.b.saveAndPublish(ctx, t.thread, t.identity, instance); err != nil {
		return 0, nil, err
	}

	log.Debugf("moved %s to %s", fpth, tpth)
	return dag.GetPinnedBytes(ctx), instanceToBucket(t.thread, instance), nil
}
