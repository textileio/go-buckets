package buckets

import (
	"context"
	"fmt"
	gopath "path"
	"strings"

	c "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	ifaceopts "github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

func parsePath(pth string) (fpth string, err error) {
	if strings.Contains(pth, collection.SeedName) {
		err = fmt.Errorf("paths containing %s are not allowed", collection.SeedName)
		return
	}
	return trimSlash(pth), nil
}

// trimSlash removes a slash prefix from the path.
func trimSlash(pth string) string {
	return strings.TrimPrefix(pth, "/")
}

// listPaths returns a PathItem describing the bucket path.
func (b *Buckets) listPath(ctx context.Context, bucket *collection.Bucket, pth path.Path) (*PathItem, error) {
	item, err := b.pathToItem(ctx, bucket, pth, true)
	if err != nil {
		return nil, err
	}
	if pth.String() == bucket.Path {
		item.Name = bucket.Name
	}
	return item, nil
}

// pathToItem returns items at path, optionally including one level down of links.
func (b *Buckets) pathToItem(
	ctx context.Context,
	bucket *collection.Bucket,
	pth path.Path,
	includeNextLevel bool,
) (*PathItem, error) {
	var linkKey []byte
	if bucket != nil {
		linkKey = bucket.GetLinkEncryptionKey()
	}
	n, err := dag.GetNodeAtPath(ctx, b.ipfs, pth, linkKey)
	if err != nil {
		return nil, err
	}
	return b.nodeToItem(ctx, bucket, n, pth.String(), linkKey, false, includeNextLevel)
}

// nodeToItem returns a path item from an IPLD node.
func (b *Buckets) nodeToItem(
	ctx context.Context,
	bucket *collection.Bucket,
	node ipld.Node,
	pth string,
	key []byte,
	decrypt,
	includeNextLevel bool,
) (*PathItem, error) {
	if decrypt && key != nil {
		var err error
		node, _, err = dag.DecryptNode(node, key)
		if err != nil {
			return nil, err
		}
	}

	stat, err := node.Stat()
	if err != nil {
		return nil, err
	}
	item := &PathItem{
		Cid:  node.Cid().String(),
		Name: gopath.Base(pth),
		Path: pth,
		Size: int64(stat.CumulativeSize),
	}

	if bucket != nil {
		name := trimSlash(strings.TrimPrefix(pth, bucket.Path))
		md, _, ok := bucket.GetMetadataForPath(name, false)
		if !ok {
			return nil, fmt.Errorf("could not resolve path: %s", pth)
		}
		item.Metadata = md
	}

	if pn, ok := node.(*mdag.ProtoNode); ok {
		fn, _ := unixfs.FSNodeFromBytes(pn.Data())
		if fn != nil && fn.IsDir() {
			item.IsDir = true
		}
	}
	if item.IsDir {
		for _, l := range node.Links() {
			if l.Name == "" {
				break
			}
			if includeNextLevel {
				p := gopath.Join(pth, l.Name)
				n, err := l.GetNode(ctx, b.ipfs.Dag())
				if err != nil {
					return nil, err
				}
				i, err := b.nodeToItem(ctx, bucket, n, p, key, true, false)
				if err != nil {
					return nil, err
				}
				item.Items = append(item.Items, *i)
			}
			item.ItemsCount++
		}
	}
	return item, nil
}

// removePath removes a path from a bucket.
func (b *Buckets) removePath(
	ctx context.Context,
	instance *collection.Bucket,
	pth string,
) (context.Context, path.Resolved, error) {
	bpth := path.New(instance.Path)
	var dirPath path.Resolved
	var err error
	if instance.IsPrivate() {
		ctx, dirPath, err = dag.RemoveNodeAtPath(
			ctx,
			b.ipfs,
			path.Join(bpth, pth),
			instance.GetLinkEncryptionKey(),
		)
		if err != nil {
			return ctx, nil, fmt.Errorf("remove node failed: %v", err)
		}
	} else {
		dirPath, err = b.ipfs.Object().RmLink(ctx, bpth, pth)
		if err != nil {
			return ctx, nil, err
		}
		ctx, err = dag.UpdateOrAddPin(ctx, b.ipfs, bpth, dirPath)
		if err != nil {
			return ctx, nil, fmt.Errorf("update pin failed: %v", err)
		}
	}
	return ctx, dirPath, nil
}

// getBucketAndPath returns a bucket instance and full bucket path.
func (b *Buckets) getBucketAndPath(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pth string,
) (*collection.Bucket, path.Path, error) {
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return nil, nil, err
	}
	bpth, err := getBucketPath(instance, pth)
	return instance, bpth, err
}

func getBucketPath(bucket *collection.Bucket, pth string) (path.Path, error) {
	bpth := path.New(gopath.Join(bucket.Path, pth))
	if err := bpth.IsValid(); err != nil {
		return nil, err
	}
	return bpth, nil
}

// setPathFromExistingCid sets the path with a cid from the network, encrypting with file key if present.
func (b *Buckets) setPathFromExistingCid(
	ctx context.Context,
	instance *collection.Bucket,
	buckPath path.Path,
	destPath string,
	cid c.Cid,
	linkKey,
	fileKey []byte,
) (context.Context, path.Resolved, error) {
	var dirPath path.Resolved
	if destPath == "" {
		sn, err := dag.MakeBucketSeed(fileKey)
		if err != nil {
			return ctx, nil, err
		}
		ctx, dirPath, err = dag.CreateBucketPathWithCid(ctx, b.ipfs, destPath, cid, linkKey, fileKey, sn)
		if err != nil {
			return ctx, nil, fmt.Errorf("generating bucket new root: %v", err)
		}
		if instance.IsPrivate() {
			buckPathResolved, err := b.ipfs.ResolvePath(ctx, buckPath)
			if err != nil {
				return ctx, nil, fmt.Errorf("resolving path: %v", err)
			}
			ctx, err = dag.UnpinNodeAndBranch(ctx, b.ipfs, buckPathResolved, linkKey)
			if err != nil {
				return ctx, nil, fmt.Errorf("unpinning pinned root: %v", err)
			}
		} else {
			ctx, err = dag.UnpinPath(ctx, b.ipfs, buckPath)
			if err != nil {
				return ctx, nil, fmt.Errorf("updating pinned root: %v", err)
			}
		}
	} else {
		bootPath := path.IpfsPath(cid)
		if instance.IsPrivate() {
			n, nodes, err := dag.NewDirFromExistingPath(
				ctx,
				b.ipfs,
				bootPath,
				destPath,
				linkKey,
				fileKey,
				nil,
				"",
			)
			if err != nil {
				return ctx, nil, fmt.Errorf("resolving remote path: %v", err)
			}
			ctx, dirPath, err = dag.InsertNodeAtPath(ctx, b.ipfs, n, path.Join(buckPath, destPath), linkKey)
			if err != nil {
				return ctx, nil, fmt.Errorf("updating pinned root: %v", err)
			}
			ctx, err = dag.AddAndPinNodes(ctx, b.ipfs, nodes)
			if err != nil {
				return ctx, nil, err
			}
		} else {
			var err error
			dirPath, err = b.ipfs.Object().AddLink(
				ctx,
				buckPath,
				destPath,
				bootPath,
				ifaceopts.Object.Create(true),
			)
			if err != nil {
				return ctx, nil, fmt.Errorf("adding folder: %v", err)
			}
			ctx, err = dag.UpdateOrAddPin(ctx, b.ipfs, buckPath, dirPath)
			if err != nil {
				return ctx, nil, fmt.Errorf("updating pinned root: %v", err)
			}
		}
	}
	return ctx, dirPath, nil
}
