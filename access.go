package buckets

import (
	"context"
	"fmt"
	"time"

	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

// PushPathAccessRoles pushes new access roles to bucket paths.
// Access roles are keyed by did.DID.
// Roles are inherited by path children.
func (b *Buckets) PushPathAccessRoles(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	root path.Resolved,
	pth string,
	roles map[did.DID]collection.Role,
) (int64, *Bucket, error) {
	txn, err := b.NewTxn(thread, key, identity)
	if err != nil {
		return 0, nil, err
	}
	defer txn.Close()
	return txn.PushPathAccessRoles(ctx, root, pth, roles)
}

// PushPathAccessRoles is Txn based PushPathInfo.
func (t *Txn) PushPathAccessRoles(
	ctx context.Context,
	root path.Resolved,
	pth string,
	roles map[did.DID]collection.Role,
) (int64, *Bucket, error) {
	pth, err := parsePath(pth)
	if err != nil {
		return 0, nil, err
	}

	instance, bpth, err := t.b.getBucketAndPath(ctx, t.thread, t.key, t.identity, pth)
	if err != nil {
		return 0, nil, err
	}
	if root != nil && root.String() != instance.Path {
		return 0, nil, ErrNonFastForward
	}

	linkKey := instance.GetLinkEncryptionKey()
	pathNode, err := dag.GetNodeAtPath(ctx, t.b.ipfs, bpth, linkKey)
	if err != nil {
		return 0, nil, err
	}

	md, mdPath, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return 0, nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	var target collection.Metadata
	if mdPath != pth { // If the metadata is inherited from a parent, create a new entry
		target = collection.Metadata{
			Roles: make(map[did.DID]collection.Role),
		}
	} else {
		target = md
	}
	var currentFileKeys map[string][]byte
	if instance.IsPrivate() {
		currentFileKeys, err = instance.GetFileEncryptionKeysForPrefix(pth)
		if err != nil {
			return 0, nil, err
		}
		newFileKey, err := dcrypto.NewKey()
		if err != nil {
			return 0, nil, err
		}
		target.SetFileEncryptionKey(newFileKey) // Create or refresh the file key
	}

	var changed bool
	for k, r := range roles {
		if x, ok := target.Roles[k]; ok && x == r {
			continue
		}
		if r > collection.NoneRole {
			target.Roles[k] = r
		} else {
			delete(target.Roles, k)
		}
		changed = true
	}
	if changed {
		instance.UpdatedAt = time.Now().UnixNano()
		target.UpdatedAt = instance.UpdatedAt
		instance.Metadata[pth] = target
		if instance.IsPrivate() {
			if err := instance.RotateFileEncryptionKeysForPrefix(pth); err != nil {
				return 0, nil, err
			}
		}
		if err := t.b.c.Verify(ctx, t.thread, instance, collection.WithIdentity(t.identity)); err != nil {
			return 0, nil, err
		}

		if instance.IsPrivate() {
			newFileKeys, err := instance.GetFileEncryptionKeysForPrefix(pth)
			if err != nil {
				return 0, nil, err
			}
			nmap, err := dag.EncryptDag(
				ctx,
				t.b.ipfs,
				pathNode,
				pth,
				linkKey,
				currentFileKeys,
				newFileKeys,
				nil,
				nil,
			)
			if err != nil {
				return 0, nil, err
			}
			nodes := make([]ipld.Node, len(nmap))
			i := 0
			for _, tn := range nmap {
				nodes[i] = tn.Node
				i++
			}
			pn := nmap[pathNode.Cid()].Node
			var dirPath path.Resolved
			ctx, dirPath, err = dag.InsertNodeAtPath(ctx, t.b.ipfs, pn, path.Join(path.New(instance.Path), pth), linkKey)
			if err != nil {
				return 0, nil, err
			}
			ctx, err = dag.AddAndPinNodes(ctx, t.b.ipfs, nodes)
			if err != nil {
				return 0, nil, err
			}
			instance.Path = dirPath.String()
		}

		if err := t.b.c.Save(ctx, t.thread, instance, collection.WithIdentity(t.identity)); err != nil {
			return 0, nil, err
		}
	}

	log.Debugf("pushed access roles for %s in %s", pth, t.key)
	return dag.GetPinnedBytes(ctx), instanceToBucket(t.thread, instance), nil
}

// PullPathAccessRoles pulls access roles for a bucket path.
func (b *Buckets) PullPathAccessRoles(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pth string,
) (map[did.DID]collection.Role, error) {
	if err := thread.Validate(); err != nil {
		return nil, fmt.Errorf("invalid thread id: %v", err)
	}
	pth, err := parsePath(pth)
	if err != nil {
		return nil, err
	}
	instance, bpth, err := b.getBucketAndPath(ctx, thread, key, identity, pth)
	if err != nil {
		return nil, err
	}
	if _, err = dag.GetNodeAtPath(ctx, b.ipfs, bpth, instance.GetLinkEncryptionKey()); err != nil {
		return nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	md, _, ok := instance.GetMetadataForPath(pth, false)
	if !ok {
		return nil, fmt.Errorf("could not resolve path: %s", pth)
	}

	log.Debugf("pulled access roles for %s in %s", pth, key)
	return md.Roles, nil
}

// IsReadablePath returns whether or not a path is readable by an identity.
func (b *Buckets) IsReadablePath(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pth string,
) (bool, error) {
	if err := thread.Validate(); err != nil {
		return false, fmt.Errorf("invalid thread id: %v", err)
	}
	pth, err := parsePath(pth)
	if err != nil {
		return false, err
	}
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return false, err
	}
	_, doc, err := core.Validate(identity, nil)
	if err != nil {
		return false, err
	}
	return instance.IsReadablePath(pth, doc.ID), nil
}

// IsWritablePath returns whether or not a path is writable by an identity.
func (b *Buckets) IsWritablePath(
	ctx context.Context,
	thread core.ID,
	key string,
	identity did.Token,
	pth string,
) (bool, error) {
	if err := thread.Validate(); err != nil {
		return false, fmt.Errorf("invalid thread id: %v", err)
	}
	pth, err := parsePath(pth)
	if err != nil {
		return false, err
	}
	instance, err := b.c.GetSafe(ctx, thread, key, collection.WithIdentity(identity))
	if err != nil {
		return false, err
	}
	_, doc, err := core.Validate(identity, nil)
	if err != nil {
		return false, err
	}
	return instance.IsWritablePath(pth, doc.ID), nil
}
