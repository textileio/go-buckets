package dag

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	gopath "path"
	"strings"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets/collection"
)

// MakeBucketSeed returns a raw ipld node containing a random seed.
func MakeBucketSeed(key []byte) (ipld.Node, error) {
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	// Encrypt seed if key is set (treating the seed differently here would complicate bucket reading)
	if key != nil {
		var err error
		seed, err = EncryptData(seed, nil, key)
		if err != nil {
			return nil, err
		}
	}
	return dag.NewRawNode(seed), nil
}

// CreateBucketPath creates an IPFS path which only contains the seed file.
// The returned path will be pinned.
func CreateBucketPath(
	ctx context.Context,
	ipfs iface.CoreAPI,
	seed ipld.Node,
	key []byte,
) (context.Context, path.Resolved, error) {
	// Create the initial bucket directory
	n, err := newDirWithNode(seed, collection.SeedName, key)
	if err != nil {
		return ctx, nil, err
	}
	if err = ipfs.Dag().AddMany(ctx, []ipld.Node{n, seed}); err != nil {
		return ctx, nil, err
	}
	pins := []ipld.Node{n}
	if key != nil {
		pins = append(pins, seed)
	}
	ctx, err = PinBlocks(ctx, ipfs, pins)
	if err != nil {
		return ctx, nil, err
	}
	return ctx, path.IpfsPath(n.Cid()), nil
}

// CreateBucketPathWithCid creates an IPFS path from a UnixFS dag.
// with collection.SeedName seed file added to the root of the dag.
// The returned path will be pinned.
func CreateBucketPathWithCid(
	ctx context.Context,
	ipfs iface.CoreAPI,
	buckPath string,
	cid cid.Cid,
	linkKey,
	fileKey []byte,
	seed ipld.Node,
) (context.Context, path.Resolved, error) {
	cidPath := path.IpfsPath(cid)
	cidSize, err := GetPathSize(ctx, ipfs, cidPath)
	if err != nil {
		return ctx, nil, fmt.Errorf("resolving boot cid node: %v", err)
	}

	// Check context owner's storage allowance
	owner, ok := OwnerFromContext(ctx)
	if ok && cidSize > owner.StorageAvailable {
		return ctx, nil, ErrStorageQuotaExhausted
	}

	// Here we have to walk and possibly encrypt the boot path dag
	n, nodes, err := NewDirFromExistingPath(
		ctx,
		ipfs,
		cidPath,
		buckPath,
		linkKey,
		fileKey,
		seed,
		collection.SeedName,
	)
	if err != nil {
		return ctx, nil, err
	}
	if err = ipfs.Dag().AddMany(ctx, nodes); err != nil {
		return ctx, nil, err
	}
	var pins []ipld.Node
	if linkKey != nil {
		pins = nodes
	} else {
		pins = []ipld.Node{n}
	}
	ctx, err = PinBlocks(ctx, ipfs, pins)
	if err != nil {
		return ctx, nil, err
	}
	return ctx, path.IpfsPath(n.Cid()), nil
}

// newDirWithNode returns a new proto node directory wrapping the node,
// which is encrypted if key is not nil.
func newDirWithNode(n ipld.Node, name string, key []byte) (ipld.Node, error) {
	dir := unixfs.EmptyDirNode()
	dir.SetCidBuilder(dag.V1CidPrefix())
	if err := dir.AddNodeLink(name, n); err != nil {
		return nil, err
	}
	return EncryptNode(dir, key)
}

// NewDirFromExistingPath returns a new dir based on path.
// If keys are not nil, this method recursively walks the path, encrypting files and directories.
// If add is not nil, it will be included in the resulting (possibly encrypted) node under a link named addName.
// This method returns the root node and a list of all new nodes (which also includes the root).
func NewDirFromExistingPath(
	ctx context.Context,
	ipfs iface.CoreAPI,
	pth path.Path,
	destPath string,
	linkKey,
	fileKey []byte,
	add ipld.Node,
	addName string,
) (ipld.Node, []ipld.Node, error) {
	rn, err := ipfs.ResolveNode(ctx, pth)
	if err != nil {
		return nil, nil, err
	}
	top, ok := rn.(*dag.ProtoNode)
	if !ok {
		return nil, nil, dag.ErrNotProtobuf
	}
	if linkKey == nil && fileKey == nil {
		nodes := []ipld.Node{top}
		if add != nil {
			if err := top.AddNodeLink(addName, add); err != nil {
				return nil, nil, err
			}
			nodes = append(nodes, add)
		}
		return top, nodes, nil
	} else if linkKey == nil || fileKey == nil {
		return nil, nil, fmt.Errorf("invalid link or file key")
	}

	// Walk the node, encrypting the leaves and directories
	var addNode *NamedNode
	if add != nil {
		addNode = &NamedNode{
			Name: addName,
			Node: add,
		}
	}
	nmap, err := EncryptDag(
		ctx,
		ipfs,
		top,
		destPath,
		linkKey,
		nil,
		nil,
		fileKey,
		addNode,
	)
	if err != nil {
		return nil, nil, err
	}

	// Collect new nodes
	nodes := make([]ipld.Node, len(nmap))
	i := 0
	for _, tn := range nmap {
		nodes[i] = tn.Node
		i++
	}
	return nmap[top.Cid()].Node, nodes, nil
}

// GetNodeAtPath returns the node at path by traversing and potentially decrypting parent nodes.
func GetNodeAtPath(
	ctx context.Context,
	ipfs iface.CoreAPI,
	pth path.Path,
	key []byte,
) (ipld.Node, error) {
	if key != nil {
		rp, fp, err := ParsePath(pth)
		if err != nil {
			return nil, err
		}
		np, _, r, err := GetNodesToPath(ctx, ipfs, rp, fp, key)
		if err != nil {
			return nil, err
		}
		if r != "" {
			return nil, fmt.Errorf("could not resolve path: %s", pth)
		}
		return np[len(np)-1].New, nil
	} else {
		rp, err := ipfs.ResolvePath(ctx, pth)
		if err != nil {
			return nil, err
		}
		return ipfs.Dag().Get(ctx, rp.Cid())
	}
}

// ResolveNodeAtPath returns the decrypted node at path and whether or not it is a directory.
func ResolveNodeAtPath(
	ctx context.Context,
	ipfs iface.CoreAPI,
	pth path.Resolved,
	key []byte,
) (ipld.Node, bool, error) {
	cn, err := ipfs.ResolveNode(ctx, pth)
	if err != nil {
		return nil, false, err
	}
	return DecryptNode(cn, key)
}

type PathNode struct {
	Old     path.Resolved
	New     ipld.Node
	Name    string
	IsJoint bool
}

// GetNodesToPath returns a list of pathNodes that point to the path,
// The remaining path segment that was not resolvable is also returned.
func GetNodesToPath(
	ctx context.Context,
	ipfs iface.CoreAPI,
	base path.Resolved,
	pth string,
	key []byte,
) (nodes []PathNode, isDir bool, remainder string, err error) {
	top, dir, err := ResolveNodeAtPath(ctx, ipfs, base, key)
	if err != nil {
		return
	}
	nodes = append(nodes, PathNode{Old: base, New: top})
	remainder = pth
	if remainder == "" {
		return
	}
	parts := strings.Split(pth, "/")
	for i := 0; i < len(parts); i++ {
		l := getLink(top.Links(), parts[i])
		if l != nil {
			p := path.IpfsPath(l.Cid)
			top, dir, err = ResolveNodeAtPath(ctx, ipfs, p, key)
			if err != nil {
				return
			}
			nodes = append(nodes, PathNode{Old: p, New: top, Name: parts[i]})
		} else {
			remainder = strings.Join(parts[i:], "/")
			return nodes, dir, remainder, nil
		}
	}
	return nodes, dir, "", nil
}

func getLink(lnks []*ipld.Link, name string) *ipld.Link {
	for _, l := range lnks {
		if l.Name == name {
			return l
		}
	}
	return nil
}

// InsertNodeAtPath inserts a node at the location of path.
// Key will be required if the path is encrypted.
func InsertNodeAtPath(
	ctx context.Context,
	ipfs iface.CoreAPI,
	child ipld.Node,
	pth path.Path,
	key []byte,
) (context.Context, path.Resolved, error) {
	// The first step here is find a resolvable list of nodes that point to path.
	rp, fp, err := ParsePath(pth)
	if err != nil {
		return ctx, nil, err
	}
	fd, fn := gopath.Split(fp)
	fd = strings.TrimSuffix(fd, "/")
	np, _, r, err := GetNodesToPath(ctx, ipfs, rp, fd, key)
	if err != nil {
		return ctx, nil, err
	}
	r = gopath.Join(r, fn)

	// If the remaining path segment is not empty, we need to create each one
	// starting at the other end and walking back up to the deepest node
	// in the node path.
	parts := strings.Split(r, "/")
	news := make([]ipld.Node, len(parts)-1)
	cn := child
	for i := len(parts) - 1; i > 0; i-- {
		n, err := newDirWithNode(cn, parts[i], key)
		if err != nil {
			return ctx, nil, err
		}
		news[i-1] = cn
		cn = n
	}
	np = append(np, PathNode{New: cn, Name: parts[0], IsJoint: true})

	// Now, we have a full list of nodes to the insert location,
	// but the existing nodes need to be updated and re-encrypted.
	change := make([]ipld.Node, len(np))
	for i := len(np) - 1; i >= 0; i-- {
		change[i] = np[i].New
		if i > 0 {
			p, ok := np[i-1].New.(*dag.ProtoNode)
			if !ok {
				return ctx, nil, dag.ErrNotProtobuf
			}
			if np[i].IsJoint {
				xn, err := p.GetLinkedNode(ctx, ipfs.Dag(), np[i].Name)
				if err != nil && !errors.Is(err, dag.ErrLinkNotFound) {
					return ctx, nil, err
				}
				if xn != nil {
					np[i].Old = path.IpfsPath(xn.Cid())
					if err := p.RemoveNodeLink(np[i].Name); err != nil {
						return ctx, nil, err
					}
				}
			} else {
				xl, err := p.GetNodeLink(np[i].Name)
				if err != nil && !errors.Is(err, dag.ErrLinkNotFound) {
					return ctx, nil, err
				}
				if xl != nil {
					if err := p.RemoveNodeLink(np[i].Name); err != nil {
						return ctx, nil, err
					}
				}
			}
			if err := p.AddNodeLink(np[i].Name, np[i].New); err != nil {
				return ctx, nil, err
			}
			np[i-1].New, err = EncryptNode(p, key)
			if err != nil {
				return ctx, nil, err
			}
		}
	}

	// Add all the _new_ nodes, which is the sum of the brand new ones
	// from the missing path segment, and the changed ones from
	// the existing path.
	allNews := append(news, change...)
	if err := ipfs.Dag().AddMany(ctx, allNews); err != nil {
		return ctx, nil, err
	}
	// Pin brand new nodes
	ctx, err = PinBlocks(ctx, ipfs, news)
	if err != nil {
		return ctx, nil, err
	}

	// Update changed node pins
	for _, n := range np {
		if n.Old != nil && n.IsJoint {
			ctx, err = UnpinBranch(ctx, ipfs, n.Old, key)
			if err != nil {
				return ctx, nil, err
			}
		}
		ctx, err = UpdateOrAddPin(ctx, ipfs, n.Old, path.IpfsPath(n.New.Cid()))
		if err != nil {
			return ctx, nil, err
		}
	}
	return ctx, path.IpfsPath(np[0].New.Cid()), nil
}

// RemoveNodeAtPath removes node at the location of path.
// Key will be required if the path is encrypted.
func RemoveNodeAtPath(
	ctx context.Context,
	ipfs iface.CoreAPI,
	pth path.Path,
	key []byte,
) (context.Context, path.Resolved, error) {
	// The first step here is find a resolvable list of nodes that point to path.
	rp, fp, err := ParsePath(pth)
	if err != nil {
		return ctx, nil, err
	}
	np, _, r, err := GetNodesToPath(ctx, ipfs, rp, fp, key)
	if err != nil {
		return ctx, nil, err
	}
	// If the remaining path segment is not empty, then we conclude that
	// the node cannot be resolved.
	if r != "" {
		return ctx, nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	np[len(np)-1].IsJoint = true

	// Now, we have a full list of nodes to the insert location,
	// but the existing nodes need to be updated and re-encrypted.
	change := make([]ipld.Node, len(np)-1)
	for i := len(np) - 1; i >= 0; i-- {
		if i < len(np)-1 {
			change[i] = np[i].New
		}
		if i > 0 {
			p, ok := np[i-1].New.(*dag.ProtoNode)
			if !ok {
				return ctx, nil, dag.ErrNotProtobuf
			}
			if err := p.RemoveNodeLink(np[i].Name); err != nil {
				return ctx, nil, err
			}
			if !np[i].IsJoint {
				if err := p.AddNodeLink(np[i].Name, np[i].New); err != nil {
					return ctx, nil, err
				}
			}
			np[i-1].New, err = EncryptNode(p, key)
			if err != nil {
				return ctx, nil, err
			}
		}
	}

	// Add all the changed nodes
	if err := ipfs.Dag().AddMany(ctx, change); err != nil {
		return ctx, nil, err
	}
	// Update / remove node pins
	for _, n := range np {
		if n.IsJoint {
			ctx, err = UnpinNodeAndBranch(ctx, ipfs, n.Old, key)
			if err != nil {
				return ctx, nil, err
			}
		} else {
			ctx, err = UpdateOrAddPin(ctx, ipfs, n.Old, path.IpfsPath(n.New.Cid()))
			if err != nil {
				return ctx, nil, err
			}
		}
	}
	return ctx, path.IpfsPath(np[0].New.Cid()), nil
}

// GetPathSize returns the cummulative size of root. If root is nil, it returns 0.
func GetPathSize(ctx context.Context, ipfs iface.CoreAPI, root path.Path) (int64, error) {
	if root == nil {
		return 0, nil
	}
	stat, err := ipfs.Object().Stat(ctx, root)
	if err != nil {
		return 0, fmt.Errorf("getting dag size: %v", err)
	}
	return int64(stat.CumulativeSize), nil
}

// NewResolvedPath returns path.Resolved from a string.
func NewResolvedPath(s string) (path.Resolved, error) {
	parts := strings.SplitN(s, "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("path is not resolvable")
	}
	c, err := cid.Decode(parts[2])
	if err != nil {
		return nil, err
	}
	return path.IpfsPath(c), nil
}

// ParsePath returns path.Resolved and a path remainder from path.Path.
func ParsePath(p path.Path) (resolved path.Resolved, fpath string, err error) {
	parts := strings.SplitN(p.String(), "/", 4)
	if len(parts) < 3 {
		err = fmt.Errorf("path does not contain a resolvable segment")
		return
	}
	c, err := cid.Decode(parts[2])
	if err != nil {
		return
	}
	if len(parts) > 3 {
		fpath = parts[3]
	}
	return path.IpfsPath(c), fpath, nil
}
