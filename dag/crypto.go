package dag

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	gopath "path"
	"sync"

	"github.com/ipfs/go-cid"
	ipfsfiles "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	iface "github.com/ipfs/interface-go-ipfs-core"
	iops "github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
	"github.com/textileio/go-buckets/collection"
	"golang.org/x/sync/errgroup"
)

// ErrInvalidNodeType indicates a node with type other than raw of proto was encountered.
var ErrInvalidNodeType = errors.New("invalid node type")

// EncryptData encrypts data with the new key, decrypting with current key if needed.
func EncryptData(data, currentKey, newKey []byte) ([]byte, error) {
	if currentKey != nil {
		var err error
		data, err = DecryptData(data, currentKey)
		if err != nil {
			return nil, err
		}
	}
	r, err := dcrypto.NewEncrypter(bytes.NewReader(data), newKey)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

// DecryptData decrypts data with key.
func DecryptData(data, key []byte) ([]byte, error) {
	r, err := dcrypto.NewDecrypter(bytes.NewReader(data), key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// EncryptNode returns the encrypted version of node if key is not nil.
func EncryptNode(n *mdag.ProtoNode, key []byte) (*mdag.ProtoNode, error) {
	if key == nil {
		return n, nil
	}
	cipher, err := EncryptData(n.RawData(), nil, key)
	if err != nil {
		return nil, err
	}
	en := mdag.NodeWithData(unixfs.FilePBData(cipher, uint64(len(cipher))))
	en.SetCidBuilder(mdag.V1CidPrefix())
	return en, nil
}

// DecryptNode returns a decrypted version of node and whether or not it is a directory.
func DecryptNode(cn ipld.Node, key []byte) (ipld.Node, bool, error) {
	switch cn := cn.(type) {
	case *mdag.RawNode:
		return cn, false, nil // All raw nodes will be leaves
	case *mdag.ProtoNode:
		if key == nil {
			return cn, false, nil // Could be a joint, but it's not important to know in the public case
		}
		fn, err := unixfs.FSNodeFromBytes(cn.Data())
		if err != nil {
			return nil, false, err
		}
		if fn.Data() == nil {
			return cn, false, nil // This node is a raw file wrapper
		}
		plain, err := DecryptData(fn.Data(), key)
		if err != nil {
			return nil, false, err
		}
		n, err := mdag.DecodeProtobuf(plain)
		if err != nil {
			return mdag.NewRawNode(plain), false, nil
		}
		n.SetCidBuilder(mdag.V1CidPrefix())
		return n, true, nil
	default:
		return nil, false, ErrInvalidNodeType
	}
}

// NamedNode describes an encrypted node.
type NamedNode struct {
	Name string
	Path string
	Node ipld.Node
	Cid  cid.Cid
}

// NamedNodes is a unique set of NamedNodes.
type NamedNodes struct {
	sync.RWMutex
	m map[cid.Cid]*NamedNode
}

func newNamedNodes() *NamedNodes {
	return &NamedNodes{
		m: make(map[cid.Cid]*NamedNode),
	}
}

// Get node by its original plaintext cid.
func (nn *NamedNodes) Get(c cid.Cid) *NamedNode {
	nn.RLock()
	defer nn.RUnlock()
	return nn.m[c]
}

// Store node by its original plaintext cid.
func (nn *NamedNodes) Store(c cid.Cid, n *NamedNode) {
	nn.Lock()
	defer nn.Unlock()
	nn.m[c] = n
}

// EncryptDag creates an encrypted version of root that includes all child nodes.
// Leaf nodes are encrypted and linked to parents, which are then encrypted and
// linked to their parents, and so on up to root.
// add will be added to the encrypted root node if not nil.
// This method returns a map of all nodes keyed by their _original_ plaintext cid,
// and a list of the root's direct links.
func EncryptDag(
	ctx context.Context,
	ipfs iface.CoreAPI,
	root ipld.Node,
	destPath string,
	linkKey []byte,
	currentFileKeys,
	newFileKeys map[string][]byte,
	newFileKey []byte,
	add *NamedNode,
) (map[cid.Cid]*NamedNode, error) {
	// Step 1: Create a preordered list of joint and leaf nodes
	var stack, joints []*NamedNode
	var cur *NamedNode
	jmap := make(map[cid.Cid]*NamedNode)
	lmap := make(map[cid.Cid]*NamedNode)

	stack = append(stack, &NamedNode{Node: root, Path: destPath})
	for len(stack) > 0 {
		n := len(stack) - 1
		cur = stack[n]
		stack = stack[:n]

		if _, ok := jmap[cur.Node.Cid()]; ok {
			continue
		}
		if _, ok := lmap[cur.Node.Cid()]; ok {
			continue
		}

		ds := ipfs.Dag()
	types:
		switch cur.Node.(type) {
		case *mdag.RawNode:
			lmap[cur.Node.Cid()] = cur
		case *mdag.ProtoNode:
			// Add links to the stack
			cur.Cid = cur.Node.Cid()
			if currentFileKeys != nil {
				var err error
				cur.Node, _, err = DecryptNode(cur.Node, linkKey)
				if err != nil {
					return nil, err
				}
			}
			for _, l := range cur.Node.Links() {
				if l.Name == "" {
					// We have discovered a raw file node wrapper
					// Use the original cur node because file node wrappers aren't encrypted
					lmap[cur.Cid] = cur
					break types
				}
				ln, err := l.GetNode(ctx, ds)
				if err != nil {
					return nil, err
				}
				stack = append(stack, &NamedNode{
					Name: l.Name,
					Path: gopath.Join(cur.Path, l.Name),
					Node: ln,
				})
			}
			joints = append(joints, cur)
			jmap[cur.Cid] = cur
		default:
			return nil, ErrInvalidNodeType
		}
	}

	// Step 2: Encrypt all leaf nodes in parallel
	nmap := newNamedNodes()
	eg, gctx := errgroup.WithContext(ctx)
	for _, l := range lmap {
		l := l
		cfk := getFileKey(nil, currentFileKeys, l.Path)
		nfk := getFileKey(newFileKey, newFileKeys, l.Path)
		if nfk == nil {
			// This shouldn't happen
			return nil, fmt.Errorf("new file key not found for path %s", l.Path)
		}
		eg.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			var cn ipld.Node
			switch l.Node.(type) {
			case *mdag.RawNode:
				data, err := EncryptData(l.Node.RawData(), cfk, nfk)
				if err != nil {
					return err
				}
				cn = mdag.NewRawNode(data)
			case *mdag.ProtoNode:
				var err error
				cn, err = EncryptFileNode(gctx, ipfs, l.Node, cfk, nfk)
				if err != nil {
					return err
				}
			}
			nmap.Store(l.Node.Cid(), &NamedNode{
				Name: l.Name,
				Node: cn,
			})
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Step 3: Encrypt joint nodes in reverse, walking up to root
	// Note: In the case where we're re-encrypting a dag, joints will already be decrypted.
	for i := len(joints) - 1; i >= 0; i-- {
		j := joints[i]
		jn := j.Node.(*mdag.ProtoNode)
		dir := unixfs.EmptyDirNode()
		dir.SetCidBuilder(mdag.V1CidPrefix())
		for _, l := range jn.Links() {
			ln := nmap.Get(l.Cid)
			if ln == nil {
				return nil, fmt.Errorf("link node not found")
			}
			if err := dir.AddNodeLink(ln.Name, ln.Node); err != nil {
				return nil, err
			}
		}
		if i == 0 && add != nil {
			if err := dir.AddNodeLink(add.Name, add.Node); err != nil {
				return nil, err
			}
			nmap.Store(add.Node.Cid(), add)
		}
		cn, err := EncryptNode(dir, linkKey)
		if err != nil {
			return nil, err
		}
		nmap.Store(j.Cid, &NamedNode{
			Name: j.Name,
			Node: cn,
		})
	}
	return nmap.m, nil
}

// CopyDag make a copy of a node, re-encrypting it if the source and destination paths have different file keys.
// The copied node will be pinned.
func CopyDag(
	ctx context.Context,
	ipfs iface.CoreAPI,
	buck *collection.Bucket,
	root ipld.Node,
	fromPath string,
	toPath string,
) (context.Context, path.Resolved, error) {
	fileKey, err := buck.GetFileEncryptionKeyForPath(toPath)
	if err != nil {
		return ctx, nil, err
	}

	currentFileKeys, err := buck.GetFileEncryptionKeysForPrefix(fromPath)
	if err != nil {
		return ctx, nil, err
	}
	newFileKeys, err := buck.GetFileEncryptionKeysForPrefix(toPath)
	if err != nil {
		return ctx, nil, err
	}
	nmap, err := EncryptDag(
		ctx,
		ipfs,
		root,
		fromPath,
		buck.GetLinkEncryptionKey(),
		currentFileKeys,
		newFileKeys,
		fileKey,
		nil,
	)
	if err != nil {
		return ctx, nil, err
	}
	nodes := make([]ipld.Node, len(nmap))
	i := 0
	for _, tn := range nmap {
		nodes[i] = tn.Node
		i++
	}
	pn := nmap[root.Cid()].Node
	ctx, dirPath, err := InsertNodeAtPath(
		ctx,
		ipfs,
		pn,
		path.Join(path.New(buck.Path), toPath),
		buck.GetLinkEncryptionKey(),
	)
	if err != nil {
		return ctx, nil, fmt.Errorf("inserting at path: %v", err)
	}

	// If updating root, add seedfile back to node.
	if toPath == "" {
		sn, err := MakeBucketSeed(fileKey)
		ctx, dirPath, err = InsertNodeAtPath(
			ctx,
			ipfs,
			sn,
			path.Join(dirPath, collection.SeedName),
			buck.GetLinkEncryptionKey(),
		)
		if err != nil {
			return ctx, nil, fmt.Errorf("replacing seedfile: %v", err)
		}
		nodes = append(nodes, sn)
	}

	ctx, err = AddAndPinNodes(ctx, ipfs, nodes)
	if err != nil {
		return ctx, nil, err
	}

	return ctx, dirPath, nil
}

func getFileKey(key []byte, pathKeys map[string][]byte, pth string) []byte {
	if pathKeys == nil {
		return key
	}
	k, ok := pathKeys[pth]
	if ok {
		return k
	}
	return key
}

// EncryptFileNode encrypts node with the new key, decrypting with current key if needed.
func EncryptFileNode(
	ctx context.Context,
	ipfs iface.CoreAPI,
	n ipld.Node,
	currentKey,
	newKey []byte,
) (ipld.Node, error) {
	fn, err := ipfs.Unixfs().Get(ctx, path.IpfsPath(n.Cid()))
	if err != nil {
		return nil, err
	}
	defer fn.Close()
	file := ipfsfiles.ToFile(fn)
	if file == nil {
		return nil, fmt.Errorf("node is a directory")
	}
	var r1 io.Reader
	if currentKey != nil {
		r, err := dcrypto.NewDecrypter(file, currentKey)
		if err != nil {
			return nil, err
		}
		r1 = r
		defer r.Close()
	} else {
		r1 = file
	}
	r2, err := dcrypto.NewEncrypter(r1, newKey)
	if err != nil {
		return nil, err
	}
	pth, err := ipfs.Unixfs().Add(
		ctx,
		ipfsfiles.NewReaderFile(r2),
		iops.Unixfs.CidVersion(1),
		iops.Unixfs.Pin(false),
	)
	if err != nil {
		return nil, err
	}
	return ipfs.ResolveNode(ctx, pth)
}
