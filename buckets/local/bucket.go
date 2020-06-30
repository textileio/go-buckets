package local

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	bserv "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-flatfs"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	md "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-merkledag/dagutils"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipfs/go-unixfs/importer/trickle"
	options "github.com/ipfs/interface-go-ipfs-core/options"
	mh "github.com/multiformats/go-multihash"
)

func init() {
	bstore.BlockPrefix = ds.NewKey("")
	ipld.Register(cid.DagProtobuf, md.DecodeProtobufBlock)
	ipld.Register(cid.Raw, md.DecodeRawBlock)
	ipld.Register(cid.DagCBOR, cbor.DecodeBlock)
	gob.Register(pathMap{})
}

var (
	// ignoredFilenames is a list of default ignored file names.
	ignoredFilenames = []string{
		".DS_Store",
	}
)

const (
	// repoPath is the path to the local bucket repository.
	repoPath = ".textile/repo"

	// PatchExt is used to ignore tmp files during a pull.
	PatchExt = ".buckpatch"
)

// pathMap hold details about a local path map.
type pathMap struct {
	Local  cid.Cid
	Remote cid.Cid
}

// Bucket tracks a local bucket tree structure.
type Bucket struct {
	path   string
	ds     ds.Batching
	bsrv   bserv.BlockService
	dag    ipld.DAGService
	layout options.Layout
	cidver int
}

// NewBucket creates a new bucket with the given path.
func NewBucket(pth string, layout options.Layout) (*Bucket, error) {
	repo := filepath.Join(pth, repoPath)
	if err := os.MkdirAll(repo, os.ModePerm); err != nil {
		return nil, err
	}
	bd, err := flatfs.CreateOrOpen(repo, flatfs.NextToLast(2), true)
	if err != nil {
		return nil, err
	}
	bs := bstore.NewBlockstore(bd)
	bsrv := bserv.New(bs, offline.Exchange(bs))
	return &Bucket{
		path:   pth,
		ds:     bd,
		bsrv:   bsrv,
		dag:    md.NewDAGService(bsrv),
		layout: layout,
		cidver: 1,
	}, nil
}

// CidVersion returns the configured cid version (0 or 1).
// The default version is 1.
func (b *Bucket) SetCidVersion(v int) {
	b.cidver = v
}

// Root returns the local and remote root cids.
func (b *Bucket) Root() (local, remote cid.Cid, err error) {
	k, err := getPathKey("")
	if err != nil {
		return
	}
	pm, err := b.getPathMap(k)
	if err != nil {
		return
	}
	return pm.Local, pm.Remote, nil
}

// getPathKey returns a flatfs safe hash of a file path.
func getPathKey(pth string) (key ds.Key, err error) {
	hash, err := mh.Encode([]byte(filepath.Clean(pth)), mh.SHA2_256)
	if err != nil {
		return
	}
	return dshelp.MultihashToDsKey(hash), nil
}

// getPathMap returns details about a local path by key.
func (b *Bucket) getPathMap(k ds.Key) (m pathMap, err error) {
	v, err := b.ds.Get(k)
	if err != nil {
		return
	}
	dec := gob.NewDecoder(bytes.NewReader(v))
	var pm pathMap
	if err = dec.Decode(&pm); err != nil {
		return
	}
	return pm, nil
}

// Save saves the bucket as a node describing the file tree at the current path.
func (b *Bucket) Save(ctx context.Context) error {
	_, maps, err := b.recursiveAddPath(ctx, b.path, b.dag)
	if err != nil {
		return err
	}
	for p, c := range maps {
		if err := b.setLocalPath(p, c); err != nil {
			return err
		}
	}
	return nil
}

// setLocalPath sets the local cid for an existing path map.
func (b *Bucket) setLocalPath(pth string, local cid.Cid) error {
	k, err := getPathKey(pth)
	if err != nil {
		return err
	}
	xm, err := b.getPathMap(k)
	if errors.Is(err, ds.ErrNotFound) {
		return b.putPathMap(k, pathMap{Local: local})
	}
	if err != nil {
		return err
	}
	xm.Local = local
	return b.putPathMap(k, xm)
}

// putPathMap saves a path map under key.
func (b *Bucket) putPathMap(k ds.Key, pm pathMap) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(pm); err != nil {
		return err
	}
	return b.ds.Put(k, buf.Bytes())
}

// recursiveAddPath walks path and adds files to the dag service.
// This method returns the resulting root node and a list of path maps.
func (b *Bucket) recursiveAddPath(ctx context.Context, pth string, dag ipld.DAGService) (ipld.Node, map[string]cid.Cid, error) {
	root := unixfs.EmptyDirNode()
	prefix, err := md.PrefixForCidVersion(b.cidver)
	if err != nil {
		return nil, nil, err
	}
	root.SetCidBuilder(prefix)
	editor := dagutils.NewDagEditor(root, dag)
	maps := make(map[string]cid.Cid)
	abs, err := filepath.Abs(pth)
	if err != nil {
		return nil, nil, err
	}
	if err = filepath.Walk(abs, func(n string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if Ignore(n) {
				return nil
			}
			p := n
			n = strings.TrimPrefix(n, abs+"/")
			if strings.HasPrefix(n, filepath.Dir(repoPath)+"/") || strings.HasSuffix(n, PatchExt) {
				return nil
			}
			file, err := os.Open(p)
			if err != nil {
				return err
			}
			defer file.Close()
			nd, err := addFile(editor.GetDagService(), b.layout, prefix, file)
			if err != nil {
				return err
			}
			if err = editor.InsertNodeAtPath(ctx, n, nd, unixfs.EmptyDirNode); err != nil {
				return err
			}
			maps[strings.TrimPrefix(p, abs+"/")] = nd.Cid()
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	en := editor.GetNode()
	if err := copyLinks(ctx, en, editor.GetDagService(), dag); err != nil {
		return nil, nil, err
	}
	maps[strings.TrimPrefix(pth, b.path)] = en.Cid()
	return en, maps, nil
}

// copyLinks recursively adds all link nodes in node to the dag service.
// Data-nodes are ignored.
// Modified from https://github.com/ipfs/go-merkledag/blob/master/dagutils/utils.go#L210
func copyLinks(ctx context.Context, nd ipld.Node, from, to ipld.DAGService) error {
	err := to.Add(ctx, nd)
	if err != nil {
		return err
	}
	for _, lnk := range nd.Links() {
		if lnk.Name == "" {
			continue
		}
		child, err := lnk.GetNode(ctx, from)
		if err != nil {
			if err == ipld.ErrNotFound {
				// not found means we didnt modify it, and it should
				// already be in the target datastore
				continue
			}
			return err
		}
		err = copyLinks(ctx, child, from, to)
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveFile saves the bucket as a node describing a directory containing reader.
func (b *Bucket) SaveFile(ctx context.Context, pth string, name string) error {
	r, err := os.Open(pth)
	if err != nil {
		return err
	}
	defer r.Close()
	root := unixfs.EmptyDirNode()
	prefix, err := md.PrefixForCidVersion(b.cidver)
	if err != nil {
		return err
	}
	root.SetCidBuilder(prefix)
	editor := dagutils.NewDagEditor(root, b.dag)
	f, err := addFile(editor.GetDagService(), b.layout, prefix, r)
	if err != nil {
		return err
	}
	if err = editor.InsertNodeAtPath(ctx, name, f, unixfs.EmptyDirNode); err != nil {
		return err
	}
	n := editor.GetNode()
	if err := copyLinks(ctx, n, editor.GetDagService(), b.dag); err != nil {
		return err
	}
	if err := b.setLocalPath(pth, n.Cid()); err != nil {
		return err
	}
	return b.setLocalPath("", n.Cid())
}

// HashFile returns the cid of the file at path.
// This method does not alter the bucket.
func (b *Bucket) HashFile(pth string) (cid.Cid, error) {
	r, err := os.Open(pth)
	if err != nil {
		return cid.Undef, err
	}
	defer r.Close()
	prefix, err := md.PrefixForCidVersion(b.cidver)
	if err != nil {
		return cid.Undef, err
	}
	n, err := addFile(dagutils.NewMemoryDagService(), b.layout, prefix, r)
	if err != nil {
		return cid.Undef, err
	}
	return n.Cid(), nil
}

// Get returns the node at cid from the bucket.
func (b *Bucket) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	return b.dag.Get(ctx, c)
}

// Diff returns a list of changes that are present in path compared to the bucket.
func (b *Bucket) Diff(ctx context.Context, pth string) (diff []*dagutils.Change, err error) {
	tmp := dagutils.NewMemoryDagService()
	var an ipld.Node
	lc, _, err := b.Root()
	if err != nil {
		return
	}
	if lc.Defined() {
		an, err = b.dag.Get(ctx, lc)
		if err != nil {
			return
		}
	} else {
		an = unixfs.EmptyDirNode()
	}
	if err = copyLinks(ctx, an, b.dag, tmp); err != nil {
		return
	}
	if err = tmp.Add(ctx, an); err != nil {
		return
	}
	bn, _, err := b.recursiveAddPath(ctx, pth, tmp)
	if err != nil {
		return
	}
	return dagutils.Diff(ctx, tmp, an, bn)
}

// SetRemotePath sets or creates a mapping from a local path to a remote cid.
func (b *Bucket) SetRemotePath(pth string, remote cid.Cid) error {
	k, err := getPathKey(pth)
	if err != nil {
		return err
	}
	xm, err := b.getPathMap(k)
	if errors.Is(err, ds.ErrNotFound) {
		return b.putPathMap(k, pathMap{Remote: remote})
	}
	if err != nil {
		return err
	}
	xm.Remote = remote
	return b.putPathMap(k, xm)
}

// MatchPath returns whether or not the path exists and has matching local and remote cids.
func (b *Bucket) MatchPath(pth string, local, remote cid.Cid) (bool, error) {
	k, err := getPathKey(pth)
	if err != nil {
		return false, err
	}
	m, err := b.getPathMap(k)
	if err != nil {
		return false, err
	}
	if m.Local.Defined() && m.Local.Equals(local) && m.Remote.Equals(remote) {
		return true, nil
	}
	return false, nil
}

// RemovePath removes a local path map from the store.
func (b *Bucket) RemovePath(ctx context.Context, pth string) error {
	k, err := getPathKey(pth)
	if err != nil {
		return err
	}
	pm, err := b.getPathMap(k)
	if errors.Is(err, ds.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := b.dag.Remove(ctx, pm.Local); err != nil {
		if !errors.Is(err, ipld.ErrNotFound) {
			return err
		}
	}
	return b.ds.Delete(k)
}

// Close closes the store and blocks service.
func (b *Bucket) Close() error {
	if err := b.ds.Close(); err != nil {
		return err
	}
	if err := b.bsrv.Close(); err != nil {
		return err
	}
	return nil
}

// Ignore returns true if the path contains an ignored file.
func Ignore(pth string) bool {
	for _, n := range ignoredFilenames {
		if strings.HasSuffix(pth, n) {
			return true
		}
	}
	return false
}

// addFile chunks reader with layout and adds blocks to the dag service.
// SHA2-256 is used as the hash function and CidV1 as the cid version.
func addFile(dag ipld.DAGService, layout options.Layout, prefix cid.Prefix, r io.Reader) (ipld.Node, error) {
	dbp := helpers.DagBuilderParams{
		Dagserv:    dag,
		RawLeaves:  true,
		Maxlinks:   helpers.DefaultLinksPerBlock,
		NoCopy:     false,
		CidBuilder: prefix,
	}
	chnk, err := chunker.FromString(r, "default")
	if err != nil {
		return nil, err
	}
	dbh, err := dbp.New(chnk)
	if err != nil {
		return nil, err
	}

	var n ipld.Node
	switch layout {
	case options.TrickleLayout:
		n, err = trickle.Layout(dbh)
	case options.BalancedLayout:
		n, err = balanced.Layout(dbh)
	default:
		return nil, fmt.Errorf("invalid layout")
	}
	return n, err
}