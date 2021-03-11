package buckets

import (
	"context"
	"fmt"
	"io"

	ipfsfiles "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-buckets/util"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

type pathReader struct {
	r       io.Reader
	closers []io.Closer
}

func (r *pathReader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *pathReader) Close() error {
	// Close in reverse.
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i].Close(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Buckets) PullPath(
	ctx context.Context,
	thread core.ID,
	key, pth string,
	identity did.Token,
) (io.ReadCloser, error) {
	pth = trimSlash(pth)
	instance, bpth, err := b.getBucketAndPath(ctx, thread, key, pth, identity)
	if err != nil {
		return nil, err
	}
	fileKey, err := instance.GetFileEncryptionKeyForPath(pth)
	if err != nil {
		return nil, err
	}

	var filePath path.Resolved
	if instance.IsPrivate() {
		buckPath, err := util.NewResolvedPath(instance.Path)
		if err != nil {
			return nil, err
		}
		np, isDir, r, err := dag.GetNodesToPath(ctx, b.ipfs, buckPath, pth, instance.GetLinkEncryptionKey())
		if err != nil {
			return nil, err
		}
		if r != "" {
			return nil, fmt.Errorf("could not resolve path: %s", bpth)
		}
		if isDir {
			return nil, fmt.Errorf("node is a directory")
		}
		fn := np[len(np)-1]
		filePath = path.IpfsPath(fn.New.Cid())
	} else {
		filePath, err = b.ipfs.ResolvePath(ctx, bpth)
		if err != nil {
			return nil, err
		}
	}

	r := &pathReader{}
	node, err := b.ipfs.Unixfs().Get(ctx, filePath)
	if err != nil {
		return nil, err
	}
	r.closers = append(r.closers, node)

	file := ipfsfiles.ToFile(node)
	if file == nil {
		_ = r.Close()
		return nil, fmt.Errorf("node is a directory")
	}
	if fileKey != nil {
		dr, err := dcrypto.NewDecrypter(file, fileKey)
		if err != nil {
			_ = r.Close()
			return nil, err
		}
		r.closers = append(r.closers, dr)
		r.r = dr
	} else {
		r.r = file
	}

	log.Debugf("pulled %s from %s", pth, instance.Key)
	return r, nil
}

func (b *Buckets) PullIPFSPath(ctx context.Context, pth string) (io.ReadCloser, error) {
	node, err := b.ipfs.Unixfs().Get(ctx, path.New(pth))
	if err != nil {
		return nil, err
	}
	file := ipfsfiles.ToFile(node)
	if file == nil {
		return nil, fmt.Errorf("node is a directory")
	}
	return file, nil
}
