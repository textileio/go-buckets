package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/cast"
	"github.com/textileio/go-buckets/api/client"
	"github.com/textileio/go-buckets/cmd"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	"github.com/textileio/go-threads/core/thread"
)

var (
	// ErrUpToDate indicates there are no locally staged changes.
	ErrUpToDate = errors.New("everything up-to-date")
	// ErrAborted indicates the caller aborted the operation via a confirm function.
	ErrAborted = errors.New("operation aborted by caller")
)

// Event describes a path event that occurred.
// These events can be used to display live info during path uploads/downloads.
type Event struct {
	// Type of event.
	Type EventType
	// Path relative to the bucket's cwd.
	Path string
	// Cid of associated Path.
	Cid cid.Cid
	// Size of the total operation or completed file.
	Size int64
	// Complete is the amount of Size that is complete (useful for upload/download progress).
	Complete int64
}

// EventType is the type of path event.
type EventType int

const (
	// EventProgress indicates a file has made some progress uploading/downloading.
	EventProgress EventType = iota
	// EventFileComplete indicates a file has completed uploading/downloading.
	EventFileComplete
	// EventFileRemoved indicates a file has been removed.
	EventFileRemoved
)

// Bucket is a local-first object storage and synchronization model built
// on ThreadDB, IPFS, and Filecoin.
// A bucket represents a dynamic Unixfs directory with auto-updating
// IPNS resolution, website rendering, and Filecoin archivng.
//
// Private buckets are fully encrypted using AES-CTR + AES-512 HMAC (see https://github.com/textileio/dcrypto for more).
// Both Unixfs node metadata (size, links, etc.) and node data (files) are obfuscated by encryption.
// The AES and HMAC keys used for bucket encryption are stored in the ThreadDB collection instance.
// This setup allows for bucket access to inherit from thread ACL rules.
//
// Additionally, files can be further protected by password-based encryption before they are added to the bucket.
// See EncryptLocalPath and DecryptLocalPath for more.
type Bucket struct {
	c     *client.Client
	cwd   string
	conf  *cmd.Config
	repo  *Repo
	links *buckets.Links

	pushBlock chan struct{}
	sync.Mutex
}

// Thread returns the bucket's thread ID.
func (b *Bucket) Thread() (id thread.ID, err error) {
	ids := b.conf.Viper.GetString("thread")
	if ids == "" {
		return thread.Undef, nil
	}
	return thread.Decode(ids)
}

// Key returns the bucket's unique key identifier, which is also an IPNS public key.
func (b *Bucket) Key() string {
	return b.conf.Viper.GetString("key")
}

// Identity returns the identity associated with the bucket.
func (b *Bucket) Identity() (thread.Identity, error) {
	identity := &thread.Libp2pIdentity{}
	err := identity.UnmarshalString(b.conf.Viper.GetString("identity"))
	return identity, err
}

// Path returns the bucket's top-level local filesystem path.
func (b *Bucket) Path() (string, error) {
	conf := b.conf.Viper.ConfigFileUsed()
	if conf == "" {
		return b.cwd, nil
	}
	return filepath.Dir(filepath.Dir(conf)), nil
}

// LocalSize returns the cumalative size of the bucket's local files.
func (b *Bucket) LocalSize() (int64, error) {
	if b.repo == nil {
		return 0, nil
	}
	bp, err := b.Path()
	if err != nil {
		return 0, err
	}
	var size int64
	err = filepath.Walk(bp, func(n string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("getting fileinfo of %s: %s", n, err)
		}
		if !info.IsDir() {
			f := strings.TrimPrefix(n, bp+string(os.PathSeparator))
			if Ignore(n) || (strings.HasPrefix(f, b.conf.Dir) && f != collection.SeedName) {
				return nil
			}
			size += info.Size()
		}
		return err
	})
	return size, err
}

// Get returns info about a bucket from the remote.
func (b *Bucket) Get(ctx context.Context) (bucket buckets.Bucket, err error) {
	ctx, err = b.authCtx(ctx, time.Hour)
	if err != nil {
		return
	}
	id, err := b.Thread()
	if err != nil {
		return
	}
	rep, err := b.c.Get(ctx, id, b.Key())
	if err != nil {
		return
	}
	return cast.BucketFromPb(rep.Bucket)
}

// Roots wraps local and remote root cids.
// If the bucket is not private (encrypted), these will be the same.
type Roots struct {
	Local  cid.Cid `json:"local"`
	Remote cid.Cid `json:"remote"`
}

// Roots returns the bucket's current local and remote root cids.
func (b *Bucket) Roots(ctx context.Context) (roots Roots, err error) {
	var lc, rc cid.Cid
	if b.repo != nil {
		lc, rc, err = b.repo.Root()
		if err != nil {
			return
		}
	}
	if !rc.Defined() {
		rc, err = b.getRemoteRoot(ctx)
		if err != nil {
			return
		}
	}
	return Roots{Local: lc, Remote: rc}, nil
}

// RemoteLinks returns the remote links for the bucket.
func (b *Bucket) RemoteLinks(ctx context.Context, pth string) (links buckets.Links, err error) {
	if b.links != nil {
		return *b.links, nil
	}
	ctx, err = b.authCtx(ctx, time.Hour)
	if err != nil {
		return
	}
	id, err := b.Thread()
	if err != nil {
		return
	}
	res, err := b.c.GetLinks(ctx, id, b.Key(), pth)
	if err != nil {
		return
	}
	links = cast.LinksFromPb(res.Links)
	b.links = &links
	return links, err
}

// CatRemotePath writes the content of the remote path to writer.
func (b *Bucket) CatRemotePath(ctx context.Context, pth string, w io.Writer) error {
	ctx, err := b.authCtx(ctx, time.Hour)
	if err != nil {
		return err
	}
	id, err := b.Thread()
	if err != nil {
		return err
	}
	return b.c.PullPath(ctx, id, b.Key(), pth, w)
}

// Destroy completely deletes the local and remote bucket.
func (b *Bucket) Destroy(ctx context.Context) (err error) {
	b.Lock()
	defer b.Unlock()
	ctx, err = b.authCtx(ctx, time.Hour)
	if err != nil {
		return
	}
	bp, err := b.Path()
	if err != nil {
		return
	}
	id, err := b.Thread()
	if err != nil {
		return
	}
	if err = b.c.Remove(ctx, id, b.Key()); err != nil {
		return
	}
	_ = os.RemoveAll(filepath.Join(bp, collection.SeedName))
	_ = os.RemoveAll(filepath.Join(bp, b.conf.Dir))
	return nil
}

// GetIdentityToken returns a did.Token for the bucket identity valid for the given duration.
func (b *Bucket) GetIdentityToken(dur time.Duration) (did.Token, error) {
	ctx, err := b.authCtx(context.TODO(), dur)
	if err != nil {
		return "", err
	}
	token, _ := did.TokenFromContext(ctx)
	return token, nil
}

func (b *Bucket) loadLocalRepo(ctx context.Context, pth, name string, setCidVersion bool) error {
	r, err := NewRepo(pth, name, options.BalancedLayout)
	if err != nil {
		return err
	}
	b.repo = r
	if setCidVersion {
		if err = b.setRepoCidVersion(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bucket) setRepoCidVersion(ctx context.Context) error {
	if b.repo == nil {
		return nil
	}
	r, err := b.Roots(ctx)
	if err != nil {
		return err
	}
	b.repo.SetCidVersion(int(r.Remote.Version()))
	return nil
}

func (b *Bucket) containsPath(pth string) (c bool, err error) {
	bp, err := b.Path()
	if err != nil {
		return
	}
	ar, err := filepath.Abs(bp)
	if err != nil {
		return
	}
	ap, err := filepath.Abs(pth)
	if err != nil {
		return
	}
	return strings.HasPrefix(ap, ar), nil
}

func (b *Bucket) getRemoteRoot(ctx context.Context) (cid.Cid, error) {
	ctx, err := b.authCtx(ctx, time.Hour)
	if err != nil {
		return cid.Undef, err
	}
	id, err := b.Thread()
	if err != nil {
		return cid.Undef, err
	}
	rr, err := b.c.Get(ctx, id, b.Key())
	if err != nil {
		return cid.Undef, err
	}
	rp, err := dag.NewResolvedPath(rr.Bucket.Path)
	if err != nil {
		return cid.Undef, err
	}
	return rp.Cid(), nil
}

// authCtx returns an identity token context for authentication and authorization.
func (b *Bucket) authCtx(ctx context.Context, dur time.Duration) (context.Context, error) {
	identity := &thread.Libp2pIdentity{}
	if err := identity.UnmarshalString(b.conf.Viper.GetString("identity")); err != nil {
		return nil, err
	}
	return authCtx(ctx, b.c, identity, dur)
}
