package local

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/spf13/cobra"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/cast"
	"github.com/textileio/go-buckets/api/client"
	"github.com/textileio/go-buckets/cmd"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/thread"
)

var (
	// ErrNotABucket indicates the given path is not within a bucket.
	ErrNotABucket = errors.New("not a bucket (or any of the parent directories): .textile")
	// ErrBucketExists is used during initialization to indicate the path already contains a bucket.
	ErrBucketExists = errors.New("bucket is already initialized")
	// ErrThreadRequired indicates the operation requires a thread ID but none was given.
	ErrThreadRequired = errors.New("thread ID is required")

	flags = map[string]cmd.Flag{
		"key":      {Key: "key", DefValue: ""},
		"thread":   {Key: "thread", DefValue: ""},
		"identity": {Key: "identity", DefValue: ""},
	}
)

// DefaultConfConfig returns the default ConfConfig.
func DefaultConfConfig() cmd.ConfConfig {
	return cmd.ConfConfig{
		Dir:       ".textile",
		Name:      "config",
		Type:      "yaml",
		EnvPrefix: "BUCK",
	}
}

// Buckets is used to create new individual buckets with the provided client and config.
type Buckets struct {
	c      *client.Client
	config cmd.ConfConfig
}

// NewBuckets creates Buckets with client and config.
func NewBuckets(c *client.Client, config cmd.ConfConfig) *Buckets {
	return &Buckets{c: c, config: config}
}

// Clients returns the underlying client object.
func (b *Buckets) Client() *client.Client {
	return b.c
}

// Config contains details for a new local bucket.
type Config struct {
	// Path is the path in which the new bucket should be created (required).
	Path string
	// Key is a key of an existing bucket (optional).
	// It's value may be inflated from a --key flag or {EnvPrefix}_KEY env variable.
	Key string
	// Thread is the thread ID of the target thread (required).
	// It's value may be inflated from a --thread flag or {EnvPrefix}_THREAD env variable.
	Thread thread.ID
	// Identity is an identity to use with the target thread.
	// It's value may be inflated from an --identity flag or {EnvPrefix}_IDENTITY env variable.
	// @todo: Handle more identities
	// @todo: Pull this from a global config of identites, i.e., ~/.threads:
	//   identities:
	//     default: clyde
	//     clyde: <priv_key_base_64>
	//     eddy: <priv_key_base_64>
	Identity thread.Identity
}

// NewConfigFromCmd returns a config by inflating values from the given cobra command and path.
// First, flags for "key" and "thread" are used if they exist.
// If still unset, the env vars {EnvPrefix}_KEY and {EnvPrefix}_THREAD are used.
func (b *Buckets) NewConfigFromCmd(c *cobra.Command, pth string) (conf Config, err error) {
	conf.Path = pth
	conf.Key = cmd.GetFlagOrEnvValue(c, "key", b.config.EnvPrefix)
	id := cmd.GetFlagOrEnvValue(c, "thread", b.config.EnvPrefix)
	if len(id) != 0 {
		conf.Thread, err = thread.Decode(id)
		if err != nil {
			return conf, err
		}
	}
	if len(conf.Key) != 0 && !conf.Thread.Defined() {
		return conf, ErrThreadRequired
	}
	identity := cmd.GetFlagOrEnvValue(c, "identity", b.config.EnvPrefix)
	if len(identity) != 0 {
		lp2pid := &thread.Libp2pIdentity{}
		if err := lp2pid.UnmarshalString(identity); err != nil {
			return conf, err
		}
		conf.Identity = lp2pid
	}
	return conf, nil
}

// NewBucket initializes a new bucket from the config.
// A local blockstore is created that's used to sync local changes with the remote.
// By default, this will be an unencrypted, unnamed, empty bucket.
// The remote bucket will also be created if it doesn't already exist.
// See NewOption for more info.
func (b *Buckets) NewBucket(ctx context.Context, conf Config, opts ...NewOption) (buck *Bucket, err error) {
	args := &newOptions{}
	for _, opt := range opts {
		opt(args)
	}

	// Ensure we're not going to overwrite an existing local config
	cwd, err := filepath.Abs(conf.Path)
	if err != nil {
		return
	}
	bc, found, err := b.config.NewConfig(cwd, flags, false)
	if err != nil {
		return
	}
	if found {
		return nil, ErrBucketExists
	}

	// Check config values
	if conf.Thread.Defined() {
		bc.Viper.Set("thread", conf.Thread.String())
	}
	bc.Viper.Set("key", conf.Key)
	if conf.Identity == nil {
		sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		conf.Identity = thread.NewLibp2pIdentity(sk)
	}
	bc.Viper.Set("identity", conf.Identity.String())

	buck = &Bucket{
		c:         b.c,
		cwd:       cwd,
		conf:      bc,
		pushBlock: make(chan struct{}, 1),
	}

	ctx, err = authCtx(ctx, b.c, conf.Identity)
	if err != nil {
		return nil, err
	}

	initRemote := conf.Key == ""
	if initRemote {
		rep, err := b.c.Create(
			ctx,
			buckets.WithThread(conf.Thread),
			buckets.WithName(args.name),
			buckets.WithPrivate(args.private),
			buckets.WithCid(args.fromCid),
		)
		if err != nil {
			return nil, err
		}

		buck.conf.Viper.Set("thread", rep.Bucket.Thread)
		buck.conf.Viper.Set("key", rep.Bucket.Key)

		seed := filepath.Join(cwd, collection.SeedName)
		file, err := os.Create(seed)
		if err != nil {
			return nil, err
		}
		_, err = file.Write(rep.Seed.Data)
		if err != nil {
			file.Close()
			return nil, err
		}
		file.Close()

		if err = buck.loadLocalRepo(ctx, cwd, b.repoName(), false); err != nil {
			return nil, err
		}
		if err = buck.repo.SaveFile(ctx, seed, collection.SeedName); err != nil {
			return nil, err
		}
		sc, err := cid.Decode(rep.Seed.Cid)
		if err != nil {
			return nil, err
		}
		if err = buck.repo.SetRemotePath(collection.SeedName, sc); err != nil {
			return nil, err
		}
		rp, err := dag.NewResolvedPath(rep.Bucket.Path)
		if err != nil {
			return nil, err
		}
		if err = buck.repo.SetRemotePath("", rp.Cid()); err != nil {
			return nil, err
		}

		links := cast.LinksFromPb(rep.Links)
		buck.links = &links
	} else {
		if err := buck.loadLocalRepo(ctx, cwd, b.repoName(), true); err != nil {
			return nil, err
		}
		r, err := buck.Roots(ctx)
		if err != nil {
			return nil, err
		}
		if err = buck.repo.SetRemotePath("", r.Remote); err != nil {
			return nil, err
		}

		if _, err = buck.RemoteLinks(ctx, ""); err != nil {
			return nil, err
		}
	}

	// Write the local config to disk
	dir := filepath.Join(cwd, buck.conf.Dir)
	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		return
	}
	config := filepath.Join(dir, buck.conf.Name+".yml")
	if err = buck.conf.Viper.WriteConfigAs(config); err != nil {
		return
	}
	cfile, err := filepath.Abs(config)
	if err != nil {
		return
	}
	buck.conf.Viper.SetConfigFile(cfile)

	// Pull remote bucket contents
	if !initRemote || args.fromCid.Defined() {
		if err := buck.repo.Save(ctx); err != nil {
			return nil, err
		}
		switch args.strategy {
		case Soft, Hybrid:
			diff, missing, remove, err := buck.diffPath(ctx, "", cwd, args.strategy == Hybrid)
			if err != nil {
				return nil, err
			}
			if err = stashChanges(diff); err != nil {
				return nil, err
			}
			if _, err = buck.handleChanges(ctx, missing, remove, args.events); err != nil {
				return nil, err
			}
			if err := buck.repo.Save(ctx); err != nil {
				return nil, err
			}
			if err = applyChanges(diff); err != nil {
				return nil, err
			}
		case Hard:
			if _, err := buck.getPath(ctx, "", cwd, nil, false, args.events); err != nil {
				return nil, err
			}
			if err := buck.repo.Save(ctx); err != nil {
				return nil, err
			}
		}
	}
	return buck, nil
}

func (b *Buckets) repoName() string {
	return filepath.Join(b.config.Dir, "repo")
}

// GetLocalBucket loads and returns the bucket at path if it exists.
func (b *Buckets) GetLocalBucket(ctx context.Context, conf Config) (*Bucket, error) {
	cwd, err := filepath.Abs(conf.Path)
	if err != nil {
		return nil, err
	}
	bc, found, err := b.config.NewConfig(cwd, flags, false)
	if err != nil {
		return nil, err
	}
	if conf.Thread.Defined() {
		bc.Viper.Set("thread", conf.Thread.String())
	}
	if conf.Key != "" {
		bc.Viper.Set("key", conf.Key)
	}
	if bc.Viper.Get("thread") == nil || bc.Viper.Get("key") == nil {
		return nil, ErrNotABucket
	}
	cmd.ExpandConfigVars(bc.Viper, bc.Flags)
	buck := &Bucket{
		c:         b.c,
		cwd:       cwd,
		conf:      bc,
		pushBlock: make(chan struct{}, 1),
	}
	if found {
		bp, err := buck.Path()
		if err != nil {
			return nil, err
		}
		if err = buck.loadLocalRepo(ctx, bp, b.repoName(), true); err != nil {
			return nil, err
		}
	}
	return buck, nil
}

// RemoteBuckets lists all existing remote buckets in the thread.
func (b *Buckets) RemoteBuckets(
	ctx context.Context,
	id thread.ID,
	identity thread.Identity,
) (list []buckets.Bucket, err error) {
	ctx, err = authCtx(ctx, b.c, identity)
	if err != nil {
		return nil, err
	}
	res, err := b.c.List(ctx, id)
	if err != nil {
		return nil, err
	}
	for _, b := range res.Buckets {
		bucket, err := cast.BucketFromPb(b)
		if err != nil {
			return nil, err
		}
		list = append(list, bucket)
	}
	return list, nil
}

// authCtx returns an identity token context for authentication and authorization.
func authCtx(ctx context.Context, c *client.Client, identity thread.Identity) (context.Context, error) {
	return c.NewTokenContext(ctx, identity, time.Hour*24)
}
