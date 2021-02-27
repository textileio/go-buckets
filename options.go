package buckets

import (
	c "github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	core "github.com/textileio/go-threads/core/thread"
)

type CreateOptions struct {
	Thread  core.ID
	Name    string
	Private bool
	Cid     c.Cid
}

type CreateOption func(*CreateOptions)

// WithThread specifies a thread for the bucket.
// A new thread is created by default.
func WithThread(thread core.ID) CreateOption {
	return func(args *CreateOptions) {
		args.Thread = thread
	}
}

// WithName sets a name for the bucket.
func WithName(name string) CreateOption {
	return func(args *CreateOptions) {
		args.Name = name
	}
}

// WithPrivate specifies that an encryption key will be used for the bucket.
func WithPrivate(private bool) CreateOption {
	return func(args *CreateOptions) {
		args.Private = private
	}
}

// WithCid indicates that an inited bucket should be boostraped from a UnixFS DAG.
func WithCid(cid c.Cid) CreateOption {
	return func(args *CreateOptions) {
		args.Cid = cid
	}
}

type Options struct {
	Root     path.Resolved
	Progress chan<- int64
}

type Option func(*Options)

// WithFastForwardOnly instructs the remote to reject non-fast-forward updates by comparing root with the remote.
func WithFastForwardOnly(root path.Resolved) Option {
	return func(args *Options) {
		args.Root = root
	}
}

// WithProgress writes progress updates to the given channel.
func WithProgress(ch chan<- int64) Option {
	return func(args *Options) {
		args.Progress = ch
	}
}
