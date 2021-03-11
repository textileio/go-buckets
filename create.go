package buckets

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
)

func (b *Buckets) Create(
	ctx context.Context,
	identity did.Token,
	opts ...CreateOption,
) (*Bucket, *Seed, int64, error) {
	args := &CreateOptions{}
	for _, opt := range opts {
		opt(args)
	}

	if args.Thread.Defined() {
		if err := args.Thread.Validate(); err != nil {
			return nil, nil, 0, fmt.Errorf("invalid thread id: %v", err)
		}
	} else {
		args.Thread = core.NewRandomIDV1()
		if err := b.db.NewDB(ctx, args.Thread, db.WithNewManagedName(args.Name)); err != nil {
			return nil, nil, 0, fmt.Errorf("creating new thread: %v", err)
		}
	}

	_, owner, err := b.net.ValidateIdentity(ctx, identity)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("validating identity: %v", err)
	}

	// Create bucket keys if private
	var linkKey, fileKey []byte
	if args.Private {
		var err error
		linkKey, err = dcrypto.NewKey()
		if err != nil {
			return nil, nil, 0, err
		}
		fileKey, err = dcrypto.NewKey()
		if err != nil {
			return nil, nil, 0, err
		}
	}

	// Make a random seed, which ensures a bucket's uniqueness
	seed, err := dag.MakeBucketSeed(fileKey)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("making bucket seed: %v", err)
	}

	// Create the bucket directory
	var pth path.Resolved
	if args.Cid.Defined() {
		ctx, pth, err = dag.CreateBucketPathWithCid(
			ctx,
			b.ipfs,
			"",
			args.Cid,
			linkKey,
			fileKey,
			seed,
		)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("creating bucket with cid: %v", err)
		}
	} else {
		ctx, pth, err = dag.CreateBucketPath(ctx, b.ipfs, seed, linkKey)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("creating bucket: %v", err)
		}
	}

	// Create top-level metadata
	now := time.Now()
	md := map[string]collection.Metadata{
		"": collection.NewDefaultMetadata(owner, fileKey, now),
		collection.SeedName: {
			Roles:     make(map[did.DID]collection.Role),
			UpdatedAt: now.UnixNano(),
		},
	}

	// Create a new IPNS key
	key, err := b.ipns.CreateKey(ctx, args.Thread)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("creating IPNS key: %v", err)
	}

	// Create the bucket using the IPNS key as instance ID
	instance, err := b.c.New(
		ctx,
		args.Thread,
		key,
		owner,
		pth,
		now,
		md,
		identity,
		collection.WithBucketName(args.Name),
		collection.WithBucketKey(linkKey),
	)
	if err != nil {
		return nil, nil, 0, err
	}

	seedInfo := &Seed{
		Cid: seed.Cid(),
	}
	if instance.IsPrivate() {
		fileKey, err := instance.GetFileEncryptionKeyForPath("")
		if err != nil {
			return nil, nil, 0, err
		}
		data, err := dag.DecryptData(seed.RawData(), fileKey)
		if err != nil {
			return nil, nil, 0, err
		}
		seedInfo.Data = data
	} else {
		seedInfo.Data = seed.RawData()
	}

	// Publish the new bucket's address to the name system
	go b.ipns.Publish(pth, instance.Key)

	log.Debugf("created %s", key)
	return instanceToBucket(args.Thread, instance), seedInfo, dag.GetPinnedBytes(ctx), nil
}
