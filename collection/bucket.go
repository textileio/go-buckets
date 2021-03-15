package collection

import (
	"context"
	"encoding/base64"
	"fmt"
	gopath "path"
	"strings"
	"time"

	"github.com/alecthomas/jsonschema"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/dcrypto"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	db "github.com/textileio/go-threads/db"
)

const (
	// Name is the name of the threaddb collection used for buckets.
	Name = "buckets"
	// SeedName is the file name reserved for a random bucket seed.
	SeedName = ".textileseed"
)

var (
	schema  *jsonschema.Schema
	indexes = []db.Index{{
		Path: "path",
	}}
	config db.CollectionConfig
)

// Version is a concrete type for bucket version.
type Version int

var (
	// Version1 is the current bucket collection version.
	Version1 Version = 1
)

// Bucket represents the buckets threaddb collection schema.
type Bucket struct {
	Key       string              `json:"_id"`
	Owner     did.DID             `json:"owner"`
	Name      string              `json:"name"`
	Version   int                 `json:"version"`
	LinkKey   string              `json:"key,omitempty"`
	Path      string              `json:"path"` // @todo: This really should be Cid but will require a migration.
	Metadata  map[string]Metadata `json:"metadata"`
	CreatedAt int64               `json:"created_at"`
	UpdatedAt int64               `json:"updated_at"`
}

// Role describes an access role for a bucket item.
type Role int

const (
	// NoneRole is the default role.
	NoneRole Role = iota
	// ReaderRole can read from the associated path and its descendants.
	ReaderRole
	// WriterRole can read from and write to the associated path and its descendants.
	WriterRole
	// AdminRole can read from, write to, and modify roles at the associated path and its descendants.
	AdminRole
)

// NewRoleFromString returns the role associated with the given string.
func NewRoleFromString(s string) (Role, error) {
	switch strings.ToLower(s) {
	case "none":
		return NoneRole, nil
	case "reader":
		return ReaderRole, nil
	case "writer":
		return WriterRole, nil
	case "admin":
		return AdminRole, nil
	default:
		return NoneRole, fmt.Errorf("invalid role: %s", s)
	}
}

// String returns the string representation of the role.
func (r Role) String() string {
	switch r {
	case NoneRole:
		return "None"
	case ReaderRole:
		return "Reader"
	case WriterRole:
		return "Writer"
	case AdminRole:
		return "Admin"
	default:
		return "Invalid"
	}
}

// Metadata contains metadata about a bucket item (a file or folder).
type Metadata struct {
	Key       string                 `json:"key,omitempty"`
	Roles     map[did.DID]Role       `json:"roles"`
	Info      map[string]interface{} `json:"info,omitempty"`
	UpdatedAt int64                  `json:"updated_at"`
}

// NewDefaultMetadata returns the default metadata for a path.
func NewDefaultMetadata(owner did.DID, key []byte, ts time.Time) Metadata {
	roles := make(map[did.DID]Role)
	if owner.Defined() {
		if key == nil {
			roles["*"] = ReaderRole
		}
		roles[owner] = AdminRole
	}
	md := Metadata{
		Roles:     roles,
		UpdatedAt: ts.UnixNano(),
	}
	md.SetFileEncryptionKey(key)
	return md
}

// SetFileEncryptionKey sets the file encryption key.
func (m *Metadata) SetFileEncryptionKey(key []byte) {
	if key != nil {
		m.Key = base64.StdEncoding.EncodeToString(key)
	}
}

// IsPrivate returns whether or not the bucket is private.
func (b *Bucket) IsPrivate() bool {
	return b.LinkKey != ""
}

// GetLinkEncryptionKey returns the bucket encryption key as bytes if present.
// Version 0 buckets use the link key for all files and folders.
// Version 1 buckets only use the link for folders.
func (b *Bucket) GetLinkEncryptionKey() []byte {
	return keyBytes(b.LinkKey)
}

// GetFileEncryptionKeyForPath returns the encryption key for path.
// Version 0 buckets use the link key for all paths.
// Version 1 buckets use a different key defined in path metadata.
func (b *Bucket) GetFileEncryptionKeyForPath(pth string) ([]byte, error) {
	if b.Version == 0 {
		return b.GetLinkEncryptionKey(), nil
	}

	md, _, ok := b.GetMetadataForPath(pth, true)
	if !ok {
		return nil, fmt.Errorf("could not resolve path: %s", pth)
	}
	return keyBytes(md.Key), nil
}

func keyBytes(k string) []byte {
	if k == "" {
		return nil
	}
	b, _ := base64.StdEncoding.DecodeString(k)
	return b
}

// GetFileEncryptionKeysForPrefix returns a map of keys for every path under prefix.
func (b *Bucket) GetFileEncryptionKeysForPrefix(pre string) (map[string][]byte, error) {
	if b.Version == 0 {
		return map[string][]byte{"": b.GetLinkEncryptionKey()}, nil
	}

	keys := make(map[string][]byte)
	for p := range b.Metadata {
		if strings.HasPrefix(p, pre) {
			md, _, ok := b.GetMetadataForPath(p, true)
			if !ok {
				return nil, fmt.Errorf("could not resolve path: %s", p)
			}
			keys[p] = keyBytes(md.Key)
		}
	}
	return keys, nil
}

// RotateFileEncryptionKeysForPrefix re-generates existing metadata keys for every path under prefix.
func (b *Bucket) RotateFileEncryptionKeysForPrefix(pre string) error {
	if b.Version == 0 {
		return nil
	}

	for p, md := range b.Metadata {
		if strings.HasPrefix(p, pre) {
			if md.Key != "" {
				key, err := dcrypto.NewKey()
				if err != nil {
					return err
				}
				md.SetFileEncryptionKey(key)
			}
		}
	}
	return nil
}

// GetMetadataForPath returns metadata for path.
// The returned metadata could be from an exact path match or
// the nearest parent, i.e., path was added as part of a folder.
// If requireKey is true, metadata w/o a key is ignored and the search continues toward root.
func (b *Bucket) GetMetadataForPath(pth string, requireKey bool) (md Metadata, at string, ok bool) {
	if b.Version == 0 {
		return md, at, true
	}

	// Check for an exact match
	if md, ok = b.Metadata[pth]; ok {
		if !b.IsPrivate() || !requireKey || md.Key != "" {
			return md, pth, true
		}
	}
	// Check if we can see this path via a parent
	parent := pth
	var done bool
	for {
		if done {
			break
		}
		parent = gopath.Dir(parent)
		if parent == "." {
			parent = ""
			done = true
		}
		if md, ok = b.Metadata[parent]; ok {
			if !b.IsPrivate() || !requireKey || md.Key != "" {
				return md, parent, true
			}
		}
	}
	return md, at, false
}

// SetMetadataAtPath create new or merges existing metadata at path.
func (b *Bucket) SetMetadataAtPath(pth string, md Metadata) {
	if b.Version == 0 {
		return
	}

	x, ok := b.Metadata[pth]
	if ok {
		if md.Key != "" {
			x.Key = md.Key
		}
		if md.Roles != nil {
			x.Roles = md.Roles
		}
		if x.Info == nil {
			x.Info = md.Info
		} else if md.Info != nil {
			for k, v := range md.Info {
				x.Info[k] = v
			}
		}
		x.UpdatedAt = md.UpdatedAt
		b.Metadata[pth] = x
	} else {
		if md.Roles == nil {
			md.Roles = make(map[did.DID]Role)
		}
		if md.Info == nil {
			md.Info = make(map[string]interface{})
		}
		b.Metadata[pth] = md
	}
}

// UnsetMetadataWithPrefix removes metadata with the path prefix.
func (b *Bucket) UnsetMetadataWithPrefix(pre string) {
	if b.Version == 0 {
		return
	}

	for p := range b.Metadata {
		if strings.HasPrefix(p, pre) {
			delete(b.Metadata, p)
		}
	}
}

// ensureNoNulls inflates any values that are nil due to schema updates.
func (b *Bucket) ensureNoNulls() {
	if b.Metadata == nil {
		b.Metadata = make(map[string]Metadata)
	}
}

// Copy returns a copy of the bucket.
func (b *Bucket) Copy() *Bucket {
	md := make(map[string]Metadata)
	for k, v := range b.Metadata {
		md[k] = v
	}
	return &Bucket{
		Key:       b.Key,
		Owner:     b.Owner,
		Name:      b.Name,
		Version:   b.Version,
		LinkKey:   b.LinkKey,
		Path:      b.Path,
		Metadata:  md,
		CreatedAt: b.CreatedAt,
		UpdatedAt: b.UpdatedAt,
	}
}

// BucketOptions defines options for interacting with buckets.
type BucketOptions struct {
	Name string
	Key  []byte
}

// BucketOption holds a bucket option.
type BucketOption func(*BucketOptions)

// WithBucketName specifies a name for a bucket.
// Note: This is only valid when creating a new bucket.
func WithBucketName(n string) BucketOption {
	return func(args *BucketOptions) {
		args.Name = n
	}
}

// WithBucketKey sets the bucket encryption key.
func WithBucketKey(k []byte) BucketOption {
	return func(args *BucketOptions) {
		args.Key = k
	}
}

func init() {
	reflector := jsonschema.Reflector{ExpandedStruct: true}
	schema = reflector.Reflect(&Bucket{})
	config = db.CollectionConfig{
		Name:    Name,
		Schema:  schema,
		Indexes: indexes,
		WriteValidator: `
			if (instance && !instance.version) {
			  return true
			}
			var type = event.patch.type
			var patch = event.patch.json_patch
			var restricted = ["owner", "name", "version", "key", "archives", "created_at"]
			switch (type) {
			  case "create":
			    if (patch.owner !== "" && writer !== patch.owner) {
			      return "permission denied" // writer must match new bucket owner
			    }
			    break
			  case "save":
			    if (instance.owner === "") {
			      return true
			    }
			    if (writer !== instance.owner) {
			      for (i = 0; i < restricted.length; i++) {
			        if (patch[restricted[i]]) {
			          return "permission denied"
			        }
			      }
			    }
			    if (!patch.metadata) {
			      if (patch.path && writer !== instance.owner) {
			        return "permission denied"
			      } else {
			        patch.metadata = {}
			      }
			    }
			    var keys = Object.keys(patch.metadata)
			    for (i = 0; i < keys.length; i++) {
			      var p = patch.metadata[keys[i]]
			      var x = instance.metadata[keys[i]]
			      if (x) {
			        if (!x.roles[writer]) {
			          x.roles[writer] = 0
			        }
			        if (!x.roles["*"]) {
			          x.roles["*"] = 0
			        }
			        // merge all parents, taking most privileged role
			        if (keys[i].length > 0) {
			          var parts = keys[i].split("/")
			          parts.unshift("")
			          var path = ""
			          for (j = 0; j < parts.length; j++) {
			            if (path.length > 0) {
			              path += "/"
			            }
			            path += parts[j]
			            var y = instance.metadata[path]
			            if (!y) {
			              continue
			            }
			            if (!y.roles[writer]) {
			              y.roles[writer] = 0
			            }
			            if (!y.roles["*"]) {
			              y.roles["*"] = 0
			            }
			            if (y.roles[writer] > x.roles[writer]) {
			              x.roles[writer] = y.roles[writer]
			            }
			            if (y.roles["*"] > x.roles["*"]) {
			              x.roles["*"] = y.roles["*"]
			            }
			          }
			        }
			        // check access against merged roles
			        if (!p) {
			          if (x.roles[writer] < 3) {
			            return "permission denied" // no admin access to delete items
			          }
			        } else {
			          if (p.roles && x.roles[writer] < 3) {
			            return "permission denied" // no admin access to edit roles
			          }
			        }
			        if (x.roles[writer] < 2 && x.roles["*"] < 2) {
			          return "permission denied" // no write access
			        }
			      } else {
			        if (writer !== instance.owner) {
			          return "permission denied" // no owner access to create items
			        }
			      }
			    }
			    break
			  case "delete":
			    if (instance.owner !== "" && writer !== instance.owner) {
			      return "permission denied" // no owner access to delete instance
			    }
			    break
			}
			return true
		`,
		ReadFilter: `
			if (!instance.version) {
			  return instance
			}
			if (instance.owner === "") {
			  return instance
			}
			var filtered = {}
			var keys = Object.keys(instance.metadata)
			outer: for (i = 0; i < keys.length; i++) {
			  var m = instance.metadata[keys[i]]
			  var parts = keys[i].split("/")
			  if (keys[i].length > 0) {
			    parts.unshift("")
			  }
			  var path = ""
			  for (j = 0; j < parts.length; j++) {
			    if (path.length > 0) {
			      path += "/"
			    }
			    path += parts[j]
			    var x = instance.metadata[path]
			    if (x && (x.roles[reader] > 0 || x.roles["*"] > 0)) {
			      filtered[keys[i]] = m
			      continue outer
			    }
			  }
			}
			instance.metadata = filtered
			if (Object.keys(instance.metadata).length === 0) {
			  delete instance.key
			}
			return instance
		`,
	}
}

// Buckets is the threaddb collection for buckets.
type Buckets struct {
	Collection
}

// NewBuckets returns a new buckets collection mananger.
func NewBuckets(c *dbc.Client) (*Buckets, error) {
	return &Buckets{
		Collection: Collection{
			c:      c,
			config: config,
		},
	}, nil
}

// Create a bucket instance.
// Owner must be the identity token's subject.
func (b *Buckets) New(
	ctx context.Context,
	thread core.ID,
	key string,
	owner did.DID,
	pth path.Path,
	created time.Time,
	metadata map[string]Metadata,
	identity did.Token,
	opts ...BucketOption,
) (*Bucket, error) {
	args := &BucketOptions{}
	for _, opt := range opts {
		opt(args)
	}
	var linkKey string
	if args.Key != nil {
		linkKey = base64.StdEncoding.EncodeToString(args.Key)
	}
	if metadata == nil {
		metadata = make(map[string]Metadata)
	}
	bucket := &Bucket{
		Key:       key,
		Owner:     owner,
		Name:      args.Name,
		Version:   int(Version1),
		LinkKey:   linkKey,
		Path:      pth.String(),
		Metadata:  metadata,
		CreatedAt: created.UnixNano(),
		UpdatedAt: created.UnixNano(),
	}
	if _, err := b.Create(ctx, thread, bucket, WithIdentity(identity)); err != nil {
		return nil, fmt.Errorf("creating bucket: %s", err)
	}
	return bucket, nil
}

// GetSafe gets a bucket instance and inflates any values that are missing due to schema updates.
func (b *Buckets) GetSafe(ctx context.Context, thread core.ID, key string, opts ...Option) (*Bucket, error) {
	bucket := &Bucket{}
	if err := b.Get(ctx, thread, key, bucket, opts...); err != nil {
		return nil, fmt.Errorf("getting bucket: %v", err)
	}
	bucket.ensureNoNulls()
	return bucket, nil
}
