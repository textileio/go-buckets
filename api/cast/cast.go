package cast

import (
	"encoding/json"

	"github.com/textileio/go-buckets"
	pb "github.com/textileio/go-buckets/api/pb/buckets"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
	"github.com/textileio/go-threads/core/thread"
)

func BucketToPb(bucket *buckets.Bucket) *pb.Bucket {
	pmd := make(map[string]*pb.Metadata)
	for p, md := range bucket.Metadata {
		pmd[p] = MetadataToPb(md)
	}
	return &pb.Bucket{
		Thread:    bucket.Thread.String(),
		Key:       bucket.Key,
		Owner:     string(bucket.Owner),
		Name:      bucket.Name,
		Version:   int32(bucket.Version),
		LinkKey:   bucket.LinkKey,
		Path:      bucket.Path,
		Metadata:  pmd,
		CreatedAt: bucket.CreatedAt,
		UpdatedAt: bucket.UpdatedAt,
	}
}

func BucketFromPb(bucket *pb.Bucket) (buckets.Bucket, error) {
	name := "unnamed"
	if bucket.Name != "" {
		name = bucket.Name
	}
	id, err := thread.Decode(bucket.Thread)
	if err != nil {
		return buckets.Bucket{}, err
	}
	md := make(map[string]collection.Metadata)
	for p, m := range bucket.Metadata {
		md[p] = MetadataFromPb(m)
	}
	return buckets.Bucket{
		Thread: id,
		Bucket: collection.Bucket{
			Key:       bucket.Key,
			Owner:     did.DID(bucket.Owner),
			Name:      name,
			Version:   int(bucket.Version),
			LinkKey:   bucket.LinkKey,
			Path:      bucket.Path,
			Metadata:  md,
			CreatedAt: bucket.CreatedAt,
			UpdatedAt: bucket.UpdatedAt,
		},
	}, nil
}

func MetadataToPb(md collection.Metadata) *pb.Metadata {
	var info []byte
	if md.Info != nil {
		info, _ = json.Marshal(md.Info)
	}
	return &pb.Metadata{
		Key:       md.Key,
		Roles:     RolesToPb(md.Roles),
		UpdatedAt: md.UpdatedAt,
		Info:      info,
	}
}

func MetadataFromPb(md *pb.Metadata) collection.Metadata {
	var info map[string]interface{}
	if md.Info != nil {
		_ = json.Unmarshal(md.Info, &info)
	}
	return collection.Metadata{
		Key:       md.Key,
		Roles:     RolesFromPb(md.Roles),
		UpdatedAt: md.UpdatedAt,
		Info:      info,
	}
}

func ItemToPb(item *buckets.PathItem) *pb.PathItem {
	items := make([]*pb.PathItem, len(item.Items))
	for j, i := range item.Items {
		items[j] = ItemToPb(&i)
	}
	return &pb.PathItem{
		Cid:        item.Cid,
		Name:       item.Name,
		Path:       item.Path,
		Size:       item.Size,
		IsDir:      item.IsDir,
		Items:      items,
		ItemsCount: item.ItemsCount,
		Metadata:   MetadataToPb(item.Metadata),
	}
}

func RolesToPb(roles map[did.DID]collection.Role) map[string]pb.PathAccessRole {
	proles := make(map[string]pb.PathAccessRole)
	for k, r := range roles {
		var pr pb.PathAccessRole
		switch r {
		case collection.ReaderRole:
			pr = pb.PathAccessRole_PATH_ACCESS_ROLE_READER
		case collection.WriterRole:
			pr = pb.PathAccessRole_PATH_ACCESS_ROLE_WRITER
		case collection.AdminRole:
			pr = pb.PathAccessRole_PATH_ACCESS_ROLE_ADMIN
		default:
			pr = pb.PathAccessRole_PATH_ACCESS_ROLE_UNSPECIFIED
		}
		proles[string(k)] = pr
	}
	return proles
}

func RolesFromPb(roles map[string]pb.PathAccessRole) map[did.DID]collection.Role {
	croles := make(map[did.DID]collection.Role)
	for k, pr := range roles {
		var r collection.Role
		switch pr {
		case pb.PathAccessRole_PATH_ACCESS_ROLE_READER:
			r = collection.ReaderRole
		case pb.PathAccessRole_PATH_ACCESS_ROLE_WRITER:
			r = collection.WriterRole
		case pb.PathAccessRole_PATH_ACCESS_ROLE_ADMIN:
			r = collection.AdminRole
		default:
			r = collection.NoneRole
		}
		croles[did.DID(k)] = r
	}
	return croles
}

func InfoToPb(info map[string]interface{}) ([]byte, error) {
	return json.Marshal(info)
}

func InfoFromPb(info []byte) (map[string]interface{}, error) {
	var pinfo map[string]interface{}
	if err := json.Unmarshal(info, &pinfo); err != nil {
		return nil, err
	}
	return pinfo, nil
}

func LinksToPb(links buckets.Links) *pb.Links {
	return &pb.Links{
		Url:  links.URL,
		Www:  links.WWW,
		Ipns: links.IPNS,
		Bps:  links.BPS,
	}
}

func LinksFromPb(links *pb.Links) buckets.Links {
	return buckets.Links{
		URL:  links.Url,
		WWW:  links.Www,
		IPNS: links.Ipns,
		BPS:  links.Bps,
	}
}
