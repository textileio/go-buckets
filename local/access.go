package local

import (
	"context"

	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
)

// PushPathAccessRoles updates path access roles by merging the pushed roles with existing roles
// and returns the merged roles.
// roles is a map of string marshaled public keys to path roles. A non-nil error is returned
// if the map keys are not unmarshalable to public keys.
// To delete a role for a public key, set its value to buckets.None.
func (b *Bucket) PushPathAccessRoles(
	ctx context.Context,
	pth string,
	roles map[did.DID]collection.Role,
) (merged map[did.DID]collection.Role, err error) {
	ctx, err = b.authCtx(ctx)
	if err != nil {
		return
	}
	id, err := b.Thread()
	if err != nil {
		return
	}
	if err = b.c.PushPathAccessRoles(ctx, id, b.Key(), pth, roles); err != nil {
		return
	}
	return b.c.PullPathAccessRoles(ctx, id, b.Key(), pth)
}

// PullPathAccessRoles returns access roles for a path.
func (b *Bucket) PullPathAccessRoles(ctx context.Context, pth string) (roles map[did.DID]collection.Role, err error) {
	ctx, err = b.authCtx(ctx)
	if err != nil {
		return
	}
	id, err := b.Thread()
	if err != nil {
		return
	}
	return b.c.PullPathAccessRoles(ctx, id, b.Key(), pth)
}
