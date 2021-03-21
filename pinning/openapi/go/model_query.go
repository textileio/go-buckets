package openapi

import (
	"time"
)

// Query represents Pin query parameters.
// This is derived from the openapi spec using https://github.com/deepmap/oapi-codegen, not
// https://github.com/OpenAPITools/openapi-generator, which doens't output anything for the
// listPins query body.
type Query struct {
	// Cid can be used to filter by one or more Pin Cids.
	Cid []string `form:"cid" json:"cid,omitempty"`
	// Name can be used to filer by Pin name (by default case-sensitive, exact match).
	Name string `form:"name" json:"name,omitempty"`
	// Match can be used to customize the text matching strategy applied when Name is present.
	Match TextMatchingStrategy `form:"match" json:"match,omitempty"`
	// Status can be used to filter by Pin status.
	Status []Status `form:"status" json:"status,omitempty"`
	// Before can by used to filter by before creation (queued) time.
	Before *time.Time `form:"before" json:"before,omitempty"`
	// After can by used to filter by after creation (queued) time.
	After *time.Time `form:"after" json:"after,omitempty"`
	// Limit specifies the max number of Pins to return.
	Limit *int32 `form:"limit" json:"limit,omitempty"`
	// Meta can be used to filter results by Pin metadata.
	Meta *map[string]string `form:"meta" json:"meta,omitempty"`
}
