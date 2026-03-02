package domain

import "context"

// BuildScope constants define the supported build scope values, mirroring the
// custom property stored in ci.repoproperties.
const (
	// BuildScopeAll indicates all images in the repository were built together,
	// so every component shares the same slip-computed tag.
	BuildScopeAll = "all"

	// BuildScopeModified indicates only modified components were built, so each
	// component has its own tag resolved from ci.buildinfo.
	BuildScopeModified = "modified"
)

// ImageTagResult holds the resolved image tags for a routing slip's build.
type ImageTagResult struct {
	// Tags maps component name → image tag.
	// In build_scope=all mode every component shares the slip-computed tag.
	// In build_scope=modified mode each component carries its actual ci.buildinfo tag.
	Tags map[string]string `json:"tags"`

	// BuildScope is the resolved build_scope from ci.repoproperties ("all" or "modified").
	BuildScope string `json:"build_scope"`

	// SlipTag is the YY.WW.SHA7 tag computed from the routing slip metadata.
	// Returned for informational purposes (always populated when a slip exists).
	SlipTag string `json:"slip_tag"`
}

// ImageTagReader resolves per-component image tags for a given correlation ID.
// The implementation queries ci.buildinfo and ci.repoproperties in ClickHouse,
// using the routing slip to derive the repository context and slip-computed tag.
type ImageTagReader interface {
	// ResolveImageTags looks up the slip by correlationID, determines build_scope
	// from ci.repoproperties, and returns per-component image tags.
	//
	// For build_scope=all: all components receive the slip-computed tag (YY.WW.SHA7).
	// For build_scope=modified: each component receives its actual tag from ci.buildinfo.
	ResolveImageTags(ctx context.Context, correlationID string) (*ImageTagResult, error)
}
