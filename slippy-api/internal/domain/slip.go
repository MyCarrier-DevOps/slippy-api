package domain

import (
	"context"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// Slip is an alias for the upstream slippy.Slip type.
// This keeps domain consumers decoupled from direct import of the library package,
// while avoiding unnecessary type duplication (DRY).
type Slip = slippy.Slip

// SlipWithCommit pairs a slip with the commit SHA that matched it.
type SlipWithCommit = slippy.SlipWithCommit

// Write-related type aliases — decouple domain consumers from the library package.
type (
	StepStatus          = slippy.StepStatus
	StateHistoryEntry   = slippy.StateHistoryEntry
	AncestryEntry       = slippy.AncestryEntry
	PushOptions         = slippy.PushOptions
	CreateSlipResult    = slippy.CreateSlipResult
	ComponentDefinition = slippy.ComponentDefinition
)

// SlipReader defines the read-only interface for querying routing slips.
// This is the contract that handlers depend on — implementations include the
// ClickHouse store adapter and the caching decorator.
type SlipReader interface {
	// Load retrieves a slip by its correlation ID
	Load(ctx context.Context, correlationID string) (*Slip, error)

	// LoadByCommit retrieves a slip by repository and commit SHA
	LoadByCommit(ctx context.Context, repository, commitSHA string) (*Slip, error)

	// FindByCommits finds the first matching slip for an ordered list of commits.
	// Returns the slip and the matched commit SHA.
	FindByCommits(ctx context.Context, repository string, commits []string) (*Slip, string, error)

	// FindAllByCommits finds all slips matching any commit in the ordered list.
	FindAllByCommits(ctx context.Context, repository string, commits []string) ([]SlipWithCommit, error)
}

// Invalidator is a post-write hook that removes cached entries for a slip.
// Implementations must treat failures as non-fatal and log rather than propagate.
type Invalidator interface {
	InvalidateByCorrelationID(ctx context.Context, correlationID string)
}

// SlipWriter defines the write interface for mutating routing slips.
// Methods map to business-level operations used by pushhookparser (slip creation)
// and Slippy CI CLI (pre-job/post-job step lifecycle).
type SlipWriter interface {
	// CreateSlipForPush creates a new routing slip for a git push event,
	// including ancestry resolution and ancestor abandonment/promotion.
	CreateSlipForPush(ctx context.Context, opts PushOptions) (*CreateSlipResult, error)

	// StartStep marks a pipeline step as running.
	StartStep(ctx context.Context, correlationID, stepName, componentName string) error

	// CompleteStep marks a pipeline step as completed.
	CompleteStep(ctx context.Context, correlationID, stepName, componentName string) error

	// FailStep marks a pipeline step as failed with a reason.
	FailStep(ctx context.Context, correlationID, stepName, componentName, reason string) error

	// SkipStep marks a pipeline step as skipped with an optional reason.
	SkipStep(ctx context.Context, correlationID, stepName, componentName, reason string) error

	// SetComponentImageTag records the built container image tag for a component.
	SetComponentImageTag(ctx context.Context, correlationID, componentName, imageTag string) error

	// PromoteSlip marks a slip as promoted to another branch via a PR merge.
	PromoteSlip(ctx context.Context, correlationID, promotedTo string) error

	// AbandonSlip marks a slip as abandoned, superseded by a newer push.
	AbandonSlip(ctx context.Context, correlationID, supersededBy string) error
}
