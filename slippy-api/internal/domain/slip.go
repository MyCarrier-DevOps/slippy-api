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
