package infrastructure

import (
	"context"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// SlipStoreAdapter adapts the upstream slippy.SlipStore (read+write) to the
// read-only domain.SlipReader interface. This ensures the API layer cannot
// accidentally invoke write operations.
type SlipStoreAdapter struct {
	store slippy.SlipStore
}

// NewSlipStoreAdapter wraps an upstream SlipStore as a read-only SlipReader.
func NewSlipStoreAdapter(store slippy.SlipStore) *SlipStoreAdapter {
	return &SlipStoreAdapter{store: store}
}

// Compile-time interface compliance check.
var _ domain.SlipReader = (*SlipStoreAdapter)(nil)

func (a *SlipStoreAdapter) Load(ctx context.Context, correlationID string) (*domain.Slip, error) {
	return a.store.Load(ctx, correlationID)
}

func (a *SlipStoreAdapter) LoadByCommit(ctx context.Context, repository, commitSHA string) (*domain.Slip, error) {
	return a.store.LoadByCommit(ctx, repository, commitSHA)
}

func (a *SlipStoreAdapter) FindByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) (*domain.Slip, string, error) {
	return a.store.FindByCommits(ctx, repository, commits)
}

func (a *SlipStoreAdapter) FindAllByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	return a.store.FindAllByCommits(ctx, repository, commits)
}

// Close releases resources held by the underlying store.
func (a *SlipStoreAdapter) Close() error {
	return a.store.Close()
}
