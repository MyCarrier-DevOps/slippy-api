package infrastructure

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- Mock SlipResolver ---

type mockSlipResolver struct {
	resolveSlipFn func(ctx context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error)
}

func (m *mockSlipResolver) ResolveSlip(ctx context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
	return m.resolveSlipFn(ctx, opts)
}

// --- Constructor ---

func TestNewSlipResolverAdapter(t *testing.T) {
	reader := &forkAwareMockReader{}
	resolver := &mockSlipResolver{}
	adapter := NewSlipResolverAdapter(resolver, reader)
	assert.NotNil(t, adapter)
}

// --- Load passthrough ---

func TestAdapter_Load_Passthrough(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "abc-123"}
	reader := &forkAwareMockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return expected, nil
		},
	}

	adapter := NewSlipResolverAdapter(&mockSlipResolver{}, reader)
	slip, err := adapter.Load(context.Background(), "abc-123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

// --- LoadByCommit ---

func TestAdapter_LoadByCommit_ResolvedViaAncestry(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ancestor-slip", Repository: "org/repo"}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			assert.Equal(t, "org/repo", opts.Repository)
			assert.Equal(t, "sha123", opts.Ref)
			return &slippy.ResolveResult{
				Slip:          expected,
				ResolvedBy:    "ancestry",
				MatchedCommit: "parent-sha",
			}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, &forkAwareMockReader{})
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

func TestAdapter_LoadByCommit_ResolvedViaImageTag(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "tag-slip"}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return &slippy.ResolveResult{
				Slip:          expected,
				ResolvedBy:    "image_tag",
				MatchedCommit: "abc1234",
			}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, &forkAwareMockReader{})
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

func TestAdapter_LoadByCommit_FallsBackToReaderOnNotFound(t *testing.T) {
	// Ancestry resolution fails, but fork-aware reader finds the slip
	// under a different repo name.
	forkSlip := &domain.Slip{CorrelationID: "fork-slip", Repository: "parent/repo"}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, slippy.NewResolveError(opts.Repository, opts.Ref, slippy.ErrSlipNotFound)
		},
	}
	reader := &forkAwareMockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return forkSlip, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommit(context.Background(), "fork/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, forkSlip, slip)
}

func TestAdapter_LoadByCommit_NotFoundEverywhere(t *testing.T) {
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, slippy.NewResolveError(opts.Repository, opts.Ref, slippy.ErrSlipNotFound)
		},
	}
	reader := &forkAwareMockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "sha123")
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
}

func TestAdapter_LoadByCommit_NonNotFoundErrorPassthrough(t *testing.T) {
	resolverErr := errors.New("github API rate limited")
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, resolverErr
		},
	}

	adapter := NewSlipResolverAdapter(resolver, &forkAwareMockReader{})
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "sha123")
	assert.ErrorIs(t, err, resolverErr)
	assert.Nil(t, slip)
}

func TestAdapter_LoadByCommit_InvalidRepositoryError(t *testing.T) {
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, slippy.ErrInvalidRepository
		},
	}

	adapter := NewSlipResolverAdapter(resolver, &forkAwareMockReader{})
	slip, err := adapter.LoadByCommit(context.Background(), "no-slash", "sha123")
	assert.ErrorIs(t, err, slippy.ErrInvalidRepository)
	assert.Nil(t, slip)
}

// --- FindByCommits passthrough ---

func TestAdapter_FindByCommits_Passthrough(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ghi-789"}
	reader := &forkAwareMockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return expected, "c1", nil
		},
	}

	adapter := NewSlipResolverAdapter(&mockSlipResolver{}, reader)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, "c1", commit)
}

// --- FindAllByCommits passthrough ---

func TestAdapter_FindAllByCommits_Passthrough(t *testing.T) {
	expected := []domain.SlipWithCommit{
		{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
	}
	reader := &forkAwareMockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return expected, nil
		},
	}

	adapter := NewSlipResolverAdapter(&mockSlipResolver{}, reader)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "a", results[0].Slip.CorrelationID)
}
