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

// --- Mock SlipReader ---

type mockReader struct {
	loadFn             func(ctx context.Context, id string) (*domain.Slip, error)
	loadByCommitFn     func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	findByCommitsFn    func(ctx context.Context, repo string, commits []string) (*domain.Slip, string, error)
	findAllByCommitsFn func(ctx context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error)
}

func (m *mockReader) Load(ctx context.Context, id string) (*domain.Slip, error) {
	return m.loadFn(ctx, id)
}

func (m *mockReader) LoadByCommit(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitFn(ctx, repo, sha)
}

func (m *mockReader) FindByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) (*domain.Slip, string, error) {
	return m.findByCommitsFn(ctx, repo, commits)
}

func (m *mockReader) FindAllByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	return m.findAllByCommitsFn(ctx, repo, commits)
}

// --- Mock SlipResolver ---

type mockSlipResolver struct {
	resolveSlipFn func(ctx context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error)
}

func (m *mockSlipResolver) ResolveSlip(ctx context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
	return m.resolveSlipFn(ctx, opts)
}

// --- Constructor ---

func TestNewSlipResolverAdapter(t *testing.T) {
	reader := &mockReader{}
	resolver := &mockSlipResolver{}
	adapter := NewSlipResolverAdapter(resolver, reader)
	assert.NotNil(t, adapter)
}

// --- Load passthrough ---

func TestAdapter_Load_Passthrough(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "abc-123"}
	reader := &mockReader{
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

	adapter := NewSlipResolverAdapter(resolver, &mockReader{})
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

	adapter := NewSlipResolverAdapter(resolver, &mockReader{})
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

func TestAdapter_LoadByCommit_NotFound(t *testing.T) {
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, slippy.NewResolveError(opts.Repository, opts.Ref, slippy.ErrSlipNotFound)
		},
	}

	adapter := NewSlipResolverAdapter(resolver, &mockReader{})
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

	adapter := NewSlipResolverAdapter(resolver, &mockReader{})
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

	adapter := NewSlipResolverAdapter(resolver, &mockReader{})
	slip, err := adapter.LoadByCommit(context.Background(), "no-slash", "sha123")
	assert.ErrorIs(t, err, slippy.ErrInvalidRepository)
	assert.Nil(t, slip)
}

// --- FindByCommits ---

func TestAdapter_FindByCommits_DirectHit(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ghi-789"}
	reader := &mockReader{
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

func TestAdapter_FindByCommits_AncestryFallback(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ancestor-slip"}
	reader := &mockReader{
		findByCommitsFn: func(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return &slippy.ResolveResult{
				Slip:          expected,
				ResolvedBy:    "ancestry",
				MatchedCommit: "parent-sha",
			}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"sha1"})
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, "sha1", commit)
}

func TestAdapter_FindByCommits_AncestryFallback_SecondCommit(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "second-slip"}
	reader := &mockReader{
		findByCommitsFn: func(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			if opts.Ref == "sha1" {
				return nil, slippy.NewResolveError(opts.Repository, opts.Ref, slippy.ErrSlipNotFound)
			}
			return &slippy.ResolveResult{
				Slip:          expected,
				ResolvedBy:    "ancestry",
				MatchedCommit: "ancestor-of-sha2",
			}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"sha1", "sha2"})
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, "sha2", commit)
}

func TestAdapter_FindByCommits_AncestryNotFound(t *testing.T) {
	reader := &mockReader{
		findByCommitsFn: func(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, slippy.NewResolveError(opts.Repository, opts.Ref, slippy.ErrSlipNotFound)
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"sha1"})
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

func TestAdapter_FindByCommits_ReaderError(t *testing.T) {
	readerErr := errors.New("clickhouse timeout")
	reader := &mockReader{
		findByCommitsFn: func(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
			return nil, "", readerErr
		},
	}

	adapter := NewSlipResolverAdapter(&mockSlipResolver{}, reader)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"sha1"})
	assert.ErrorIs(t, err, readerErr)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

func TestAdapter_FindByCommits_ResolverNonNotFoundError(t *testing.T) {
	reader := &mockReader{
		findByCommitsFn: func(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}
	resolverErr := errors.New("github API error")
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, _ slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, resolverErr
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"sha1"})
	assert.ErrorIs(t, err, resolverErr)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

// --- FindAllByCommits ---

func TestAdapter_FindAllByCommits_DirectHit(t *testing.T) {
	expected := []domain.SlipWithCommit{
		{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
	}
	reader := &mockReader{
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

func TestAdapter_FindAllByCommits_AncestryFallback(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ancestor-slip"}
	reader := &mockReader{
		findAllByCommitsFn: func(_ context.Context, _ string, _ []string) ([]domain.SlipWithCommit, error) {
			return nil, nil
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return &slippy.ResolveResult{
				Slip:          expected,
				ResolvedBy:    "ancestry",
				MatchedCommit: "parent-sha",
			}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"sha1", "sha2"})
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, "ancestor-slip", results[0].Slip.CorrelationID)
}

func TestAdapter_FindAllByCommits_AncestryPartialMatch(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "only-one"}
	reader := &mockReader{
		findAllByCommitsFn: func(_ context.Context, _ string, _ []string) ([]domain.SlipWithCommit, error) {
			return nil, nil
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			if opts.Ref == "sha1" {
				return &slippy.ResolveResult{
					Slip:          expected,
					ResolvedBy:    "ancestry",
					MatchedCommit: "parent-of-sha1",
				}, nil
			}
			return nil, slippy.NewResolveError(opts.Repository, opts.Ref, slippy.ErrSlipNotFound)
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"sha1", "sha2"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "sha1", results[0].MatchedCommit)
}

func TestAdapter_FindAllByCommits_ReaderError(t *testing.T) {
	readerErr := errors.New("clickhouse timeout")
	reader := &mockReader{
		findAllByCommitsFn: func(_ context.Context, _ string, _ []string) ([]domain.SlipWithCommit, error) {
			return nil, readerErr
		},
	}

	adapter := NewSlipResolverAdapter(&mockSlipResolver{}, reader)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"sha1"})
	assert.ErrorIs(t, err, readerErr)
	assert.Nil(t, results)
}

func TestAdapter_FindAllByCommits_ResolverError(t *testing.T) {
	reader := &mockReader{
		findAllByCommitsFn: func(_ context.Context, _ string, _ []string) ([]domain.SlipWithCommit, error) {
			return nil, nil
		},
	}
	resolverErr := errors.New("github API error")
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, _ slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			return nil, resolverErr
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"sha1"})
	assert.ErrorIs(t, err, resolverErr)
	assert.Nil(t, results)
}
