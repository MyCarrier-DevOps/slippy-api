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
	loadFn              func(ctx context.Context, id string) (*domain.Slip, error)
	loadByCommitFn      func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	loadByCommitExactFn func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	findByCommitsFn     func(ctx context.Context, repo string, commits []string) (*domain.Slip, string, error)
	findAllByCommitsFn  func(ctx context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error)
}

func (m *mockReader) Load(ctx context.Context, id string) (*domain.Slip, error) {
	return m.loadFn(ctx, id)
}

func (m *mockReader) LoadByCommit(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitFn(ctx, repo, sha)
}

func (m *mockReader) LoadByCommitExact(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitExactFn(ctx, repo, sha)
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

// --- LoadByCommit exact-first (full-SHA) behavior ---

// fullSHA is a valid 40-hex commit SHA used by the exact-first tests.
const fullSHA = "f615c4c0000000000000000000000000deadbeef"

// TestAdapter_LoadByCommit_FullSHA_ExactHit is the regression guard for the
// 404 flap: a GET by an explicit full 40-hex SHA for a live slip MUST resolve
// via the direct exact-SHA read and MUST NOT touch the GitHub ancestry resolver.
func TestAdapter_LoadByCommit_FullSHA_ExactHit(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "live-slip", Repository: "org/repo"}
	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, fullSHA, sha)
			return expected, nil
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, _ slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			t.Fatal("resolver MUST NOT be called when an exact full-SHA slip exists — guards the 404 flap")
			return nil, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", fullSHA)
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

// TestAdapter_LoadByCommit_FullSHA_ExactMissFallsBackToAncestry asserts that a
// full SHA with no exact live slip falls back to the ancestry walk and returns
// the ancestor's slip.
//
// NOTE: reader.LoadByCommitExact maps to LoadLiveByCommit, which filters out
// slips whose status is terminal (NOT IN ('abandoned','promoted','compensated')).
// So an ErrSlipNotFound here is NOT only "no row at all" — it ALSO covers the
// case where a row exists for the exact SHA but its status is terminal and was
// excluded by the live filter. In both cases LoadByCommit must fall back to the
// ancestry resolver (this test), never short-circuit to a wrong/empty result.
// See TestAdapter_LoadByCommit_FullSHA_TerminalStatusExactMiss_FallsBackToAncestry
// for the same path asserted explicitly against the terminal-status semantics.
func TestAdapter_LoadByCommit_FullSHA_ExactMissFallsBackToAncestry(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ancestor-slip", Repository: "org/repo"}
	exactCalls := 0
	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, _, _ string) (*domain.Slip, error) {
			exactCalls++
			return nil, slippy.ErrSlipNotFound
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			assert.Equal(t, "org/repo", opts.Repository)
			assert.Equal(t, fullSHA, opts.Ref)
			return &slippy.ResolveResult{
				Slip:          expected,
				ResolvedBy:    "ancestry",
				MatchedCommit: "parent-sha",
			}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", fullSHA)
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, 1, exactCalls, "exact read must be attempted exactly once before ancestry fallback")
}

// TestAdapter_LoadByCommit_FullSHA_TerminalStatusExactMiss_FallsBackToAncestry
// pins the no-regression claim for the exact-SHA-first change: when the exact
// SHA belongs to a slip whose status is TERMINAL (abandoned/promoted/compensated),
// LoadLiveByCommit (behind reader.LoadByCommitExact) filters it out and returns
// slippy.ErrSlipNotFound. In that case LoadByCommit MUST fall back to the ancestry
// resolver and return the resolver's result — a terminal-status exact slip must
// NOT short-circuit LoadByCommit to nil / "not found".
//
// This guards the failure mode where a promoted/abandoned slip on the exact SHA
// would otherwise mask the live lineage slip the caller actually wants.
func TestAdapter_LoadByCommit_FullSHA_TerminalStatusExactMiss_FallsBackToAncestry(t *testing.T) {
	// The live lineage slip the ancestry walk is expected to surface once the
	// terminal-status exact slip is filtered out by LoadLiveByCommit.
	expected := &domain.Slip{CorrelationID: "live-lineage-slip", Repository: "org/repo"}

	exactCalls := 0
	resolverCalls := 0

	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			exactCalls++
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, fullSHA, sha)
			// Simulate LoadLiveByCommit excluding a slip whose status is terminal
			// (e.g. promoted/abandoned/compensated): the live-status filter yields
			// zero rows, which collapses to ErrSlipNotFound.
			return nil, slippy.ErrSlipNotFound
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			resolverCalls++
			assert.Equal(t, "org/repo", opts.Repository)
			assert.Equal(t, fullSHA, opts.Ref)
			return &slippy.ResolveResult{
				Slip:          expected,
				ResolvedBy:    "ancestry",
				MatchedCommit: "parent-sha",
			}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", fullSHA)

	require.NoError(t, err)
	assert.Equal(t, expected, slip, "must return the ancestry resolver's live-lineage slip, not the filtered terminal slip")
	assert.Equal(t, 1, exactCalls, "exact read must be attempted exactly once before ancestry fallback")
	assert.Equal(t, 1, resolverCalls, "ancestry resolver MUST be invoked when the exact slip is terminal-status-filtered")
}

// TestAdapter_LoadByCommit_BranchRef_SkipsExact asserts a non-full-SHA ref (a
// branch name) goes straight to ancestry and never attempts the exact read.
func TestAdapter_LoadByCommit_BranchRef_SkipsExact(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "branch-slip", Repository: "org/repo"}
	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, _, _ string) (*domain.Slip, error) {
			t.Fatal("exact read MUST NOT be attempted for a non-full-SHA branch ref")
			return nil, nil
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			assert.Equal(t, "integration", opts.Ref)
			return &slippy.ResolveResult{Slip: expected, ResolvedBy: "ancestry", MatchedCommit: "p"}, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "integration")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

// TestAdapter_LoadByCommit_FullSHA_ExactStoreErrorPropagates asserts that a
// non-not-found error from the exact read (e.g. ClickHouse down) is propagated
// and NOT masked as a not-found / 404, and that ancestry is not consulted.
func TestAdapter_LoadByCommit_FullSHA_ExactStoreErrorPropagates(t *testing.T) {
	storeErr := errors.New("clickhouse timeout")
	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, _, _ string) (*domain.Slip, error) {
			return nil, storeErr
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, _ slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			t.Fatal("resolver MUST NOT be called when the exact read returns an infrastructure error")
			return nil, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", fullSHA)
	assert.ErrorIs(t, err, storeErr)
	assert.NotErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
}

// TestIsFullCommitSHA covers the SHA-classification helper.
func TestIsFullCommitSHA(t *testing.T) {
	cases := []struct {
		name string
		ref  string
		want bool
	}{
		{"full lowercase hex", "f615c4c0000000000000000000000000deadbeef", true},
		{"full uppercase hex", "F615C4C0000000000000000000000000DEADBEEF", true},
		{"branch name", "integration", false},
		{"short sha", "f615c4c", false},
		{"empty", "", false},
		{"41 chars", "f615c4c0000000000000000000000000deadbeef0", false},
		{"39 chars", "f615c4c0000000000000000000000000deadbee", false},
		{"non-hex char at 40 len", "g615c4c0000000000000000000000000deadbeef", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isFullCommitSHA(tc.ref))
		})
	}
}

// --- LoadByCommitExact ---

// TestSlipResolverAdapter_LoadByCommitExact_BypassesResolver asserts the adapter
// delegates directly to reader.LoadByCommitExact and NEVER consults the resolver.
// This is the core regression guard for the dedup-loser stuck-slip bug — anyone who
// tries to "improve" the method by adding ancestry fallback will fail this test.
func TestSlipResolverAdapter_LoadByCommitExact_BypassesResolver(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "live-slip", Repository: "org/repo"}
	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, "sha123", sha)
			return expected, nil
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, _ slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			t.Fatal("resolver MUST NOT be called by LoadByCommitExact — exact-SHA semantics demand bypass")
			return nil, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommitExact(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

// TestSlipResolverAdapter_LoadByCommitExact_ReturnsNotFoundOnEmpty asserts
// ErrSlipNotFound propagates from the reader without falling back to ancestry.
func TestSlipResolverAdapter_LoadByCommitExact_ReturnsNotFoundOnEmpty(t *testing.T) {
	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, _, _ string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}
	resolver := &mockSlipResolver{
		resolveSlipFn: func(_ context.Context, _ slippy.ResolveOptions) (*slippy.ResolveResult, error) {
			t.Fatal("resolver MUST NOT be called when exact lookup returns not-found")
			return nil, nil
		},
	}

	adapter := NewSlipResolverAdapter(resolver, reader)
	slip, err := adapter.LoadByCommitExact(context.Background(), "org/repo", "sha123")
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
}

// TestSlipResolverAdapter_LoadByCommitExact_StoreError asserts non-not-found
// errors propagate unchanged.
func TestSlipResolverAdapter_LoadByCommitExact_StoreError(t *testing.T) {
	storeErr := errors.New("clickhouse timeout")
	reader := &mockReader{
		loadByCommitExactFn: func(_ context.Context, _, _ string) (*domain.Slip, error) {
			return nil, storeErr
		},
	}

	adapter := NewSlipResolverAdapter(&mockSlipResolver{}, reader)
	slip, err := adapter.LoadByCommitExact(context.Background(), "org/repo", "sha123")
	assert.ErrorIs(t, err, storeErr)
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
