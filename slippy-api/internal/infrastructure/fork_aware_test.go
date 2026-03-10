package infrastructure

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse/clickhousetest"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- Mock SlipReader for fork-aware tests ---

type forkAwareMockReader struct {
	loadFn             func(ctx context.Context, id string) (*domain.Slip, error)
	loadByCommitFn     func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	findByCommitsFn    func(ctx context.Context, repo string, commits []string) (*domain.Slip, string, error)
	findAllByCommitsFn func(ctx context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error)
}

func (m *forkAwareMockReader) Load(ctx context.Context, id string) (*domain.Slip, error) {
	return m.loadFn(ctx, id)
}

func (m *forkAwareMockReader) LoadByCommit(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitFn(ctx, repo, sha)
}

func (m *forkAwareMockReader) FindByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) (*domain.Slip, string, error) {
	return m.findByCommitsFn(ctx, repo, commits)
}

func (m *forkAwareMockReader) FindAllByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	return m.findAllByCommitsFn(ctx, repo, commits)
}

// --- Constructor ---

func TestNewForkAwareSlipReader(t *testing.T) {
	reader := &forkAwareMockReader{}
	session := &clickhousetest.MockSession{}
	fa := NewForkAwareSlipReader(reader, session, "ci")
	assert.NotNil(t, fa)
}

// --- Load passthrough (no fork logic) ---

func TestForkAware_Load_Passthrough(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "abc-123"}
	reader := &forkAwareMockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			assert.Equal(t, "abc-123", id)
			return expected, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, &clickhousetest.MockSession{}, "ci")
	slip, err := fa.Load(context.Background(), "abc-123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

// --- LoadByCommit ---

func TestForkAware_LoadByCommit_DirectHit(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "def-456", Repository: "org/repo"}
	reader := &forkAwareMockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return expected, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, &clickhousetest.MockSession{}, "ci")
	slip, err := fa.LoadByCommit(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

func TestForkAware_LoadByCommit_ForkFallback(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "def-456", Repository: "org/repo", CommitSHA: "sha123"}

	reader := &forkAwareMockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			if repo == "fork-user/repo" {
				return nil, slippy.ErrSlipNotFound
			}
			if repo == "org/repo" {
				return expected, nil
			}
			return nil, slippy.ErrSlipNotFound
		},
	}

	session := &clickhousetest.MockSession{
		QueryRowFunc: func(_ context.Context, query string, args ...any) ch.Row {
			return &clickhousetest.MockRow{
				ScanData: []any{"org/repo"},
			}
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	slip, err := fa.LoadByCommit(context.Background(), "fork-user/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, "org/repo", slip.Repository)
}

func TestForkAware_LoadByCommit_NonNotFoundError(t *testing.T) {
	dbErr := errors.New("connection refused")
	reader := &forkAwareMockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, dbErr
		},
	}

	fa := NewForkAwareSlipReader(reader, &clickhousetest.MockSession{}, "ci")
	slip, err := fa.LoadByCommit(context.Background(), "org/repo", "sha123")
	assert.ErrorIs(t, err, dbErr)
	assert.Nil(t, slip)
}

func TestForkAware_LoadByCommit_ResolveFailsReturnsOriginalError(t *testing.T) {
	reader := &forkAwareMockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	session := &clickhousetest.MockSession{
		QueryRowFunc: func(_ context.Context, query string, args ...any) ch.Row {
			return &clickhousetest.MockRow{
				ScanErr: errors.New("no rows"),
			}
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	slip, err := fa.LoadByCommit(context.Background(), "fork-user/repo", "sha123")
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
}

func TestForkAware_LoadByCommit_ResolveSameRepoReturnsOriginalError(t *testing.T) {
	reader := &forkAwareMockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	session := &clickhousetest.MockSession{
		QueryRowFunc: func(_ context.Context, query string, args ...any) ch.Row {
			// Resolves to the same repo — no point retrying.
			return &clickhousetest.MockRow{
				ScanData: []any{"org/repo"},
			}
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	slip, err := fa.LoadByCommit(context.Background(), "org/repo", "sha123")
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
}

// --- FindByCommits ---

func TestForkAware_FindByCommits_DirectHit(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ghi-789"}
	reader := &forkAwareMockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return expected, "c1", nil
		},
	}

	fa := NewForkAwareSlipReader(reader, &clickhousetest.MockSession{}, "ci")
	slip, commit, err := fa.FindByCommits(context.Background(), "org/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, "c1", commit)
}

func TestForkAware_FindByCommits_ForkFallback(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ghi-789", Repository: "org/repo"}

	reader := &forkAwareMockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			if repo == "fork-user/repo" {
				return nil, "", slippy.ErrSlipNotFound
			}
			if repo == "org/repo" {
				return expected, "c1", nil
			}
			return nil, "", slippy.ErrSlipNotFound
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanData: [][]any{{"org/repo"}},
			}, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	slip, commit, err := fa.FindByCommits(context.Background(), "fork-user/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, "c1", commit)
}

func TestForkAware_FindByCommits_ResolveFailsReturnsOriginalError(t *testing.T) {
	reader := &forkAwareMockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			return nil, errors.New("query failed")
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	slip, commit, err := fa.FindByCommits(context.Background(), "fork-user/repo", []string{"c1"})
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

func TestForkAware_FindByCommits_NonNotFoundError(t *testing.T) {
	dbErr := errors.New("timeout")
	reader := &forkAwareMockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return nil, "", dbErr
		},
	}

	fa := NewForkAwareSlipReader(reader, &clickhousetest.MockSession{}, "ci")
	slip, commit, err := fa.FindByCommits(context.Background(), "org/repo", []string{"c1"})
	assert.ErrorIs(t, err, dbErr)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

func TestForkAware_FindByCommits_ResolveEmptyReturnsOriginalError(t *testing.T) {
	reader := &forkAwareMockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{},
			}, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	slip, commit, err := fa.FindByCommits(context.Background(), "fork-user/repo", []string{"c1"})
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

// --- FindAllByCommits ---

func TestForkAware_FindAllByCommits_DirectHit(t *testing.T) {
	expected := []domain.SlipWithCommit{
		{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
	}
	reader := &forkAwareMockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return expected, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, &clickhousetest.MockSession{}, "ci")
	results, err := fa.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "a", results[0].Slip.CorrelationID)
}

func TestForkAware_FindAllByCommits_ForkFallback(t *testing.T) {
	expected := []domain.SlipWithCommit{
		{Slip: &domain.Slip{CorrelationID: "a", Repository: "org/repo"}, MatchedCommit: "c1"},
		{Slip: &domain.Slip{CorrelationID: "b", Repository: "org/repo"}, MatchedCommit: "c2"},
	}

	reader := &forkAwareMockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			if repo == "fork-user/repo" {
				return nil, nil // empty, no error
			}
			if repo == "org/repo" {
				return expected, nil
			}
			return nil, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanData: [][]any{{"org/repo"}},
			}, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	results, err := fa.FindAllByCommits(context.Background(), "fork-user/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, "a", results[0].Slip.CorrelationID)
	assert.Equal(t, "b", results[1].Slip.CorrelationID)
}

func TestForkAware_FindAllByCommits_ResolveFailsReturnsEmpty(t *testing.T) {
	reader := &forkAwareMockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return nil, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			return nil, errors.New("query failed")
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	results, err := fa.FindAllByCommits(context.Background(), "fork-user/repo", []string{"c1"})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestForkAware_FindAllByCommits_ErrorPassthrough(t *testing.T) {
	dbErr := errors.New("connection lost")
	reader := &forkAwareMockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return nil, dbErr
		},
	}

	fa := NewForkAwareSlipReader(reader, &clickhousetest.MockSession{}, "ci")
	results, err := fa.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	assert.ErrorIs(t, err, dbErr)
	assert.Nil(t, results)
}

func TestForkAware_FindAllByCommits_ResolveEmptyReturnsEmpty(t *testing.T) {
	reader := &forkAwareMockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return nil, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{},
			}, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	results, err := fa.FindAllByCommits(context.Background(), "fork-user/repo", []string{"c1"})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestForkAware_FindAllByCommits_MultipleRepos(t *testing.T) {
	// Commits span two different parent repos (unusual but possible).
	reader := &forkAwareMockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			switch repo {
			case "fork-user/repo":
				return nil, nil
			case "org/repo":
				return []domain.SlipWithCommit{
					{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
				}, nil
			case "other-org/repo":
				return []domain.SlipWithCommit{
					{Slip: &domain.Slip{CorrelationID: "b"}, MatchedCommit: "c2"},
				}, nil
			default:
				return nil, nil
			}
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true, true},
				ScanData: [][]any{{"org/repo"}, {"other-org/repo"}},
			}, nil
		},
	}

	fa := NewForkAwareSlipReader(reader, session, "ci")
	results, err := fa.FindAllByCommits(context.Background(), "fork-user/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Len(t, results, 2)
}
