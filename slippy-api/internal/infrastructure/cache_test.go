package infrastructure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- Mock SlipReader for testing ---

type mockSlipReader struct {
	loadFn             func(ctx context.Context, id string) (*domain.Slip, error)
	loadByCommitFn     func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	findByCommitsFn    func(ctx context.Context, repo string, commits []string) (*domain.Slip, string, error)
	findAllByCommitsFn func(ctx context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error)
}

func (m *mockSlipReader) Load(ctx context.Context, id string) (*domain.Slip, error) {
	return m.loadFn(ctx, id)
}

func (m *mockSlipReader) LoadByCommit(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitFn(ctx, repo, sha)
}

func (m *mockSlipReader) FindByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) (*domain.Slip, string, error) {
	return m.findByCommitsFn(ctx, repo, commits)
}

func (m *mockSlipReader) FindAllByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	return m.findAllByCommitsFn(ctx, repo, commits)
}

// --- CachedSlipReader Unit Tests ---

func TestNewCachedSlipReader(t *testing.T) {
	mock := &mockSlipReader{}
	// Verify constructor doesn't panic and returns non-nil
	// We pass nil for redis.Cmdable since we're just testing construction
	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	assert.NotNil(t, cached)
}

func TestNewCachedSlipReader_ZeroTTL(t *testing.T) {
	mock := &mockSlipReader{}
	cached := NewCachedSlipReader(mock, nil, 0)
	assert.NotNil(t, cached)
}

func TestCachedSlipReader_LoadDelegates(t *testing.T) {
	expectedSlip := &domain.Slip{CorrelationID: "abc-123"}
	mock := &mockSlipReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			assert.Equal(t, "abc-123", id)
			return expectedSlip, nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	slip, err := cached.Load(context.Background(), "abc-123")
	require.NoError(t, err)
	assert.Equal(t, expectedSlip, slip)
}

func TestCachedSlipReader_LoadDelegatesError(t *testing.T) {
	mock := &mockSlipReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return nil, errors.New("store unavailable")
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	slip, err := cached.Load(context.Background(), "abc-123")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "store unavailable")
	assert.Nil(t, slip)
}

func TestCachedSlipReader_LoadByCommitDelegates(t *testing.T) {
	expectedSlip := &domain.Slip{CorrelationID: "def-456", Repository: "org/repo"}
	mock := &mockSlipReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, "abc123", sha)
			return expectedSlip, nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	slip, err := cached.LoadByCommit(context.Background(), "org/repo", "abc123")
	require.NoError(t, err)
	assert.Equal(t, expectedSlip, slip)
}

func TestCachedSlipReader_LoadByCommitDelegatesError(t *testing.T) {
	mock := &mockSlipReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, errors.New("timeout")
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	slip, err := cached.LoadByCommit(context.Background(), "org/repo", "sha")
	assert.Error(t, err)
	assert.Nil(t, slip)
}

func TestCachedSlipReader_FindByCommitsDelegates(t *testing.T) {
	callCount := 0
	expectedSlip := &domain.Slip{CorrelationID: "xyz-789"}
	mock := &mockSlipReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			callCount++
			return expectedSlip, "commit-1", nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	slip, commit, err := cached.FindByCommits(context.Background(), "org/repo", []string{"commit-1", "commit-2"})
	require.NoError(t, err)
	assert.Equal(t, expectedSlip, slip)
	assert.Equal(t, "commit-1", commit)
	assert.Equal(t, 1, callCount)
}

func TestCachedSlipReader_FindByCommitsPropagatesError(t *testing.T) {
	mock := &mockSlipReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return nil, "", errors.New("store error")
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	slip, commit, err := cached.FindByCommits(context.Background(), "org/repo", []string{"c1"})
	assert.Error(t, err)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

func TestCachedSlipReader_FindAllByCommitsDelegates(t *testing.T) {
	callCount := 0
	expectedResults := []domain.SlipWithCommit{
		{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
		{Slip: &domain.Slip{CorrelationID: "b"}, MatchedCommit: "c2"},
	}
	mock := &mockSlipReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			callCount++
			return expectedResults, nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	results, err := cached.FindAllByCommits(context.Background(), "org/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, 1, callCount)
}

func TestCachedSlipReader_FindAllByCommitsPropagatesError(t *testing.T) {
	mock := &mockSlipReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return nil, errors.New("connection reset")
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	results, err := cached.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	assert.Error(t, err)
	assert.Nil(t, results)
}

func TestCachedSlipReader_FindAllByCommits_EmptyResult(t *testing.T) {
	mock := &mockSlipReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return []domain.SlipWithCommit{}, nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 10*time.Minute)
	results, err := cached.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	require.NoError(t, err)
	assert.Empty(t, results)
}
