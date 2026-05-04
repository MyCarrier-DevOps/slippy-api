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

// --- mockSlipStore implements slippy.SlipStore for testing the adapter ---

type mockSlipStore struct {
	loadFn             func(ctx context.Context, id string) (*slippy.Slip, error)
	loadByCommitFn     func(ctx context.Context, repo, sha string) (*slippy.Slip, error)
	findByCommitsFn    func(ctx context.Context, repo string, commits []string) (*slippy.Slip, string, error)
	findAllByCommitsFn func(ctx context.Context, repo string, commits []string) ([]slippy.SlipWithCommit, error)
	closeFn            func() error

	// Write methods (unused by adapter, but required by the interface)
	createFn                func(ctx context.Context, slip *slippy.Slip) error
	updateFn                func(ctx context.Context, slip *slippy.Slip) error
	updateStepFn            func(ctx context.Context, id, step, comp string, status slippy.StepStatus) error
	updateStepWithHistoryFn func(ctx context.Context, id, step, comp string, status slippy.StepStatus, entry slippy.StateHistoryEntry) error
	updateComponentFn       func(ctx context.Context, id, comp, stepType string, status slippy.StepStatus) error
	updateSlipStatusFn      func(ctx context.Context, id string, status slippy.SlipStatus) error
	appendHistoryFn         func(ctx context.Context, id string, entry slippy.StateHistoryEntry) error
	setComponentImageTagFn  func(ctx context.Context, id, step, comp, tag string) error
	pingFn                  func(ctx context.Context) error
}

func (m *mockSlipStore) Load(ctx context.Context, id string) (*slippy.Slip, error) {
	return m.loadFn(ctx, id)
}

func (m *mockSlipStore) LoadByCommit(ctx context.Context, repo, sha string) (*slippy.Slip, error) {
	return m.loadByCommitFn(ctx, repo, sha)
}

func (m *mockSlipStore) FindByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) (*slippy.Slip, string, error) {
	return m.findByCommitsFn(ctx, repo, commits)
}

func (m *mockSlipStore) FindAllByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) ([]slippy.SlipWithCommit, error) {
	return m.findAllByCommitsFn(ctx, repo, commits)
}

func (m *mockSlipStore) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func (m *mockSlipStore) Create(ctx context.Context, slip *slippy.Slip) error {
	if m.createFn != nil {
		return m.createFn(ctx, slip)
	}
	return nil
}

func (m *mockSlipStore) Update(ctx context.Context, slip *slippy.Slip) error {
	if m.updateFn != nil {
		return m.updateFn(ctx, slip)
	}
	return nil
}

func (m *mockSlipStore) UpdateStep(ctx context.Context, id, step, comp string, status slippy.StepStatus) error {
	if m.updateStepFn != nil {
		return m.updateStepFn(ctx, id, step, comp, status)
	}
	return nil
}

func (m *mockSlipStore) UpdateStepWithHistory(
	ctx context.Context,
	id, step, comp string,
	status slippy.StepStatus,
	entry slippy.StateHistoryEntry,
) error {
	if m.updateStepWithHistoryFn != nil {
		return m.updateStepWithHistoryFn(ctx, id, step, comp, status, entry)
	}
	return nil
}

func (m *mockSlipStore) UpdateComponentStatus(
	ctx context.Context,
	id, comp, stepType string,
	status slippy.StepStatus,
) error {
	if m.updateComponentFn != nil {
		return m.updateComponentFn(ctx, id, comp, stepType, status)
	}
	return nil
}

func (m *mockSlipStore) UpdateSlipStatus(ctx context.Context, id string, status slippy.SlipStatus) error {
	if m.updateSlipStatusFn != nil {
		return m.updateSlipStatusFn(ctx, id, status)
	}
	return nil
}

func (m *mockSlipStore) AppendHistory(ctx context.Context, id string, entry slippy.StateHistoryEntry) error {
	if m.appendHistoryFn != nil {
		return m.appendHistoryFn(ctx, id, entry)
	}
	return nil
}

func (m *mockSlipStore) InsertAncestryLink(_ context.Context, _ *slippy.Slip, _ slippy.AncestryEntry) error {
	return nil
}

func (m *mockSlipStore) ResolveAncestry(_ context.Context, _, _, _ string, _ int) ([]slippy.AncestryEntry, error) {
	return nil, nil
}

func (m *mockSlipStore) SetComponentImageTag(ctx context.Context, id, step, comp, tag string) error {
	if m.setComponentImageTagFn != nil {
		return m.setComponentImageTagFn(ctx, id, step, comp, tag)
	}
	return nil
}

func (m *mockSlipStore) Ping(ctx context.Context) error {
	if m.pingFn != nil {
		return m.pingFn(ctx)
	}
	return nil
}

// --- Store Adapter Tests ---

func TestNewSlipStoreAdapter(t *testing.T) {
	store := &mockSlipStore{}
	adapter := NewSlipStoreAdapter(store)
	assert.NotNil(t, adapter)
}

func TestSlipStoreAdapter_Load_Success(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "abc-123", Repository: "org/repo"}
	store := &mockSlipStore{
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			assert.Equal(t, "abc-123", id)
			return expected, nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, err := adapter.Load(context.Background(), "abc-123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

func TestSlipStoreAdapter_Load_NotFound(t *testing.T) {
	store := &mockSlipStore{
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, err := adapter.Load(context.Background(), "missing")
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
}

func TestSlipStoreAdapter_Load_InvalidCorrelationID(t *testing.T) {
	store := &mockSlipStore{
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			return nil, slippy.ErrInvalidCorrelationID
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, err := adapter.Load(context.Background(), "")
	assert.ErrorIs(t, err, slippy.ErrInvalidCorrelationID)
	assert.Nil(t, slip)
}

func TestSlipStoreAdapter_LoadByCommit_Success(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "def-456", Repository: "org/repo", CommitSHA: "sha123"}
	store := &mockSlipStore{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*slippy.Slip, error) {
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, "sha123", sha)
			return expected, nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
}

func TestSlipStoreAdapter_LoadByCommit_NotFound(t *testing.T) {
	store := &mockSlipStore{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, err := adapter.LoadByCommit(context.Background(), "org/repo", "missing-sha")
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
}

func TestSlipStoreAdapter_LoadByCommit_InvalidRepository(t *testing.T) {
	store := &mockSlipStore{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*slippy.Slip, error) {
			return nil, slippy.ErrInvalidRepository
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, err := adapter.LoadByCommit(context.Background(), "invalid", "sha")
	assert.ErrorIs(t, err, slippy.ErrInvalidRepository)
	assert.Nil(t, slip)
}

func TestSlipStoreAdapter_FindByCommits_Success(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ghi-789"}
	store := &mockSlipStore{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*slippy.Slip, string, error) {
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, []string{"c1", "c2"}, commits)
			return expected, "c1", nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Equal(t, expected, slip)
	assert.Equal(t, "c1", commit)
}

func TestSlipStoreAdapter_FindByCommits_NotFound(t *testing.T) {
	store := &mockSlipStore{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*slippy.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}

	adapter := NewSlipStoreAdapter(store)
	slip, commit, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"c1"})
	assert.ErrorIs(t, err, slippy.ErrSlipNotFound)
	assert.Nil(t, slip)
	assert.Empty(t, commit)
}

func TestSlipStoreAdapter_FindAllByCommits_Success(t *testing.T) {
	expected := []domain.SlipWithCommit{
		{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
		{Slip: &domain.Slip{CorrelationID: "b"}, MatchedCommit: "c2"},
	}
	store := &mockSlipStore{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]slippy.SlipWithCommit, error) {
			return expected, nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, "a", results[0].Slip.CorrelationID)
}

func TestSlipStoreAdapter_FindAllByCommits_EmptyResult(t *testing.T) {
	store := &mockSlipStore{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]slippy.SlipWithCommit, error) {
			return nil, nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestSlipStoreAdapter_FindAllByCommits_StoreError(t *testing.T) {
	store := &mockSlipStore{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]slippy.SlipWithCommit, error) {
			return nil, errors.New("connection refused")
		},
	}

	adapter := NewSlipStoreAdapter(store)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	assert.Nil(t, results)
}

func TestSlipStoreAdapter_Close_Success(t *testing.T) {
	closed := false
	store := &mockSlipStore{
		closeFn: func() error {
			closed = true
			return nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	err := adapter.Close()
	require.NoError(t, err)
	assert.True(t, closed)
}

func TestSlipStoreAdapter_Close_Error(t *testing.T) {
	store := &mockSlipStore{
		closeFn: func() error {
			return errors.New("close failed")
		},
	}

	adapter := NewSlipStoreAdapter(store)
	err := adapter.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "close failed")
}

func TestSlipStoreAdapter_Load_ContextCancelled(t *testing.T) {
	store := &mockSlipStore{
		loadFn: func(ctx context.Context, id string) (*slippy.Slip, error) {
			return nil, ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	adapter := NewSlipStoreAdapter(store)
	slip, err := adapter.Load(ctx, "abc-123")
	assert.Error(t, err)
	assert.Nil(t, slip)
}
