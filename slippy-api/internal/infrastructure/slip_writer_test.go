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

// mockGitHubAPI implements slippy.GitHubAPI for testing.
type mockGitHubAPI struct {
	getCommitAncestryFn func(ctx context.Context, owner, repo, ref string, depth int) ([]string, error)
}

func (m *mockGitHubAPI) GetCommitAncestry(ctx context.Context, owner, repo, ref string, depth int) ([]string, error) {
	if m.getCommitAncestryFn != nil {
		return m.getCommitAncestryFn(ctx, owner, repo, ref, depth)
	}
	return nil, nil
}

func (m *mockGitHubAPI) GetPRHeadCommit(_ context.Context, _, _ string, _ int) (string, error) {
	return "", nil
}

func (m *mockGitHubAPI) ClearCache() {}

// newTestWriterAdapter builds a SlipWriterAdapter backed by a mockSlipStore.
// The slippy.Client is constructed with NewClientWithDependencies so we can
// inject the mock store. A minimal PipelineConfig with a "builds_completed"
// step (aggregating "build") is included so SetComponentImageTag works.
// testPipelineConfigJSON is a minimal pipeline config that ParsePipelineConfig
// can initialize with proper internal indexes (aggregateMap, stepsByName).
const testPipelineConfigJSON = `{
	"name": "test",
	"steps": [
		{"name": "push_parsed"},
		{"name": "builds_completed", "aggregates": "build"}
	]
}`

func newTestWriterAdapter(store slippy.SlipStore) *SlipWriterAdapter {
	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(testPipelineConfigJSON))
	if err != nil {
		panic("failed to parse test pipeline config: " + err.Error())
	}
	cfg := slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	}
	client := slippy.NewClientWithDependencies(store, &mockGitHubAPI{}, cfg)
	return NewSlipWriterAdapter(client)
}

// --- Compile-time check ---

func TestSlipWriterAdapter_ImplementsInterface(t *testing.T) {
	var _ domain.SlipWriter = (*SlipWriterAdapter)(nil)
}

// --- CreateSlipForPush ---

func TestSlipWriterAdapter_CreateSlipForPush_Success(t *testing.T) {
	store := &mockSlipStore{
		// LoadByCommit is called by CreateSlipForPush for retry detection.
		// Returning not-found causes a fresh create.
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
		createFn: func(_ context.Context, _ *slippy.Slip) error {
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	result, err := adapter.CreateSlipForPush(context.Background(), domain.PushOptions{
		CorrelationID: "abc-123",
		Repository:    "org/repo",
		Branch:        "main",
		CommitSHA:     "deadbeef1234567890",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "abc-123", result.Slip.CorrelationID)
	assert.Equal(t, "org/repo", result.Slip.Repository)
}

func TestSlipWriterAdapter_CreateSlipForPush_ValidationError(t *testing.T) {
	store := &mockSlipStore{}
	adapter := newTestWriterAdapter(store)

	// Empty CorrelationID should fail validation.
	result, err := adapter.CreateSlipForPush(context.Background(), domain.PushOptions{
		Repository: "org/repo",
		CommitSHA:  "deadbeef",
	})
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "correlation_id")
}

func TestSlipWriterAdapter_CreateSlipForPush_StoreError(t *testing.T) {
	storeErr := errors.New("clickhouse connection refused")
	store := &mockSlipStore{
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
		createFn: func(_ context.Context, _ *slippy.Slip) error {
			return storeErr
		},
	}
	adapter := newTestWriterAdapter(store)

	result, err := adapter.CreateSlipForPush(context.Background(), domain.PushOptions{
		CorrelationID: "abc-123",
		Repository:    "org/repo",
		CommitSHA:     "deadbeef",
	})
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestSlipWriterAdapter_CreateSlipForPush_RetryDetection(t *testing.T) {
	// When a slip already exists for the commit, CreateSlipForPush handles retry.
	existingSlip := &slippy.Slip{
		CorrelationID: "existing-123",
		Repository:    "org/repo",
		CommitSHA:     "deadbeef1234567890",
	}
	store := &mockSlipStore{
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return existingSlip, nil
		},
		updateStepFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus) error {
			return nil
		},
		appendHistoryFn: func(_ context.Context, _ string, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			return existingSlip, nil
		},
	}
	adapter := newTestWriterAdapter(store)

	result, err := adapter.CreateSlipForPush(context.Background(), domain.PushOptions{
		CorrelationID: "abc-123",
		Repository:    "org/repo",
		CommitSHA:     "deadbeef1234567890",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "existing-123", result.Slip.CorrelationID)
}

// --- StartStep ---

func TestSlipWriterAdapter_StartStep_Success(t *testing.T) {
	var called bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, id, step, comp string, status slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			called = true
			assert.Equal(t, "abc-123", id)
			assert.Equal(t, "builds_completed", step)
			assert.Equal(t, "api", comp)
			assert.Equal(t, slippy.StepStatusRunning, status)
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.StartStep(context.Background(), "abc-123", "builds_completed", "api")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestSlipWriterAdapter_StartStep_Error(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return slippy.ErrSlipNotFound
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.StartStep(context.Background(), "abc-123", "builds_completed", "api")
	assert.Error(t, err)
}

// --- CompleteStep ---

func TestSlipWriterAdapter_CompleteStep_Success(t *testing.T) {
	var called bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, status slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			called = true
			assert.Equal(t, slippy.StepStatusCompleted, status)
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), "abc-123", "builds_completed", "api")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestSlipWriterAdapter_CompleteStep_Error(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return errors.New("database error")
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), "abc-123", "builds_completed", "api")
	assert.Error(t, err)
}

// --- FailStep ---

func TestSlipWriterAdapter_FailStep_Success(t *testing.T) {
	var called bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, status slippy.StepStatus, entry slippy.StateHistoryEntry) error {
			called = true
			assert.Equal(t, slippy.StepStatusFailed, status)
			assert.Equal(t, "build timeout", entry.Message)
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), "abc-123", "builds_completed", "api", "build timeout")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestSlipWriterAdapter_FailStep_NotFound(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return slippy.ErrSlipNotFound
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), "abc-123", "builds_completed", "api", "reason")
	assert.Error(t, err)
}

// --- SkipStep ---

func TestSlipWriterAdapter_SkipStep_Success(t *testing.T) {
	var called bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, status slippy.StepStatus, entry slippy.StateHistoryEntry) error {
			called = true
			assert.Equal(t, slippy.StepStatusSkipped, status)
			assert.Equal(t, "alert-gate passed", entry.Message)
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.SkipStep(context.Background(), "abc-123", "builds_completed", "api", "alert-gate passed")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestSlipWriterAdapter_SkipStep_NotFound(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return slippy.ErrSlipNotFound
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.SkipStep(context.Background(), "abc-123", "builds_completed", "api", "reason")
	assert.Error(t, err)
}

func TestSlipWriterAdapter_SkipStep_PipelineStep_TriggersHydration(t *testing.T) {
	hydrationSlip := &slippy.Slip{CorrelationID: "abc-123"}
	var updateCalled bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return hydrationSlip, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			updateCalled = true
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.SkipStep(context.Background(), "abc-123", "push_parsed", "", "not needed")
	require.NoError(t, err)
	assert.True(t, updateCalled, "expected hydrateAndPersist to call Update for pipeline step")
}

func TestSlipWriterAdapter_SkipStep_HydrationError_NonFatal(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return nil, errors.New("clickhouse unavailable")
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.SkipStep(context.Background(), "abc-123", "push_parsed", "", "skip")
	require.NoError(t, err)
}

// --- SetComponentImageTag ---

func TestSlipWriterAdapter_SetComponentImageTag_Success(t *testing.T) {
	var called bool
	store := &mockSlipStore{
		setComponentImageTagFn: func(_ context.Context, id, step, comp, tag string) error {
			called = true
			assert.Equal(t, "abc-123", id)
			assert.Equal(t, "build", step)
			assert.Equal(t, "api", comp)
			assert.Equal(t, "26.09.abc1234", tag)
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.SetComponentImageTag(context.Background(), "abc-123", "api", "26.09.abc1234")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestSlipWriterAdapter_SetComponentImageTag_Error(t *testing.T) {
	store := &mockSlipStore{
		setComponentImageTagFn: func(_ context.Context, _, _, _, _ string) error {
			return errors.New("database error")
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.SetComponentImageTag(context.Background(), "abc-123", "api", "26.09.abc1234")
	assert.Error(t, err)
}

func TestSlipWriterAdapter_SetComponentImageTag_NoPipelineConfig(t *testing.T) {
	store := &mockSlipStore{}
	// Create client without pipeline config.
	client := slippy.NewClientWithDependencies(store, nil, slippy.Config{})
	adapter := NewSlipWriterAdapter(client)

	err := adapter.SetComponentImageTag(context.Background(), "abc-123", "api", "26.09.abc1234")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no pipeline config")
}

// --- Hydration (hydrateAndPersist) ---
//
// "push_parsed" is a plain pipeline step in testPipelineConfigJSON (no aggregates field).
// "builds_completed" is an aggregate step (aggregates "build").

func TestSlipWriterAdapter_StartStep_PipelineStep_TriggersHydration(t *testing.T) {
	hydrationSlip := &slippy.Slip{CorrelationID: "abc-123"}
	var updateCalled bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			assert.Equal(t, "abc-123", id)
			return hydrationSlip, nil
		},
		updateFn: func(_ context.Context, slip *slippy.Slip) error {
			updateCalled = true
			assert.Equal(t, hydrationSlip, slip)
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.StartStep(context.Background(), "abc-123", "push_parsed", "")
	require.NoError(t, err)
	assert.True(t, updateCalled, "expected hydrateAndPersist to call Update for pipeline step")
}

func TestSlipWriterAdapter_CompleteStep_PipelineStep_TriggersHydration(t *testing.T) {
	hydrationSlip := &slippy.Slip{CorrelationID: "abc-123"}
	var updateCalled bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return hydrationSlip, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			updateCalled = true
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), "abc-123", "push_parsed", "")
	require.NoError(t, err)
	assert.True(t, updateCalled, "expected hydrateAndPersist to call Update for pipeline step")
}

func TestSlipWriterAdapter_FailStep_PipelineStep_TriggersHydration(t *testing.T) {
	hydrationSlip := &slippy.Slip{CorrelationID: "abc-123"}
	var updateCalled bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return hydrationSlip, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			updateCalled = true
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), "abc-123", "push_parsed", "", "test failure")
	require.NoError(t, err)
	assert.True(t, updateCalled, "expected hydrateAndPersist to call Update for pipeline step")
}

func TestSlipWriterAdapter_StartStep_ComponentStep_SkipsHydration(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			t.Fatal("Load should not be called for component steps")
			return nil, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			t.Fatal("Update should not be called for component steps")
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	// componentName != "" → hydration must be skipped.
	err := adapter.StartStep(context.Background(), "abc-123", "builds_completed", "api")
	require.NoError(t, err)
}

func TestSlipWriterAdapter_CompleteStep_AggregateStep_SkipsHydration(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			t.Fatal("Load should not be called: aggregate steps hydrate internally")
			return nil, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			t.Fatal("Update should not be called: aggregate steps hydrate internally")
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	// builds_completed is an aggregate step; even with empty componentName, hydration
	// is skipped because the store path already calls Load + Update internally.
	err := adapter.CompleteStep(context.Background(), "abc-123", "builds_completed", "")
	require.NoError(t, err)
}

func TestSlipWriterAdapter_HydrationError_NonFatal(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return nil, errors.New("clickhouse unavailable")
		},
	}
	adapter := newTestWriterAdapter(store)

	// The step write succeeded; hydration failing must not propagate as an error.
	err := adapter.CompleteStep(context.Background(), "abc-123", "push_parsed", "")
	require.NoError(t, err)
}

func TestSlipWriterAdapter_StartStep_HydrationError_NonFatal(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return nil, errors.New("clickhouse unavailable")
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.StartStep(context.Background(), "abc-123", "push_parsed", "")
	require.NoError(t, err)
}

func TestSlipWriterAdapter_FailStep_HydrationError_NonFatal(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return nil, errors.New("clickhouse unavailable")
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), "abc-123", "push_parsed", "", "oops")
	require.NoError(t, err)
}

// TestSlipWriterAdapter_HydrateAndPersist_UpdateError exercises hydrateAndPersist's
// Update error path: Load returns a slip, but Store().Update fails. The write path
// still succeeds because hydration errors are non-fatal.
func TestSlipWriterAdapter_HydrateAndPersist_UpdateError(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123"}, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			return errors.New("update failed")
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), "abc-123", "push_parsed", "")
	require.NoError(t, err, "hydration update errors must not propagate")
}

// TestSlipWriterAdapter_IsPipelineStep_NilPipelineConfig exercises the fallback
// branch where the client has no pipeline config — isPipelineStep returns true,
// so hydration still runs.
func TestSlipWriterAdapter_IsPipelineStep_NilPipelineConfig(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123"}, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			return nil
		},
	}
	// Construct a client with no pipeline config so PipelineConfig() returns nil.
	client := slippy.NewClientWithDependencies(store, &mockGitHubAPI{}, slippy.Config{})
	adapter := NewSlipWriterAdapter(client)

	// componentName empty and pipelineCfg nil → isPipelineStep returns true →
	// hydrateAndPersist is invoked. Both Load and Update must be called.
	err := adapter.CompleteStep(context.Background(), "abc-123", "anything", "")
	require.NoError(t, err)
}
