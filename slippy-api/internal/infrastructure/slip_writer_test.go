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
		// RunPostExecution calls checkPipelineCompletion which calls Load.
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
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
		// RunPostExecution calls checkPipelineCompletion which calls Load.
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
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
	// In v1.3.77+, checkPipelineCompletion always calls Load to evaluate pipeline
	// terminal state before deciding whether to write. The adapter's hydrateAndPersist
	// (which would call Load then Update) must still be skipped for aggregate steps.
	// We verify the real invariant: Update is never called by the adapter layer.
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		// Load is called by checkPipelineCompletion inside the library (non-aggregate
		// terminal step path). Return an in-progress slip to exercise the non-terminal
		// short-circuit path.
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{
				CorrelationID: "abc-123",
				Status:        slippy.SlipStatusInProgress,
			}, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			t.Fatal("Update should not be called: adapter must not double-hydrate aggregate steps")
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	// builds_completed is an aggregate step; the adapter's hydrateAndPersist path
	// (Load + Update) must be skipped. Load may be called by the library's
	// checkPipelineCompletion, but Update must not be called by the adapter.
	err := adapter.CompleteStep(context.Background(), "abc-123", "builds_completed", "")
	require.NoError(t, err)
}

func TestSlipWriterAdapter_HydrationError_NonFatal(t *testing.T) {
	// For a pipeline step (empty componentName), the direct CompleteStep path calls
	// checkPipelineCompletion once (from UpdateStepWithStatus), and hydrateAndPersist
	// calls Load a second time. Only the second (hydrateAndPersist) may fail non-fatally.
	var loadCalls int
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			loadCalls++
			if loadCalls <= 1 {
				return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
			}
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
	// For a pipeline step (empty componentName), the direct FailStep path calls
	// checkPipelineCompletion once (from UpdateStepWithStatus), and hydrateAndPersist
	// calls Load a second time. Only the second (hydrateAndPersist) may fail non-fatally.
	var loadCalls int
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			loadCalls++
			if loadCalls <= 1 {
				return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
			}
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

// TestSlipWriterAdapter_CompleteStep_PipelineStep_DoesNotDoubleCheckCompletion asserts
// that for a pipeline step (componentName == ""), the direct CompleteStep path fires
// checkPipelineCompletion exactly once (inside UpdateStepWithStatus), then
// hydrateAndPersist fires Load a second time. Total: exactly 2 Load calls.
func TestSlipWriterAdapter_CompleteStep_PipelineStep_DoesNotDoubleCheckCompletion(t *testing.T) {
	var loadCalls int
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			loadCalls++
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), "abc-123", "push_parsed", "")
	require.NoError(t, err)
	// checkPipelineCompletion (1 Load) + hydrateAndPersist (1 Load) = 2 total.
	// The old RunPostExecution path caused 3 Loads (double checkPipelineCompletion + hydrate).
	assert.Equal(
		t,
		2,
		loadCalls,
		"expected exactly 2 Load calls: one from checkPipelineCompletion, one from hydrateAndPersist",
	)
}

// TestSlipWriterAdapter_FailStep_PipelineStep_DoesNotDoubleCheckCompletion asserts
// that for a pipeline step (componentName == ""), the direct FailStep path fires
// checkPipelineCompletion exactly once (inside UpdateStepWithStatus), then
// hydrateAndPersist fires Load a second time. Total: exactly 2 Load calls.
func TestSlipWriterAdapter_FailStep_PipelineStep_DoesNotDoubleCheckCompletion(t *testing.T) {
	var loadCalls int
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			loadCalls++
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), "abc-123", "push_parsed", "", "test failure")
	require.NoError(t, err)
	// checkPipelineCompletion (1 Load) + hydrateAndPersist (1 Load) = 2 total.
	// The old RunPostExecution path caused 3 Loads (double checkPipelineCompletion + hydrate).
	assert.Equal(
		t,
		2,
		loadCalls,
		"expected exactly 2 Load calls: one from checkPipelineCompletion, one from hydrateAndPersist",
	)
}

// --- C2: updateSlipStatusFn migration tests ---

// TestSlipWriterAdapter_FailStep_ComponentStep_UpdatesSlipStatus verifies that
// completing a component step with a failure captures the slip status transition
// via updateSlipStatusFn (atomic INSERT SELECT), not via a full store.Update round-trip.
func TestSlipWriterAdapter_FailStep_ComponentStep_UpdatesSlipStatus(t *testing.T) {
	const id = "abc-123"

	var capturedID string
	var capturedStatus slippy.SlipStatus
	var updateCalled bool

	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		// One failed component step → checkPipelineCompletion reaches primary-failure branch.
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{
				CorrelationID: id,
				Status:        slippy.SlipStatusInProgress,
				Steps: map[string]slippy.Step{
					"build": {Status: slippy.StepStatusFailed},
				},
			}, nil
		},
		updateSlipStatusFn: func(_ context.Context, rid string, status slippy.SlipStatus) error {
			capturedID = rid
			capturedStatus = status
			return nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			updateCalled = true
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), id, "builds_completed", "api", "build timeout")
	require.NoError(t, err)

	assert.Equal(t, id, capturedID, "updateSlipStatus must be called with the correlationID")
	assert.Equal(t, slippy.SlipStatusFailed, capturedStatus, "slip status must be Failed when a component step fails")
	assert.False(t, updateCalled, "store.Update must not be called for component-step writes")
}

// TestSlipWriterAdapter_CompleteStep_ComponentStep_CallsCheckPipelineCompletion verifies
// that completing a component step calls Load (via checkPipelineCompletion) but does NOT
// call store.Update — only updateSlipStatusFn if a transition is needed.
// For an in-progress slip with no terminal aggregate step, no status transition occurs.
func TestSlipWriterAdapter_CompleteStep_ComponentStep_CallsCheckPipelineCompletion(t *testing.T) {
	const id = "abc-123"
	var loadCalled bool
	var updateCalled bool
	var updateSlipStatusCalled bool

	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		// In-progress slip with no terminal aggregate — completion check short-circuits.
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			loadCalled = true
			return &slippy.Slip{
				CorrelationID: id,
				Status:        slippy.SlipStatusInProgress,
				Steps:         map[string]slippy.Step{},
			}, nil
		},
		updateSlipStatusFn: func(_ context.Context, _ string, _ slippy.SlipStatus) error {
			updateSlipStatusCalled = true
			return nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			updateCalled = true
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), id, "builds_completed", "api")
	require.NoError(t, err)

	assert.True(t, loadCalled, "Load must be called by checkPipelineCompletion")
	assert.False(t, updateSlipStatusCalled, "updateSlipStatusFn must NOT be called when slip stays InProgress")
	assert.False(t, updateCalled, "store.Update must not be called for component-step writes")
}

// TestSlipWriterAdapter_ComponentStep_DoesNotCallStoreUpdate is a guard test asserting
// that any component-step path (CompleteStep or FailStep with componentName != "") never
// invokes store.Update. All status writes must go through updateSlipStatusFn.
func TestSlipWriterAdapter_ComponentStep_DoesNotCallStoreUpdate(t *testing.T) {
	const id = "abc-123"

	makeStore := func() (*mockSlipStore, *bool) {
		updateCalled := false
		return &mockSlipStore{
			updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
				return nil
			},
			loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
				return &slippy.Slip{CorrelationID: id, Status: slippy.SlipStatusInProgress}, nil
			},
			updateFn: func(_ context.Context, _ *slippy.Slip) error {
				updateCalled = true
				return nil
			},
		}, &updateCalled
	}

	t.Run("CompleteStep", func(t *testing.T) {
		store, updateCalled := makeStore()
		adapter := newTestWriterAdapter(store)
		err := adapter.CompleteStep(context.Background(), id, "builds_completed", "api")
		require.NoError(t, err)
		assert.False(t, *updateCalled, "store.Update must not be called for component CompleteStep")
	})

	t.Run("FailStep", func(t *testing.T) {
		store, updateCalled := makeStore()
		adapter := newTestWriterAdapter(store)
		err := adapter.FailStep(context.Background(), id, "builds_completed", "api", "reason")
		require.NoError(t, err)
		assert.False(t, *updateCalled, "store.Update must not be called for component FailStep")
	})
}
