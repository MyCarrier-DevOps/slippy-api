package infrastructure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

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
	// nil locker + nil reader → preserves the original lock-free behavior, exercising
	// the regression path that must keep passing when the cache is disabled.
	return NewSlipWriterAdapter(client, nil, nil)
}

// --- Compile-time check ---

func TestSlipWriterAdapter_ImplementsInterface(t *testing.T) {
	var _ domain.SlipWriter = (*SlipWriterAdapter)(nil)
}

// --- CreateSlipForPush ---

func TestSlipWriterAdapter_CreateSlipForPush_Success(t *testing.T) {
	store := &mockSlipStore{
		// CreateSlipForPush retry detection in goLibMyCarrier slippy v1.4.0+ uses
		// LoadLiveByCommit (exact-SHA, terminal-status-filtered). The mock's
		// default LoadLiveByCommit returns ErrSlipNotFound, which triggers the
		// fresh-create path below. No explicit loadLiveByCommitFn needed.
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
		// goLibMyCarrier slippy.CreateSlipForPush retry-detection migrated to
		// LoadLiveByCommit in v1.4.0-feature-82464-add-loadlivebycommit.2 — the
		// retry-detection path is exact-SHA-by-intent and excludes superseded
		// terminal statuses at the DB layer. This mock returns the existing slip
		// from the live-by-commit lookup.
		loadLiveByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
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

// TestSlipWriterAdapter_CompleteStep_SurvivesRequestCancellation verifies that
// a request-context cancellation does NOT abort the ClickHouse write. The
// writer derives a context.WithoutCancel-based write ctx so the durable
// `slip_component_states` row lands even if the HTTP client disconnects or an
// LB resets the upstream connection mid-flight.
func TestSlipWriterAdapter_CompleteStep_SurvivesRequestCancellation(t *testing.T) {
	var seenCtxErr error
	var called bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(ctx context.Context, _, _, _ string, status slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			called = true
			seenCtxErr = ctx.Err()
			assert.Equal(t, slippy.StepStatusCompleted, status)
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
		},
	}
	adapter := newTestWriterAdapter(store)

	reqCtx, cancel := context.WithCancel(context.Background())
	cancel() // simulate client/LB cancellation BEFORE the write starts

	err := adapter.CompleteStep(reqCtx, "abc-123", "builds_completed", "api")
	require.NoError(t, err)
	assert.True(t, called, "write must be attempted despite cancelled request ctx")
	assert.NoError(t, seenCtxErr, "store must receive a live, non-cancelled ctx")
}

// TestSlipWriterAdapter_StartStep_SurvivesRequestCancellation mirrors the above
// for StartStep — the most common rerun-after-failure path.
func TestSlipWriterAdapter_StartStep_SurvivesRequestCancellation(t *testing.T) {
	var seenCtxErr error
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(ctx context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			seenCtxErr = ctx.Err()
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	reqCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := adapter.StartStep(reqCtx, "abc-123", "builds_completed", "api")
	require.NoError(t, err)
	assert.NoError(t, seenCtxErr, "store must receive a live, non-cancelled ctx")
}

// TestSlipWriterAdapter_FailStep_SurvivesRequestCancellation mirrors the above
// for FailStep.
func TestSlipWriterAdapter_FailStep_SurvivesRequestCancellation(t *testing.T) {
	var seenCtxErr error
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(ctx context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			seenCtxErr = ctx.Err()
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
		},
	}
	adapter := newTestWriterAdapter(store)

	reqCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := adapter.FailStep(reqCtx, "abc-123", "builds_completed", "api", "reason")
	require.NoError(t, err)
	assert.NoError(t, seenCtxErr, "store must receive a live, non-cancelled ctx")
}

// withTestWriteOpTimeout temporarily shortens writeOpTimeout so tests don't
// have to wait the full 15s to assert it bounds a slow op.
func withTestWriteOpTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := writeOpTimeout
	writeOpTimeout = d
	t.Cleanup(func() { writeOpTimeout = orig })
}

// TestSlipWriterAdapter_CompleteStep_WriteOpTimeoutBoundsSlowOp asserts the
// 15s (here shortened) timeout in writeContext actually cuts a slow op off.
// Without this guard a hung ClickHouse driver could block a request handler
// indefinitely; this is the safety net behind the WithoutCancel decoupling.
func TestSlipWriterAdapter_CompleteStep_WriteOpTimeoutBoundsSlowOp(t *testing.T) {
	withTestWriteOpTimeout(t, 50*time.Millisecond)

	var seenCtxErr error
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(ctx context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			// Block longer than the timeout, then record what the ctx saw.
			select {
			case <-ctx.Done():
				seenCtxErr = ctx.Err()
				return ctx.Err()
			case <-time.After(2 * time.Second):
				seenCtxErr = nil
				return nil
			}
		},
	}
	adapter := newTestWriterAdapter(store)

	start := time.Now()
	err := adapter.CompleteStep(context.Background(), "abc-123", "builds_completed", "api")
	elapsed := time.Since(start)

	require.Error(t, err, "the upstream call must surface the deadline")
	assert.ErrorIs(t, seenCtxErr, context.DeadlineExceeded,
		"writeContext must enforce writeOpTimeout regardless of the request ctx")
	assert.Less(t, elapsed, 1*time.Second,
		"the bound must fire well before the store's natural completion")
}

// TestWriteContext_PreservesSpanContext asserts the contract documented on
// writeContext: span context survives context.WithoutCancel, so writes still
// attribute to the request's trace. Without this, traces would fragment at
// every adapter method boundary.
func TestWriteContext_PreservesSpanContext(t *testing.T) {
	// Install an SDK tracer provider so the span is real (default global is a noop).
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	defer span.End()

	wctx, cancel := writeContext(ctx)
	defer cancel()

	parentSC := span.SpanContext()
	wctxSC := trace.SpanFromContext(wctx).SpanContext()

	assert.True(t, wctxSC.IsValid(), "wctx must carry a valid span context")
	assert.Equal(t, parentSC.TraceID(), wctxSC.TraceID(),
		"trace must be preserved through context.WithoutCancel")
	assert.Equal(t, parentSC.SpanID(), wctxSC.SpanID(),
		"current span must be preserved through context.WithoutCancel")
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
	adapter := NewSlipWriterAdapter(client, nil, nil)

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

// TestWriteOpTimeout_DefaultIs240s verifies that the package-level default
// write timeout is 240 s (not the former 15 s). This is a regression guard:
// if the constant reverts, this test catches it before the timeout can kill
// in-flight ClickHouse writes in production.
func TestWriteOpTimeout_DefaultIs240s(t *testing.T) {
	// writeOpTimeout is set at package init from initWriteOpTimeout(). In the
	// test environment SLIPPY_WRITE_OP_TIMEOUT is unset, so it must equal the
	// compile-time default.
	assert.Equal(t, defaultWriteOpTimeout, 240*time.Second,
		"defaultWriteOpTimeout constant must be 240s")
	// The live var should also match the default when the env is absent.
	// (Tests that shorten it via withTestWriteOpTimeout restore it in t.Cleanup.)
	assert.GreaterOrEqual(t, writeOpTimeout, 240*time.Second,
		"writeOpTimeout must be at least 240s in a clean test environment")
}

// TestInitWriteOpTimeout_ZeroFallsBackToDefault verifies that
// SLIPPY_WRITE_OP_TIMEOUT=0 is rejected by the floor check and falls back to
// defaultWriteOpTimeout. A zero timeout would make context.WithTimeout expire
// instantly, causing every write to fail before the query is even sent.
func TestInitWriteOpTimeout_ZeroFallsBackToDefault(t *testing.T) {
	t.Setenv("SLIPPY_WRITE_OP_TIMEOUT", "0")
	got := initWriteOpTimeout()
	assert.Equal(t, defaultWriteOpTimeout, got,
		"SLIPPY_WRITE_OP_TIMEOUT=0 must fall back to default (floor is 1s)")
}

// TestInitWriteOpTimeout_NegativeFallsBackToDefault verifies that a negative
// value (e.g. -5) is rejected and falls back to defaultWriteOpTimeout.
func TestInitWriteOpTimeout_NegativeFallsBackToDefault(t *testing.T) {
	t.Setenv("SLIPPY_WRITE_OP_TIMEOUT", "-5")
	got := initWriteOpTimeout()
	assert.Equal(t, defaultWriteOpTimeout, got,
		"SLIPPY_WRITE_OP_TIMEOUT=-5 must fall back to default (floor is 1s)")
}

// TestInitWriteOpTimeout_ValidValueIsAccepted verifies that a valid value
// within [minWriteOpTimeout, maxWriteOpTimeout] is accepted as-is.
func TestInitWriteOpTimeout_ValidValueIsAccepted(t *testing.T) {
	t.Setenv("SLIPPY_WRITE_OP_TIMEOUT", "30s")
	got := initWriteOpTimeout()
	assert.Equal(t, 30*time.Second, got,
		"SLIPPY_WRITE_OP_TIMEOUT=30s must be accepted")
}

// TestInitWriteOpTimeout_AbsurdUpperBoundFallsBackToDefault verifies that an
// absurdly large value (e.g. 700s, above the 600s ceiling) falls back to the
// default rather than tying up a handler indefinitely.
func TestInitWriteOpTimeout_AbsurdUpperBoundFallsBackToDefault(t *testing.T) {
	t.Setenv("SLIPPY_WRITE_OP_TIMEOUT", "700s")
	got := initWriteOpTimeout()
	assert.Equal(t, defaultWriteOpTimeout, got,
		"SLIPPY_WRITE_OP_TIMEOUT=700s must fall back to default (ceiling is 600s)")
}

// TestInitWriteOpTimeout_UnsetReturnsDefault verifies that when the env var is
// absent, initWriteOpTimeout returns defaultWriteOpTimeout.
func TestInitWriteOpTimeout_UnsetReturnsDefault(t *testing.T) {
	t.Setenv("SLIPPY_WRITE_OP_TIMEOUT", "")
	got := initWriteOpTimeout()
	assert.Equal(t, defaultWriteOpTimeout, got,
		"unset SLIPPY_WRITE_OP_TIMEOUT must return defaultWriteOpTimeout")
}

// TestSlipWriterAdapter_HydrateAndPersist_TimeoutNonFatal_AfterClientWrite
// verifies that a slow/timing-out hydrateAndPersist (simulated by shortening
// writeOpTimeout and having the Load block longer than the timeout) does NOT
// cause the overall write to return an error. The authoritative client write
// has already succeeded; the cache writeback is best-effort.
//
// This is the key regression guard for the fix: previously the 15 s timeout
// would fire and kill the whole context, which surfaced as an error to the
// handler and caused a 504 (now 202). With a 240 s timeout this fires less
// often in production, but the non-fatal semantics must hold regardless.
func TestSlipWriterAdapter_HydrateAndPersist_TimeoutNonFatal_AfterClientWrite(t *testing.T) {
	withTestWriteOpTimeout(t, 30*time.Millisecond)

	clientWritten := false
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			clientWritten = true
			return nil
		},
		// Load is called by hydrateAndPersist. Block until the write-context
		// deadline fires to simulate a slow CH query during the cache writeback.
		loadFn: func(ctx context.Context, _ string) (*slippy.Slip, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return &slippy.Slip{CorrelationID: "abc-123"}, nil
			}
		},
	}
	adapter := newTestWriterAdapter(store)

	// push_parsed is a plain pipeline step → hydrateAndPersist fires.
	err := adapter.StartStep(context.Background(), "abc-123", "push_parsed", "")
	require.NoError(t, err,
		"a timing-out hydrateAndPersist must NOT propagate as an error after a successful client write")
	assert.True(t, clientWritten,
		"the authoritative client write must have completed before the timeout fires")
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
	adapter := NewSlipWriterAdapter(client, nil, nil)

	// componentName empty and pipelineCfg nil → isPipelineStep returns true →
	// hydrateAndPersist is invoked. Both Load and Update must be called.
	err := adapter.CompleteStep(context.Background(), "abc-123", "anything", "")
	require.NoError(t, err)
}

// TestSlipWriterAdapter_CompleteStep_PipelineStep_DoesNotDoubleCheckCompletion asserts
// that for a pipeline step (componentName == ""), the direct CompleteStep path fires
// checkPipelineCompletion (one Load) and hydrateAndPersist fires one more Load.
// Total: exactly 2 Load calls.
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
	assert.Equal(
		t,
		2,
		loadCalls,
		"expected exactly 2 Load calls: one from checkPipelineCompletion, one from hydrateAndPersist",
	)
}

// TestSlipWriterAdapter_FailStep_PipelineStep_DoesNotDoubleCheckCompletion asserts
// that for a pipeline step (componentName == ""), the direct FailStep path fires
// checkPipelineCompletion (one Load) and hydrateAndPersist fires one more Load.
// Total: exactly 2 Load calls.
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

// TestSlipWriterAdapter_CompleteStep_FromFailed_Recovery verifies that a step
// can transition from a terminal failure status back to completed when re-run
// succeeds. This is the documented `failed → running → completed` recovery flow
// (STATE_MACHINE_V3.md §Recovery Rules) — no step-level state is immutable.
func TestSlipWriterAdapter_CompleteStep_FromFailed_Recovery(t *testing.T) {
	const id = "abc-123"
	var writeCalled bool
	var writtenStatus slippy.StepStatus

	store := &mockSlipStore{
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{
				CorrelationID: id,
				Status:        slippy.SlipStatusFailed,
				Steps:         map[string]slippy.Step{"prod_deploy": {Status: slippy.StepStatusFailed}},
			}, nil
		},
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, status slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			writeCalled = true
			writtenStatus = status
			return nil
		},
		updateFn: func(_ context.Context, _ *slippy.Slip) error {
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), id, "prod_deploy", "")
	require.NoError(t, err, "failed → completed recovery must be permitted")
	assert.True(t, writeCalled, "underlying write must run for recovery transition")
	assert.Equal(t, slippy.StepStatusCompleted, writtenStatus)
}

// --- Read-your-own-writes overlay regression tests ---
//
// These tests verify that hydrateAndPersist overlays the just-written step status
// into the in-memory slip before calling Update, even when Load returns a stale
// value (simulating ClickHouse async-insert visibility lag). Without the overlay,
// the stale running status would be written back to routing_slips — permanently
// violating I5. Mirror of TestOverlayUpdatesStepsStatus_T1Regression in goLibMyCarrier.

// TestHydrateAndPersist_AsyncInsertRace_CompleteStep verifies that when Load returns
// a stale running status for a pipeline step (simulating async-insert lag), CompleteStep
// overlays completed before Update so routing_slips is not permanently stuck.
func TestHydrateAndPersist_AsyncInsertRace_CompleteStep(t *testing.T) {
	const id = "b058127d-fe0a-497d-81e6-08edc7ea71b2"
	const stepName = "dev_tests"

	// Slip returned by Load has the step still running — simulates async-insert lag
	// where the just-inserted completed event is not yet visible to the SELECT.
	staleSlip := &slippy.Slip{
		CorrelationID: id,
		Status:        slippy.SlipStatusInProgress,
		Steps: map[string]slippy.Step{
			stepName: {Status: slippy.StepStatusRunning},
		},
	}

	var persistedSlip *slippy.Slip
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			// Return a copy to prevent the overlay from modifying the original.
			copy := *staleSlip
			steps := make(map[string]slippy.Step, len(staleSlip.Steps))
			for k, v := range staleSlip.Steps {
				steps[k] = v
			}
			copy.Steps = steps
			return &copy, nil
		},
		updateFn: func(_ context.Context, s *slippy.Slip) error {
			persistedSlip = s
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), id, stepName, "")
	require.NoError(t, err)
	require.NotNil(t, persistedSlip, "Update must be called")

	// Core assertion: despite Load returning running (stale), the persisted step
	// must show completed — the overlay won. This is the I5 invariant.
	require.Contains(t, persistedSlip.Steps, stepName, "step must be present in persisted slip")
	assert.Equal(
		t,
		slippy.StepStatusCompleted,
		persistedSlip.Steps[stepName].Status,
		"overlay must write completed status even when Load returns stale running: I5 regression",
	)
}

// TestHydrateAndPersist_AsyncInsertRace_FailStep verifies the overlay for FailStep
// on a pipeline step when Load returns a stale running status.
func TestHydrateAndPersist_AsyncInsertRace_FailStep(t *testing.T) {
	const id = "abc-fail-overlay"
	const stepName = "dev_tests"

	staleSlip := &slippy.Slip{
		CorrelationID: id,
		Status:        slippy.SlipStatusInProgress,
		Steps: map[string]slippy.Step{
			stepName: {Status: slippy.StepStatusRunning},
		},
	}

	var persistedSlip *slippy.Slip
	var loadCalls int
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			loadCalls++
			copy := *staleSlip
			steps := make(map[string]slippy.Step, len(staleSlip.Steps))
			for k, v := range staleSlip.Steps {
				steps[k] = v
			}
			copy.Steps = steps
			return &copy, nil
		},
		updateFn: func(_ context.Context, s *slippy.Slip) error {
			persistedSlip = s
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), id, stepName, "", "test engine timeout")
	require.NoError(t, err)
	require.NotNil(t, persistedSlip, "Update must be called")
	assert.Equal(
		t,
		slippy.StepStatusFailed,
		persistedSlip.Steps[stepName].Status,
		"overlay must write failed status even when Load returns stale running: I5 regression",
	)
}

// TestHydrateAndPersist_OverlaySkipsNewerStatus verifies the guard condition: if the
// slip returned by Load already has a newer CompletedAt (e.g. from a concurrent
// terminal event), the overlay must leave the newer status untouched.
func TestHydrateAndPersist_OverlaySkipsNewerStatus(t *testing.T) {
	const id = "abc-newer-guard"
	const stepName = "dev_tests"

	futureTime := time.Now().Add(time.Hour) // Load returns a step completed in the future

	slipWithNewerStatus := &slippy.Slip{
		CorrelationID: id,
		Status:        slippy.SlipStatusInProgress,
		Steps: map[string]slippy.Step{
			stepName: {Status: slippy.StepStatusCompleted, CompletedAt: &futureTime},
		},
	}

	var persistedSlip *slippy.Slip
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			copy := *slipWithNewerStatus
			steps := make(map[string]slippy.Step, len(slipWithNewerStatus.Steps))
			for k, v := range slipWithNewerStatus.Steps {
				steps[k] = v
			}
			copy.Steps = steps
			return &copy, nil
		},
		updateFn: func(_ context.Context, s *slippy.Slip) error {
			persistedSlip = s
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	// CompleteStep for dev_tests — writtenAt will be time.Now() which is < futureTime.
	err := adapter.CompleteStep(context.Background(), id, stepName, "")
	require.NoError(t, err)
	require.NotNil(t, persistedSlip, "Update must be called")

	// The overlay must NOT overwrite the newer CompletedAt from Load.
	assert.Equal(
		t,
		&futureTime,
		persistedSlip.Steps[stepName].CompletedAt,
		"overlay must preserve newer CompletedAt from Load (defensive guard)",
	)
}

// TestOverlayPipelineStep_NilSlip verifies that overlayPipelineStep is a no-op when
// called with a nil slip (defensive guard, mirrors overlayComponentState nil check).
func TestOverlayPipelineStep_NilSlip(t *testing.T) {
	// Must not panic.
	overlayPipelineStep(nil, "dev_tests", slippy.StepStatusCompleted, time.Now())
}

// TestOverlayPipelineStep_MissingStep verifies that overlayPipelineStep is a no-op
// when the stepName is not present in slip.Steps.
func TestOverlayPipelineStep_MissingStep(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{},
	}
	overlayPipelineStep(slip, "nonexistent_step", slippy.StepStatusCompleted, time.Now())
	// No panic, no entry added.
	assert.Empty(t, slip.Steps)
}

// TestOverlayPipelineStep_NilCompletedAt verifies that an overlay always wins when
// the existing step has no CompletedAt (i.e. it is still running / pending).
func TestOverlayPipelineStep_NilCompletedAt(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			"dev_tests": {Status: slippy.StepStatusRunning, CompletedAt: nil},
		},
	}
	now := time.Now()
	overlayPipelineStep(slip, "dev_tests", slippy.StepStatusCompleted, now)
	assert.Equal(t, slippy.StepStatusCompleted, slip.Steps["dev_tests"].Status)
	require.NotNil(t, slip.Steps["dev_tests"].CompletedAt)
}

// TestOverlayPipelineStep_OlderCompletedAt verifies that an overlay wins when
// writtenAt is strictly after the existing CompletedAt.
func TestOverlayPipelineStep_OlderCompletedAt(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			"dev_tests": {Status: slippy.StepStatusFailed, CompletedAt: &past},
		},
	}
	now := time.Now()
	overlayPipelineStep(slip, "dev_tests", slippy.StepStatusCompleted, now)
	assert.Equal(t, slippy.StepStatusCompleted, slip.Steps["dev_tests"].Status)
}

// TestOverlayPipelineStep_RunningDoesNotClobberTerminal is the F1 unit regression test.
// Verifies that a non-terminal (running) overlay is silently dropped when the step
// already has a terminal status with a past CompletedAt. This is the exact scenario
// from slip b058127d where a second StartStep at 17:01 arrived after CompleteStep at
// 16:58, and the overlay clobbered "completed" with "running".
func TestOverlayPipelineStep_RunningDoesNotClobberTerminal(t *testing.T) {
	past := time.Now().Add(-3 * time.Minute) // CompleteStep fired 3 min ago
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			"dev_tests": {Status: slippy.StepStatusCompleted, CompletedAt: &past},
		},
	}
	// writtenAt is time.Now() — after past, so the old timestamp guard would have let this through.
	overlayPipelineStep(slip, "dev_tests", slippy.StepStatusRunning, time.Now())
	assert.Equal(
		t,
		slippy.StepStatusCompleted,
		slip.Steps["dev_tests"].Status,
		"running must not clobber completed: F1 guard regression",
	)
	assert.Equal(
		t,
		&past,
		slip.Steps["dev_tests"].CompletedAt,
		"CompletedAt must be preserved when overlay is rejected",
	)
}

// TestHydrateAndPersist_StartStep_DoesNotClobberTerminalStatus is the F1 end-to-end
// regression test via the real hydrateAndPersist path. Reproduces the production
// scenario from slip b058127d where a second StartStep arrived 3 minutes after
// CompleteStep, and the overlay clobbered "completed" with "running".
func TestHydrateAndPersist_StartStep_DoesNotClobberTerminalStatus(t *testing.T) {
	const id = "b058127d-fe0a-497d-81e6-08edc7ea71b2"
	const stepName = "push_parsed"

	// Load returns a slip whose step is already terminal (completed 3 min ago).
	// This simulates the scenario where CompleteStep already flushed and the
	// second StartStep fires (out-of-order re-trigger).
	completedAt := time.Now().Add(-3 * time.Minute)
	terminalSlip := &slippy.Slip{
		CorrelationID: id,
		Status:        slippy.SlipStatusInProgress,
		Steps: map[string]slippy.Step{
			stepName: {Status: slippy.StepStatusCompleted, CompletedAt: &completedAt},
		},
	}

	var persistedSlip *slippy.Slip
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			// Return a copy so the overlay operates on a fresh value each call.
			cp := *terminalSlip
			steps := make(map[string]slippy.Step, len(terminalSlip.Steps))
			for k, v := range terminalSlip.Steps {
				steps[k] = v
			}
			cp.Steps = steps
			return &cp, nil
		},
		updateFn: func(_ context.Context, s *slippy.Slip) error {
			persistedSlip = s
			return nil
		},
	}
	adapter := newTestWriterAdapter(store)

	// StartStep on a pipeline step (componentName == "") triggers hydrateAndPersist
	// with StepStatusRunning. The F1 guard must prevent the running overlay from
	// clobbering the terminal completed status already visible in Load.
	err := adapter.StartStep(context.Background(), id, stepName, "")
	require.NoError(t, err)
	require.NotNil(t, persistedSlip, "Update must still be called (hydration runs)")

	assert.Equal(
		t,
		slippy.StepStatusCompleted,
		persistedSlip.Steps[stepName].Status,
		"StartStep must not clobber terminal completed status: F1 regression (slip b058127d)",
	)
}
