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
	// i5LockEnabled=false matches the production default (plan v3 §G.1).
	return NewSlipWriterAdapter(client, nil, nil, false)
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
	adapter := NewSlipWriterAdapter(client, nil, nil, false)

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
		updateFn: func(_ context.Context, slip *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
			return nil
		},
	}
	// Construct a client with no pipeline config so PipelineConfig() returns nil.
	client := slippy.NewClientWithDependencies(store, &mockGitHubAPI{}, slippy.Config{})
	adapter := NewSlipWriterAdapter(client, nil, nil, false)

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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
			updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, _ *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, s *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, s *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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
		updateFn: func(_ context.Context, s *slippy.Slip, _ ...slippy.StepStatusOverride) error {
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

// noEventLog is the test-helper callback that simulates "no events yet" — the
// R1 guard falls through to the in-memory CompletedAt check. Used by the
// pre-R1 tests whose behaviour should be unchanged.
func noEventLog() (slippy.StepStatus, bool, error) { return "", false, nil }

// eventLogReturns builds a latestStepStatusFn that always returns the supplied
// (status, true, nil). Used by R1 tests that want to drive the event-log guard
// explicitly.
func eventLogReturns(status slippy.StepStatus) latestStepStatusFn {
	return func() (slippy.StepStatus, bool, error) { return status, true, nil }
}

// TestOverlayPipelineStep_NilSlip verifies that overlayPipelineStep is a no-op when
// called with a nil slip (defensive guard, mirrors overlayComponentState nil check).
func TestOverlayPipelineStep_NilSlip(t *testing.T) {
	// Must not panic. Returns false (no overlay applied).
	applied := overlayPipelineStep(nil, "dev_tests", slippy.StepStatusCompleted, time.Now(), noEventLog)
	assert.False(t, applied)
}

// TestOverlayPipelineStep_MissingStep verifies that overlayPipelineStep is a no-op
// when the stepName is not present in slip.Steps.
func TestOverlayPipelineStep_MissingStep(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{},
	}
	applied := overlayPipelineStep(slip, "nonexistent_step", slippy.StepStatusCompleted, time.Now(), noEventLog)
	assert.False(t, applied)
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
	applied := overlayPipelineStep(slip, "dev_tests", slippy.StepStatusCompleted, now, noEventLog)
	assert.True(t, applied)
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
	applied := overlayPipelineStep(slip, "dev_tests", slippy.StepStatusCompleted, now, noEventLog)
	assert.True(t, applied)
	assert.Equal(t, slippy.StepStatusCompleted, slip.Steps["dev_tests"].Status)
}

// TestOverlayPipelineStep_RunningDoesNotClobberTerminal is the F1 unit regression test,
// extended for R1 (ADO #82468). The R1 guard now consults the event log via the
// latestStepStatusFn callback rather than the in-memory CompletedAt; the original
// in-memory guard is retained as the fail-open fallback.
//
// This test drives the R1 guard explicitly: event log reports completed, caller
// writes running, overlay MUST be dropped (applied == false), Steps map untouched.
func TestOverlayPipelineStep_RunningDoesNotClobberTerminal(t *testing.T) {
	past := time.Now().Add(-3 * time.Minute) // CompleteStep fired 3 min ago
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			"dev_tests": {Status: slippy.StepStatusCompleted, CompletedAt: &past},
		},
	}
	// R1: event log says completed. Caller writes running → overlay dropped.
	applied := overlayPipelineStep(
		slip, "dev_tests", slippy.StepStatusRunning, time.Now(),
		eventLogReturns(slippy.StepStatusCompleted),
	)
	assert.False(t, applied, "applied must be false when R1 drops the overlay")
	assert.Equal(
		t,
		slippy.StepStatusCompleted,
		slip.Steps["dev_tests"].Status,
		"running must not clobber completed: F1 guard regression (now R1-backed)",
	)
	assert.Equal(
		t,
		&past,
		slip.Steps["dev_tests"].CompletedAt,
		"CompletedAt must be preserved when overlay is rejected",
	)
}

// TestOverlayPipelineStep_R1_EventLogTerminalBlocksNonTerminal verifies the R1
// path even when the in-memory CompletedAt is nil (the 436cc68c failure mode:
// Load returned a stale snapshot whose CompletedAt did not yet reflect the
// just-written terminal event). The R1 guard alone must drop the overlay.
func TestOverlayPipelineStep_R1_EventLogTerminalBlocksNonTerminal(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			// CompletedAt deliberately nil — the in-memory guard alone would NOT
			// have blocked the overlay. R1 must do it.
			"dev_tests": {Status: slippy.StepStatusRunning, CompletedAt: nil},
		},
	}
	applied := overlayPipelineStep(
		slip, "dev_tests", slippy.StepStatusRunning, time.Now(),
		eventLogReturns(slippy.StepStatusCompleted),
	)
	assert.False(t, applied, "R1 must drop non-terminal overlay when event log is terminal")
	assert.Equal(t, slippy.StepStatusRunning, slip.Steps["dev_tests"].Status,
		"Steps map must be left untouched when R1 drops the overlay")
}

// TestOverlayPipelineStep_R1_EventLogQueryError_FailOpen verifies that an event-log
// query error does not block the overlay — the in-memory guard remains in effect.
func TestOverlayPipelineStep_R1_EventLogQueryError_FailOpen(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			"dev_tests": {Status: slippy.StepStatusRunning, CompletedAt: nil},
		},
	}
	failingFn := func() (slippy.StepStatus, bool, error) {
		return "", false, assert.AnError
	}
	applied := overlayPipelineStep(slip, "dev_tests", slippy.StepStatusCompleted, time.Now(), failingFn)
	assert.True(t, applied, "fail-open: query error must not block a terminal overlay when the in-memory guard would allow it")
	assert.Equal(t, slippy.StepStatusCompleted, slip.Steps["dev_tests"].Status)
}

// TestOverlayPipelineStep_R1_NoEventsYet_AppliesNormally verifies that the
// !found return (no event row yet) falls through to the existing in-memory
// path — preserves first-event behaviour.
func TestOverlayPipelineStep_R1_NoEventsYet_AppliesNormally(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			"dev_tests": {Status: slippy.StepStatusRunning, CompletedAt: nil},
		},
	}
	applied := overlayPipelineStep(slip, "dev_tests", slippy.StepStatusCompleted, time.Now(), noEventLog)
	assert.True(t, applied, "no events yet must allow overlay (first-event path)")
	assert.Equal(t, slippy.StepStatusCompleted, slip.Steps["dev_tests"].Status)
}

// TestOverlayPipelineStep_R2_BothTerminalDiverge_EventLogWins covers the PR #39
// review finding: a same-µs race where both writers send terminal but DIFFER
// (e.g. completed vs failed). argMax in ClickHouse resolves to failed (status_int
// tiebreak 5 > 4) but without this branch, the OTHER writer's overlay would pin
// its caller-supplied terminal status into routing_slips.<step>_status, briefly
// disagreeing with argMax truth.
//
// Contract: when event-log status is terminal AND caller status is terminal AND
// they differ, the overlay pins the EVENT-LOG status (substitutes into the
// step's transition + the *_status override) so the *_status column matches the
// argMax-resolved row.
func TestOverlayPipelineStep_R2_BothTerminalDiverge_EventLogWins(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			// CompletedAt nil so the in-memory guard would otherwise have allowed
			// the caller's terminal overlay through. The R2 divergence branch is
			// the only thing forcing event-log wins.
			"dev_tests": {Status: slippy.StepStatusRunning, CompletedAt: nil},
		},
	}
	// Caller wrote completed; event log shows failed (the argMax-winning row).
	applied := overlayPipelineStep(
		slip, "dev_tests", slippy.StepStatusCompleted, time.Now(),
		eventLogReturns(slippy.StepStatusFailed),
	)
	assert.True(t, applied,
		"divergent terminals: overlay MUST still apply so the pin path runs with event-log truth")
	assert.Equal(t, slippy.StepStatusFailed, slip.Steps["dev_tests"].Status,
		"divergent terminals: overlay MUST substitute event-log status (failed) for caller status (completed)")
	require.NotNil(t, slip.Steps["dev_tests"].CompletedAt,
		"divergent-terminal overlay must set CompletedAt via ApplyStatusTransition")
}

// TestOverlayPipelineStep_R2_BothTerminalAgree_AppliesNormally is the negative
// control: when caller and event log agree on the same terminal status, the
// divergence branch does NOT fire and the overlay applies the caller status
// (which equals the event-log status) as a normal terminal pin.
func TestOverlayPipelineStep_R2_BothTerminalAgree_AppliesNormally(t *testing.T) {
	slip := &slippy.Slip{
		Steps: map[string]slippy.Step{
			"dev_tests": {Status: slippy.StepStatusRunning, CompletedAt: nil},
		},
	}
	applied := overlayPipelineStep(
		slip, "dev_tests", slippy.StepStatusCompleted, time.Now(),
		eventLogReturns(slippy.StepStatusCompleted),
	)
	assert.True(t, applied)
	assert.Equal(t, slippy.StepStatusCompleted, slip.Steps["dev_tests"].Status)
}

// TestHydrateAndPersist_StartStep_DoesNotClobberTerminalStatus is the F1 end-to-end
// regression test via the real hydrateAndPersist path. Reproduces the production
// scenario from slip b058127d where a second StartStep arrived 3 minutes after
// CompleteStep, and the overlay clobbered "completed" with "running".
//
// R1 extension (ADO #82468): the regression is now driven by the event-log
// guard via latestStepStatusFromEventsFn, not the in-memory CompletedAt. Also
// verifies that R2 Option D drops the step override when R1 dropped the overlay.
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
	var observedOverrides []slippy.StepStatusOverride
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
		updateFn: func(_ context.Context, s *slippy.Slip, overrides ...slippy.StepStatusOverride) error {
			persistedSlip = s
			observedOverrides = append([]slippy.StepStatusOverride(nil), overrides...)
			return nil
		},
		// R1: event log says completed. R1 must drop the running overlay.
		latestStepStatusFromEventsFn: func(_ context.Context, _, step string) (slippy.StepStatus, bool, error) {
			if step == stepName {
				return slippy.StepStatusCompleted, true, nil
			}
			return "", false, nil
		},
	}
	adapter := newTestWriterAdapter(store)

	// StartStep on a pipeline step (componentName == "") triggers hydrateAndPersist
	// with StepStatusRunning. The R1 guard must prevent the running overlay from
	// clobbering the terminal completed status already visible in Load.
	err := adapter.StartStep(context.Background(), id, stepName, "")
	require.NoError(t, err)
	require.NotNil(t, persistedSlip, "Update must still be called (hydration runs)")

	assert.Equal(
		t,
		slippy.StepStatusCompleted,
		persistedSlip.Steps[stepName].Status,
		"StartStep must not clobber terminal completed status: F1/R1 regression (slip b058127d)",
	)
	// R2 Option D: when R1 drops the overlay, hydrateAndPersist must NOT pin an
	// override on Update — otherwise the override would clobber the
	// routing_slips column literal back to running.
	assert.Empty(t, observedOverrides,
		"hydrateAndPersist must pass NO override when R1 dropped the overlay (Option D conditional pin)")
}

// --- Option 1 sentinel propagation tests (plan v3 §C.8) ---
//
// These tests assert the adapter propagates slippy.ErrTerminalAlreadyExists
// unwrapped enough that errors.Is at the handler boundary succeeds. The
// library wraps the sentinel in *slippy.StepError; the adapter must NOT
// double-wrap or swallow it.

// TestSlipWriterAdapter_StartStep_ErrTerminalAlreadyExists_PropagatesSentinel
// verifies the gate sentinel survives the adapter layer for StartStep.
func TestSlipWriterAdapter_StartStep_ErrTerminalAlreadyExists_PropagatesSentinel(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return slippy.ErrTerminalAlreadyExists
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.StartStep(context.Background(), "abc-123", "push_parsed", "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, slippy.ErrTerminalAlreadyExists),
		"adapter MUST propagate ErrTerminalAlreadyExists so the handler can map to 409 (plan v3 §C.1)")
}

// TestSlipWriterAdapter_CompleteStep_ErrTerminalAlreadyExists_PropagatesSentinel
// verifies the gate sentinel survives for CompleteStep on a pipeline step.
func TestSlipWriterAdapter_CompleteStep_ErrTerminalAlreadyExists_PropagatesSentinel(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return slippy.ErrTerminalAlreadyExists
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.CompleteStep(context.Background(), "abc-123", "push_parsed", "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, slippy.ErrTerminalAlreadyExists),
		"adapter MUST propagate ErrTerminalAlreadyExists for CompleteStep")
}

// TestSlipWriterAdapter_FailStep_ErrTerminalAlreadyExists_PropagatesSentinel
// verifies the gate sentinel survives for FailStep on a pipeline step.
func TestSlipWriterAdapter_FailStep_ErrTerminalAlreadyExists_PropagatesSentinel(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return slippy.ErrTerminalAlreadyExists
		},
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			return &slippy.Slip{CorrelationID: "abc-123", Status: slippy.SlipStatusInProgress}, nil
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.FailStep(context.Background(), "abc-123", "push_parsed", "", "reason")
	require.Error(t, err)
	assert.True(t, errors.Is(err, slippy.ErrTerminalAlreadyExists),
		"adapter MUST propagate ErrTerminalAlreadyExists for FailStep")
}

// TestSlipWriterAdapter_SkipStep_ErrTerminalAlreadyExists_PropagatesSentinel
// verifies the gate sentinel survives for SkipStep on a pipeline step.
func TestSlipWriterAdapter_SkipStep_ErrTerminalAlreadyExists_PropagatesSentinel(t *testing.T) {
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return slippy.ErrTerminalAlreadyExists
		},
	}
	adapter := newTestWriterAdapter(store)

	err := adapter.SkipStep(context.Background(), "abc-123", "push_parsed", "", "reason")
	require.Error(t, err)
	assert.True(t, errors.Is(err, slippy.ErrTerminalAlreadyExists),
		"adapter MUST propagate ErrTerminalAlreadyExists for SkipStep")
}

// --- Per-correlationID lock unit tests (plan v3 §M) ---

// mockLocker is a controllable Locker for unit-testing withCorrIDLock.
type mockLocker struct {
	tryAcquireFn func(ctx context.Context, key string, ttl time.Duration) (bool, string, error)
	releaseFn    func(ctx context.Context, key, token string) error
	releaseCalls int
}

func (m *mockLocker) TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, string, error) {
	if m.tryAcquireFn != nil {
		return m.tryAcquireFn(ctx, key, ttl)
	}
	return true, "tok", nil
}

func (m *mockLocker) Release(ctx context.Context, key, token string) error {
	m.releaseCalls++
	if m.releaseFn != nil {
		return m.releaseFn(ctx, key, token)
	}
	return nil
}

// withLockEnabledAdapter constructs a SlipWriterAdapter with the I5 lock flag
// injected = true and the supplied locker wired in. Pattern lets each test
// focus on lock semantics without rewiring constructors. Now that the flag is
// a constructor parameter (DI), this helper no longer needs t.Setenv —
// callers are safe to run with t.Parallel().
func withLockEnabledAdapter(t *testing.T, store slippy.SlipStore, locker Locker) *SlipWriterAdapter {
	t.Helper()
	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(testPipelineConfigJSON))
	require.NoError(t, err)
	client := slippy.NewClientWithDependencies(store, &mockGitHubAPI{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})
	return NewSlipWriterAdapter(client, locker, nil, true)
}

// validTestUUID is a valid UUID string used by lock tests where the corrID
// must parse for CorrIDLockKey to return a non-empty key.
const validTestUUID = "11111111-2222-3333-4444-555555555555"

// TestCorrIDLock_StartStep_LockMiss_Returns409 verifies that acquired=false
// returns ErrCorrIDWriteInProgress without calling the underlying client.
func TestCorrIDLock_StartStep_LockMiss_Returns409(t *testing.T) {
	var clientCalled bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			clientCalled = true
			return nil
		},
	}
	locker := &mockLocker{
		tryAcquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", nil
		},
	}
	adapter := withLockEnabledAdapter(t, store, locker)

	err := adapter.StartStep(context.Background(), validTestUUID, "push_parsed", "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrCorrIDWriteInProgress),
		"lock miss MUST return ErrCorrIDWriteInProgress (mapped to 409)")
	assert.False(t, clientCalled, "underlying client MUST NOT run when lock acquire failed")
}

// TestCorrIDLock_PromoteSlip_LockMiss_Returns409 covers Mod 2 — PromoteSlip
// adapter wrapping.
func TestCorrIDLock_PromoteSlip_LockMiss_Returns409(t *testing.T) {
	var clientCalled bool
	store := &mockSlipStore{
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			clientCalled = true
			return &slippy.Slip{}, nil
		},
	}
	locker := &mockLocker{
		tryAcquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", nil
		},
	}
	adapter := withLockEnabledAdapter(t, store, locker)

	err := adapter.PromoteSlip(context.Background(), validTestUUID, "newCorrID")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrCorrIDWriteInProgress),
		"PromoteSlip lock miss MUST return ErrCorrIDWriteInProgress")
	assert.False(t, clientCalled, "PromoteSlip client MUST NOT run when lock acquire failed")
}

// TestCorrIDLock_AbandonSlip_LockMiss_Returns409 covers Mod 2 — AbandonSlip
// adapter wrapping.
func TestCorrIDLock_AbandonSlip_LockMiss_Returns409(t *testing.T) {
	var clientCalled bool
	store := &mockSlipStore{
		loadFn: func(_ context.Context, _ string) (*slippy.Slip, error) {
			clientCalled = true
			return &slippy.Slip{}, nil
		},
	}
	locker := &mockLocker{
		tryAcquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", nil
		},
	}
	adapter := withLockEnabledAdapter(t, store, locker)

	err := adapter.AbandonSlip(context.Background(), validTestUUID, "newCorrID")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrCorrIDWriteInProgress),
		"AbandonSlip lock miss MUST return ErrCorrIDWriteInProgress")
	assert.False(t, clientCalled, "AbandonSlip client MUST NOT run when lock acquire failed")
}

// TestCorrIDLock_InvalidCorrID_ReturnsErrInvalidCorrelationID verifies the
// in-adapter defense-in-depth UUID check rejects malformed corrIDs even
// when the handler middleware were absent.
func TestCorrIDLock_InvalidCorrID_ReturnsErrInvalidCorrelationID(t *testing.T) {
	store := &mockSlipStore{}
	locker := &mockLocker{}
	adapter := withLockEnabledAdapter(t, store, locker)

	err := adapter.StartStep(context.Background(), "not-a-uuid", "push_parsed", "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidCorrelationID),
		"non-UUID corrID MUST return domain.ErrInvalidCorrelationID")
}

// TestCorrIDLock_TryAcquireError_FailsOpen verifies that a Redis transport
// failure falls open — the client runs UNLOCKED rather than blocking the
// write. Matches the dedup_lock fail-open contract.
func TestCorrIDLock_TryAcquireError_FailsOpen(t *testing.T) {
	var clientCalled bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			clientCalled = true
			return nil
		},
	}
	locker := &mockLocker{
		tryAcquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", errors.New("redis: connection refused")
		},
	}
	adapter := withLockEnabledAdapter(t, store, locker)

	err := adapter.StartStep(context.Background(), validTestUUID, "push_parsed", "")
	require.NoError(t, err, "TryAcquire error MUST fall open (client runs unlocked)")
	assert.True(t, clientCalled, "client MUST run despite locker outage")
}

// TestCorrIDLock_NilLocker_BehavesAsBefore verifies the nil-locker path is
// identical to the pre-lock baseline — same as the dedup_lock contract.
// Even with the I5 lock flag ON, a nil locker MUST short-circuit the lock path.
func TestCorrIDLock_NilLocker_BehavesAsBefore(t *testing.T) {
	t.Parallel()
	var clientCalled bool
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			clientCalled = true
			return nil
		},
	}
	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(testPipelineConfigJSON))
	require.NoError(t, err)
	client := slippy.NewClientWithDependencies(store, &mockGitHubAPI{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})
	// i5LockEnabled=true + nil locker → lock path skipped (nil-locker short-circuit).
	adapter := NewSlipWriterAdapter(client, nil, nil, true)

	err = adapter.StartStep(context.Background(), validTestUUID, "push_parsed", "")
	require.NoError(t, err)
	assert.True(t, clientCalled, "nil locker MUST behave exactly as the pre-lock baseline")
}

// TestCorrIDLock_FlagOff_DoesNotAcquire verifies that i5LockEnabled=false skips
// the lock path entirely even when a locker is wired.
func TestCorrIDLock_FlagOff_DoesNotAcquire(t *testing.T) {
	t.Parallel()
	store := &mockSlipStore{
		updateStepWithHistoryFn: func(_ context.Context, _, _, _ string, _ slippy.StepStatus, _ slippy.StateHistoryEntry) error {
			return nil
		},
	}
	locker := &mockLocker{
		tryAcquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			t.Fatal("TryAcquire MUST NOT be called when i5LockEnabled=false")
			return false, "", nil
		},
	}
	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(testPipelineConfigJSON))
	require.NoError(t, err)
	client := slippy.NewClientWithDependencies(store, &mockGitHubAPI{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})
	adapter := NewSlipWriterAdapter(client, locker, nil, false)

	err = adapter.StartStep(context.Background(), validTestUUID, "push_parsed", "")
	require.NoError(t, err)
}

// TestCorrIDLockKey_ValidUUID returns deterministic lowercase key.
func TestCorrIDLockKey_ValidUUID(t *testing.T) {
	got := CorrIDLockKey("AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE")
	assert.Equal(t, "sliplock:cid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", got)
}

// TestCorrIDLockKey_InvalidUUID returns empty so caller knows to reject.
func TestCorrIDLockKey_InvalidUUID(t *testing.T) {
	cases := []string{
		"",
		"abc-123",
		"not-a-uuid-at-all",
		"../../../etc/passwd",
		"' OR 1=1; --",
	}
	for _, c := range cases {
		got := CorrIDLockKey(c)
		assert.Empty(t, got, "input %q must return empty key", c)
	}
}
