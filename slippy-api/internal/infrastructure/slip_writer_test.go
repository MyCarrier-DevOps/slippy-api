package infrastructure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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

// TestWriteOpTimeout_Default verifies the package-level default write timeout.
// Postgres writes commit in milliseconds, so the bound is small; this guards
// against an accidental revert to a large ClickHouse-era value.
func TestWriteOpTimeout_Default(t *testing.T) {
	// Skip if SLIPPY_WRITE_OP_TIMEOUT is set in the environment: the package-init
	// var will reflect the overridden value, not the compile-time default, making
	// the live-var assertion spuriously fail. The constant assertion still holds.
	if os.Getenv("SLIPPY_WRITE_OP_TIMEOUT") != "" {
		t.Skip("SLIPPY_WRITE_OP_TIMEOUT is set; skipping live-var assertion to avoid false failure")
	}
	// writeOpTimeout is set at package init from initWriteOpTimeout(). In the
	// test environment SLIPPY_WRITE_OP_TIMEOUT is unset, so it must equal the
	// compile-time default.
	assert.Equal(t, defaultWriteOpTimeout, 30*time.Second,
		"defaultWriteOpTimeout constant must be 30s")
	// The live var should also match the default when the env is absent.
	// (Tests that shorten it via withTestWriteOpTimeout restore it in t.Cleanup.)
	assert.GreaterOrEqual(t, writeOpTimeout, 30*time.Second,
		"writeOpTimeout must be at least 30s in a clean test environment")
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

func TestIsLockTimeout(t *testing.T) {
	assert.True(t, isLockTimeout(&pgconn.PgError{Code: "55P03"}))
	assert.True(t, isLockTimeout(fmt.Errorf("failed to lock slip: %w", &pgconn.PgError{Code: "55P03"})))
	assert.False(t, isLockTimeout(&pgconn.PgError{Code: "57014"}), "statement timeout is not a lock timeout")
	assert.False(t, isLockTimeout(errors.New("boom")))
	assert.False(t, isLockTimeout(nil))
}

func TestWriteWithLockRetry(t *testing.T) {
	a := &SlipWriterAdapter{}
	lockErr := &pgconn.PgError{Code: "55P03"}
	newSpan := func() trace.Span {
		_, span := otel.Tracer("test").Start(context.Background(), "t")
		return span
	}

	t.Run("retries lock timeout then succeeds", func(t *testing.T) {
		calls := 0
		err := a.writeWithLockRetry(context.Background(), newSpan(), func(context.Context, trace.Span) error {
			calls++
			if calls < 2 {
				return lockErr
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, calls)
	})

	t.Run("non-lock error is not retried", func(t *testing.T) {
		boom := errors.New("boom")
		calls := 0
		err := a.writeWithLockRetry(context.Background(), newSpan(), func(context.Context, trace.Span) error {
			calls++
			return boom
		})
		require.ErrorIs(t, err, boom)
		assert.Equal(t, 1, calls)
	})

	t.Run("gives up after maxLockRetries", func(t *testing.T) {
		calls := 0
		err := a.writeWithLockRetry(context.Background(), newSpan(), func(context.Context, trace.Span) error {
			calls++
			return lockErr
		})
		require.Error(t, err)
		assert.True(t, isLockTimeout(err))
		assert.Equal(t, maxLockRetries+1, calls)
	})
}

func TestTranslateStoreError(t *testing.T) {
	assert.ErrorIs(t, translateStoreError(&pgconn.PgError{Code: "55P03"}), domain.ErrWriteContended)
	assert.ErrorIs(t,
		translateStoreError(fmt.Errorf("failed to lock slip: %w", &pgconn.PgError{Code: "55P03"})),
		domain.ErrWriteContended)
	assert.ErrorIs(t, translateStoreError(&pgconn.PgError{Code: "57014"}), domain.ErrStatementTimeout)

	boom := errors.New("boom") // unrecognized errors pass through unchanged
	assert.Equal(t, boom, translateStoreError(boom))
	assert.NoError(t, translateStoreError(nil))
}

// assumedLockTimeout documents the goLibMyCarrier/postgres server-side lock_timeout that a
// single lock wait can consume; used only to guard the retry budget below.
const assumedLockTimeout = 10 * time.Second

func TestLockRetryBudgetFitsWriteTimeout(t *testing.T) {
	// (maxLockRetries+1) attempts, each able to wait up to a lock_timeout, must finish inside
	// the write-op budget — else the outer context expires first and the write surfaces as a
	// generic 504 instead of the retryable 503 (domain.ErrWriteContended).
	worst := time.Duration(maxLockRetries+1) * assumedLockTimeout
	assert.Less(t, worst, defaultWriteOpTimeout,
		"lock-retry worst case %s must stay under writeOpTimeout %s", worst, defaultWriteOpTimeout)
}
