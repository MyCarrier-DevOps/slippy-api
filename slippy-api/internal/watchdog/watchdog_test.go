package watchdog

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse/clickhousetest"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/config"
	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- mock SlipWriter capturing FailStep calls ---

type failStepCall struct {
	correlationID string
	stepName      string
	componentName string
	reason        string
}

type mockWriter struct {
	mu       sync.Mutex
	calls    []failStepCall
	failWith error
}

func (m *mockWriter) FailStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, failStepCall{correlationID, stepName, componentName, reason})
	return m.failWith
}

func (m *mockWriter) failStepCalls() []failStepCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]failStepCall, len(m.calls))
	copy(out, m.calls)
	return out
}

// The remaining SlipWriter methods are unused by the watchdog.
func (m *mockWriter) CreateSlipForPush(context.Context, domain.PushOptions) (*domain.CreateSlipResult, error) {
	return nil, nil
}
func (m *mockWriter) StartStep(context.Context, string, string, string) error    { return nil }
func (m *mockWriter) CompleteStep(context.Context, string, string, string) error { return nil }
func (m *mockWriter) SkipStep(context.Context, string, string, string, string) error {
	return nil
}
func (m *mockWriter) SetComponentImageTag(context.Context, string, string, string) error { return nil }
func (m *mockWriter) PromoteSlip(context.Context, string, string) error                  { return nil }
func (m *mockWriter) AbandonSlip(context.Context, string, string) error                  { return nil }

var _ domain.SlipWriter = (*mockWriter)(nil)

// --- helpers ---

// testPipelineConfig builds a config with one aggregate step + two plain steps.
func testPipelineConfig() *slippy.PipelineConfig {
	return &slippy.PipelineConfig{
		Name: "test",
		Steps: []slippy.StepConfig{
			{Name: "builds", Aggregates: "build"},
			{Name: "unit_tests"},
			{Name: "dev_deploy"},
		},
	}
}

// detectionRows returns a MockRows that yields the supplied rows. Each row is
// [correlationID(string), updatedAt(time.Time), status0(string), status1, status2].
func detectionRows(rows [][]any) *clickhousetest.MockRows {
	idx := 0
	return &clickhousetest.MockRows{
		NextFunc: func() bool {
			has := idx < len(rows)
			return has
		},
		ScanFunc: func(dest ...any) error {
			row := rows[idx]
			idx++
			for i := range dest {
				switch d := dest[i].(type) {
				case *string:
					*d = row[i].(string)
				case *time.Time:
					*d = row[i].(time.Time)
				}
			}
			return nil
		},
	}
}

// newTestWatchdog builds a watchdog over the given config + session + writer with
// the test pipeline config.
func newTestWatchdog(t *testing.T, mode string, session *clickhousetest.MockSession, w domain.SlipWriter) *Watchdog {
	t.Helper()
	cfg := &config.Config{
		WatchdogMode:           mode,
		StepRunningMaxDuration: 2 * time.Hour,
		WatchdogSweepInterval:  5 * time.Minute,
		WatchdogBatchLimit:     100,
		SlipDatabase:           "ci_test",
	}
	wd := New(cfg, session, w, testPipelineConfig(), nil)
	return wd
}

// runningReReadRow returns a MockSession.QueryRowFunc that always reports the
// step is still running (re-check passes).
func stillRunningRow() func(ctx context.Context, query string, args ...any) driver.Row {
	return func(ctx context.Context, query string, args ...any) driver.Row {
		return &clickhousetest.MockRow{ScanData: []any{string(slippy.StepStatusRunning)}}
	}
}

// --- tests ---

func TestDetectStuckSteps_BuildsDynamicColumns(t *testing.T) {
	var capturedQuery string
	var capturedArgs []any
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			capturedQuery = query
			capturedArgs = args
			return detectionRows(nil), nil
		},
	}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, &mockWriter{})

	cutoff := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	_, err := wd.detectStuckSteps(context.Background(), cutoff)
	require.NoError(t, err)

	// All three step-status columns must be selected.
	assert.Contains(t, capturedQuery, "builds_status")
	assert.Contains(t, capturedQuery, "unit_tests_status")
	assert.Contains(t, capturedQuery, "dev_deploy_status")
	// Terminal slips excluded.
	assert.Contains(t, capturedQuery, "status NOT IN ('completed', 'compensated', 'abandoned', 'promoted')")
	// updated_at staleness filter + batch limit.
	assert.Contains(t, capturedQuery, "updated_at < {cutoff:DateTime64(3, 'UTC')}")
	assert.Contains(t, capturedQuery, "LIMIT 1 BY correlation_id")
	assert.Contains(t, capturedQuery, "ci_test.routing_slips")
	require.Len(t, capturedArgs, 2)
}

func TestDetectStuckSteps_UnpivotsRunningColumns(t *testing.T) {
	updated := time.Date(2026, 5, 28, 1, 0, 0, 0, time.UTC)
	rows := [][]any{
		// correlationID, updatedAt, builds_status, unit_tests_status, dev_deploy_status
		{"slip-a", updated, "completed", "running", "pending"},
		{"slip-b", updated, "running", "running", "completed"},
	}
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(rows), nil
		},
	}
	wd := newTestWatchdog(t, config.WatchdogModeAlert, session, &mockWriter{})

	stuck, err := wd.detectStuckSteps(context.Background(), time.Now())
	require.NoError(t, err)

	require.Len(t, stuck, 3)
	assert.Equal(t, stuckStep{"slip-a", "unit_tests", updated}, stuck[0])
	assert.Equal(t, stuckStep{"slip-b", "builds", updated}, stuck[1])
	assert.Equal(t, stuckStep{"slip-b", "unit_tests", updated}, stuck[2])
}

func TestSweepOnce_ModeAlert_DetectsButDoesNotFail(t *testing.T) {
	updated := time.Now().Add(-3 * time.Hour)
	rows := [][]any{{"slip-x", updated, "running", "pending", "pending"}}
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(rows), nil
		},
		QueryRowFunc: stillRunningRow(),
	}
	w := &mockWriter{}
	wd := newTestWatchdog(t, config.WatchdogModeAlert, session, w)

	stuck, failed, err := wd.sweepOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, stuck)
	assert.Equal(t, 0, failed)
	assert.Empty(t, w.failStepCalls(), "alert mode must not call FailStep")
	// alert mode must not even re-check (no mutation path).
	assert.Empty(t, session.QueryRowCalls)
}

func TestSweepOnce_ModeEnforce_FailsStuckStep(t *testing.T) {
	updated := time.Now().Add(-3 * time.Hour)
	rows := [][]any{{"slip-x", updated, "pending", "running", "pending"}}
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(rows), nil
		},
		QueryRowFunc: stillRunningRow(),
	}
	w := &mockWriter{}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, w)

	stuck, failed, err := wd.sweepOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, stuck)
	assert.Equal(t, 1, failed)

	calls := w.failStepCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, "slip-x", calls[0].correlationID)
	assert.Equal(t, "unit_tests", calls[0].stepName)
	assert.Equal(t, "", calls[0].componentName, "watchdog operates pipeline-level only")
	// reason string carries the threshold + observed updated_at for auditability.
	assert.Contains(t, calls[0].reason, "watchdog: step exceeded max running duration")
	assert.Contains(t, calls[0].reason, "2h0m0s")
	assert.Contains(t, calls[0].reason, updated.UTC().Format(time.RFC3339))
}

func TestSweepOnce_RaceRecheck_SkipsWhenNoLongerRunning(t *testing.T) {
	updated := time.Now().Add(-3 * time.Hour)
	rows := [][]any{{"slip-x", updated, "running", "pending", "pending"}}
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(rows), nil
		},
		// Re-check: a genuine completion landed in the gap.
		QueryRowFunc: func(ctx context.Context, query string, args ...any) driver.Row {
			return &clickhousetest.MockRow{ScanData: []any{string(slippy.StepStatusCompleted)}}
		},
	}
	w := &mockWriter{}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, w)

	stuck, failed, err := wd.sweepOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, stuck, "still detected")
	assert.Equal(t, 0, failed, "but skipped via re-check race guard")
	assert.Empty(t, w.failStepCalls(), "must lose to a genuine completion")
}

func TestSweepOnce_RecheckError_DoesNotFail(t *testing.T) {
	updated := time.Now().Add(-3 * time.Hour)
	rows := [][]any{{"slip-x", updated, "running", "pending", "pending"}}
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(rows), nil
		},
		QueryRowFunc: func(ctx context.Context, query string, args ...any) driver.Row {
			return &clickhousetest.MockRow{ScanErr: errors.New("ch down")}
		},
	}
	w := &mockWriter{}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, w)

	_, failed, err := wd.sweepOnce(context.Background())
	require.NoError(t, err, "re-check error is per-step, non-fatal")
	assert.Equal(t, 0, failed)
	assert.Empty(t, w.failStepCalls(), "conservative: do not fail on re-check error")
}

func TestSweepOnce_FailStepError_ContinuesBatch(t *testing.T) {
	updated := time.Now().Add(-3 * time.Hour)
	rows := [][]any{
		{"slip-1", updated, "running", "pending", "pending"},
		{"slip-2", updated, "running", "pending", "pending"},
	}
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(rows), nil
		},
		QueryRowFunc: stillRunningRow(),
	}
	w := &mockWriter{failWith: errors.New("write failed")}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, w)

	stuck, failed, err := wd.sweepOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, stuck)
	assert.Equal(t, 0, failed, "both FailStep calls errored")
	// Both were still attempted — the batch was not aborted on the first error.
	assert.Len(t, w.failStepCalls(), 2)
}

func TestSweepOnce_DetectionError_ReturnsError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return nil, errors.New("ch unavailable")
		},
	}
	w := &mockWriter{}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, w)

	_, _, err := wd.sweepOnce(context.Background())
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "detection query failed"))
	assert.Empty(t, w.failStepCalls())
}

func TestStepConfirmedRunning_EmptyEventLogNotConfirmed(t *testing.T) {
	// Inverted NEW-2 semantics: an empty argMax read is ambiguous (no events, or an
	// async-lagged terminal not yet visible) so it must NOT be treated as running.
	var capturedQuery string
	session := &clickhousetest.MockSession{
		QueryRowFunc: func(ctx context.Context, query string, args ...any) driver.Row {
			capturedQuery = query
			return &clickhousetest.MockRow{ScanData: []any{""}}
		},
	}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, &mockWriter{})

	confirmed, err := wd.stepConfirmedRunning(context.Background(), "slip-x", "builds")
	require.NoError(t, err)
	assert.False(t, confirmed, "empty event log => cannot confirm running => skip (async-lag safe side)")

	// The re-check must mirror the canonical composite tiebreaker from goLibMyCarrier
	// doLoadComponentStates (sortKeyNoImageTag) so it agrees on equal-microsecond ties.
	assert.Contains(t, capturedQuery,
		"argMax(status, toUInt64(toUnixTimestamp64Micro(timestamp)) * 100 + toUInt64(toUInt8(status)))")
}

func TestStepConfirmedRunning_PositiveRunningConfirmed(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryRowFunc: func(ctx context.Context, query string, args ...any) driver.Row {
			return &clickhousetest.MockRow{ScanData: []any{string(slippy.StepStatusRunning)}}
		},
	}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, &mockWriter{})

	confirmed, err := wd.stepConfirmedRunning(context.Background(), "slip-x", "builds")
	require.NoError(t, err)
	assert.True(t, confirmed, "positive running status => confirmed")
}

// TestSweepOnce_EmptyRecheck_SkipsNoFail asserts the end-to-end inverted NEW-2
// behavior: an empty re-check result must skip the step, never call FailStep.
func TestSweepOnce_EmptyRecheck_SkipsNoFail(t *testing.T) {
	updated := time.Now().Add(-3 * time.Hour)
	rows := [][]any{{"slip-x", updated, "running", "pending", "pending"}}
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(rows), nil
		},
		// Re-check returns empty: unconfirmed (possibly async-lagged terminal).
		QueryRowFunc: func(ctx context.Context, query string, args ...any) driver.Row {
			return &clickhousetest.MockRow{ScanData: []any{""}}
		},
	}
	w := &mockWriter{}
	wd := newTestWatchdog(t, config.WatchdogModeEnforce, session, w)

	stuck, failed, err := wd.sweepOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, stuck, "still detected")
	assert.Equal(t, 0, failed, "but skipped: empty re-check cannot confirm running")
	assert.Empty(t, w.failStepCalls(), "must not fail an unconfirmed step (async-lag safe side)")
}

// sumCounter returns the total of all data points for the named Sum[int64]
// instrument across the collected metrics, or 0 if absent.
func sumCounter(rm *metricdata.ResourceMetrics, name string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if s, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// TestRun_SurvivesPanickingSweep injects a sweep that panics and asserts the Run
// loop contains it (process/loop continues), increments the error counter, and
// keeps ticking — a safety-net daemon must never crash the API it protects.
func TestRun_SurvivesPanickingSweep(t *testing.T) {
	// Install a manual-reader meter provider so we can read the error counter.
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	var sweepAttempts int64
	// Detection panics on the first few ticks, then succeeds — proving the loop
	// survives a panic AND keeps running afterward.
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			n := atomic.AddInt64(&sweepAttempts, 1)
			if n <= 2 {
				panic("boom: simulated sweep panic")
			}
			return detectionRows(nil), nil
		},
	}
	wd := newTestWatchdog(t, config.WatchdogModeAlert, session, &mockWriter{})
	wd.interval = 5 * time.Millisecond

	var panicsContained int64
	wd.onSweepPanic = func(recovered any) { atomic.AddInt64(&panicsContained, 1) }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		wd.Run(ctx)
		close(done)
	}()

	// Wait until the loop has survived both panics and run at least one clean sweep.
	deadline := time.After(3 * time.Second)
	for atomic.LoadInt64(&sweepAttempts) < 3 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("loop did not continue past panics; attempts=%d contained=%d",
				atomic.LoadInt64(&sweepAttempts), atomic.LoadInt64(&panicsContained))
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog Run did not stop on context cancel")
	}

	assert.GreaterOrEqual(t, atomic.LoadInt64(&panicsContained), int64(2),
		"both panicking sweeps must be recovered, not crash the loop")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	assert.GreaterOrEqual(t, sumCounter(&rm, "watchdog.sweep.errors"), int64(2),
		"error counter must be incremented on each recovered panic")
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
			return detectionRows(nil), nil
		},
	}
	wd := newTestWatchdog(t, config.WatchdogModeAlert, session, &mockWriter{})
	// Tighten the interval so the loop ticks during the test.
	wd.interval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		wd.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// loop exited promptly
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog Run did not stop on context cancel")
	}
}
