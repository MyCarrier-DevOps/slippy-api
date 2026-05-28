// Package watchdog provides a server-side safety net that detects slip steps
// stuck in the "running" state past a configurable maximum duration and, in
// enforce mode, fails them via the existing SlipWriter.
//
// Why this exists: a step is marked "running" by StartStep and is expected to
// reach a terminal status when its worker sends the terminal callback. If that
// callback is lost (orphaned worker, dropped message — the class of bug behind
// stranded slip b0320858 / RCA cc5622f7), the step hangs in "running" forever
// and wedges the whole pipeline. The watchdog periodically sweeps for such
// stuck steps and degrades them to "failed" (or just alerts), reusing the
// canonical SlipWriter.FailStep mutation path so it inherits the I5
// read-your-own-writes materialization fix and pipeline-completion propagation.
//
// Correctness: the watchdog must LOSE to a genuine completion. Three guards make
// that so — a generous updated_at-based threshold (a step about to complete is
// still emitting transitions, so its slip is never stale enough to be selected),
// a pre-mutate re-check of the latest event-log status (skip if no longer
// "running"), and the timestamp-versioned event-sourced store as a backstop. It
// is idempotent: a failed step no longer matches "running", so no dedup table is
// needed and re-fail storms cannot happen.
package watchdog

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/config"
	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

const (
	tracerName = "slippy-api/watchdog"
	meterName  = "slippy-api/watchdog"

	// runningStatus is the materialized step status the watchdog targets.
	runningStatus = string(slippy.StepStatusRunning)

	// componentStatesTable is the event-log table read for the pre-mutate
	// re-check (latest pipeline-level status via argMax).
	componentStatesTable = "slip_component_states"

	// pipelineComponent is the component name for pipeline-level (aggregate /
	// materialized) step events. The watchdog only ever operates on
	// pipeline-level step columns, so the re-check uses the empty component.
	pipelineComponent = ""
)

// stuckStep is one (correlationID, stepName) pair detected as stuck.
type stuckStep struct {
	CorrelationID string
	StepName      string
	UpdatedAt     time.Time
}

// Watchdog periodically sweeps routing_slips for steps stuck in "running" past a
// max duration and, in enforce mode, fails them via the SlipWriter.
type Watchdog struct {
	mode       string
	enforce    bool
	threshold  time.Duration
	interval   time.Duration
	batchLimit int
	database   string

	// steps is the ordered list of step-status column bases from the pipeline
	// config (e.g. "builds", "unit_tests"). Built once at construction.
	steps []string

	// session is the read-only ClickHouse session (shared with the store) used
	// for the detection query and the pre-mutate re-check.
	session ch.ClickhouseSessionInterface

	// writer is the ONLY mutation dependency. The watchdog never writes slip
	// state by any other path so it inherits the I5 hydration fix and pipeline
	// completion propagation that SlipWriterAdapter.FailStep provides.
	writer domain.SlipWriter

	logger *slog.Logger

	// metric instruments (best-effort; nil-safe via metric.Meter no-op fallback).
	stuckCounter  metric.Int64Counter
	failedCounter metric.Int64Counter
	errorCounter  metric.Int64Counter
	sweepDuration metric.Float64Histogram

	// now is overridable in tests; defaults to time.Now.
	now func() time.Time
}

// New constructs a Watchdog. It reads configuration and the pipeline step list
// up front. The session is used only for reads; all mutation goes through writer.
func New(
	cfg *config.Config,
	session ch.ClickhouseSessionInterface,
	writer domain.SlipWriter,
	pipelineCfg *slippy.PipelineConfig,
	logger *slog.Logger,
) *Watchdog {
	if logger == nil {
		logger = slog.Default()
	}

	steps := make([]string, 0)
	if pipelineCfg != nil {
		for _, s := range pipelineCfg.Steps {
			steps = append(steps, s.Name)
		}
	}

	w := &Watchdog{
		mode:       cfg.WatchdogMode,
		enforce:    cfg.WatchdogEnforces(),
		threshold:  cfg.StepRunningMaxDuration,
		interval:   cfg.WatchdogSweepInterval,
		batchLimit: cfg.WatchdogBatchLimit,
		database:   cfg.SlipDatabase,
		steps:      steps,
		session:    session,
		writer:     writer,
		logger:     logger,
		now:        time.Now,
	}

	w.initInstruments()
	return w
}

// Run drives the periodic sweep until ctx is cancelled. The first sweep fires
// after the first interval tick (not immediately at startup) to avoid startup
// churn; the loop exits promptly when ctx is cancelled at shutdown.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.logger.InfoContext(ctx, "watchdog started",
		"mode", w.mode,
		"threshold", w.threshold.String(),
		"interval", w.interval.String(),
		"batch_limit", w.batchLimit,
	)

	for {
		select {
		case <-ctx.Done():
			w.logger.InfoContext(ctx, "watchdog stopped", "reason", ctx.Err())
			return
		case <-ticker.C:
			if _, _, err := w.sweepOnce(ctx); err != nil {
				w.logger.ErrorContext(ctx, "watchdog sweep failed", "error", err)
			}
		}
	}
}

// sweepOnce runs a single detection + (optional) enforcement pass. It returns
// the number of stuck steps detected and the number actually failed. Errors from
// individual FailStep calls are logged and do not abort the batch; only a fatal
// detection error is returned.
func (w *Watchdog) sweepOnce(ctx context.Context) (stuckFound, failed int, err error) {
	start := w.now()
	ctx, span := otel.Tracer(tracerName).Start(ctx, "watchdog.sweepOnce",
		trace.WithAttributes(
			attribute.String("watchdog.mode", w.mode),
			attribute.String("watchdog.threshold", w.threshold.String()),
		),
	)
	defer span.End()
	defer func() {
		if w.sweepDuration != nil {
			w.sweepDuration.Record(ctx, w.now().Sub(start).Seconds())
		}
	}()

	cutoff := start.Add(-w.threshold)
	stuck, derr := w.detectStuckSteps(ctx, cutoff)
	if derr != nil {
		span.RecordError(derr)
		span.SetStatus(codes.Error, "detection failed")
		if w.errorCounter != nil {
			w.errorCounter.Add(ctx, 1)
		}
		return 0, 0, derr
	}

	stuckFound = len(stuck)
	if w.stuckCounter != nil && stuckFound > 0 {
		w.stuckCounter.Add(ctx, int64(stuckFound))
	}
	span.SetAttributes(attribute.Int("watchdog.stuck_count", stuckFound))

	for _, s := range stuck {
		reason := w.reason(s)
		span.AddEvent("stuck_step_detected", trace.WithAttributes(
			attribute.String("slip.correlation_id", s.CorrelationID),
			attribute.String("slip.step_name", s.StepName),
			attribute.String("slip.updated_at", s.UpdatedAt.UTC().Format(time.RFC3339)),
		))
		w.logger.WarnContext(ctx, "watchdog detected stuck step",
			"correlation_id", s.CorrelationID,
			"step", s.StepName,
			"updated_at", s.UpdatedAt.UTC().Format(time.RFC3339),
			"mode", w.mode,
		)

		if !w.enforce {
			// alert mode: detect + emit only, never mutate.
			continue
		}

		// Re-check immediately before mutating: a genuine terminal event may have
		// landed in the detection→action gap. If the step is no longer running, we
		// LOSE the race and skip — the real completion wins.
		stillRunning, rcErr := w.stepStillRunning(ctx, s.CorrelationID, s.StepName)
		if rcErr != nil {
			// Conservative: on re-check error, do NOT fail the step (avoid failing a
			// step that may have completed). Log and move on.
			w.logger.ErrorContext(ctx, "watchdog re-check failed; skipping fail",
				"correlation_id", s.CorrelationID, "step", s.StepName, "error", rcErr)
			if w.errorCounter != nil {
				w.errorCounter.Add(ctx, 1)
			}
			continue
		}
		if !stillRunning {
			span.AddEvent("stuck_step_recovered_skipped", trace.WithAttributes(
				attribute.String("slip.correlation_id", s.CorrelationID),
				attribute.String("slip.step_name", s.StepName),
			))
			w.logger.InfoContext(ctx, "watchdog skip: step no longer running",
				"correlation_id", s.CorrelationID, "step", s.StepName)
			continue
		}

		// componentName is always "" — the watchdog operates on pipeline-level
		// step columns only.
		if ferr := w.writer.FailStep(ctx, s.CorrelationID, s.StepName, "", reason); ferr != nil {
			// Non-fatal: log and continue the batch.
			w.logger.ErrorContext(ctx, "watchdog FailStep failed",
				"correlation_id", s.CorrelationID, "step", s.StepName, "error", ferr)
			if w.errorCounter != nil {
				w.errorCounter.Add(ctx, 1)
			}
			continue
		}
		failed++
		if w.failedCounter != nil {
			w.failedCounter.Add(ctx, 1)
		}
		w.logger.WarnContext(ctx, "watchdog failed stuck step",
			"correlation_id", s.CorrelationID, "step", s.StepName, "reason", reason)
	}

	span.SetAttributes(attribute.Int("watchdog.failed_count", failed))
	span.SetStatus(codes.Ok, "")
	return stuckFound, failed, nil
}

// reason builds the auditable FailStep reason string with the threshold and the
// observed last-update timestamp.
func (w *Watchdog) reason(s stuckStep) string {
	return fmt.Sprintf(
		"watchdog: step exceeded max running duration (>%s, last update %s)",
		w.threshold.String(),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
}

// detectStuckSteps queries routing_slips for the latest active row per slip whose
// updated_at is older than cutoff and whose slip is not terminal, then unpivots
// the dynamic <step>_status columns in Go to emit (correlationID, stepName) pairs
// where the value is "running".
//
// Detection keys on the MATERIALIZED routing_slips.<step>_status (not the event
// log) because that is the column the rest of the system reads and the column
// that is actually wedged. updated_at is the staleness clock: every step
// transition bumps it, so a slip with no transition for >= threshold has genuinely
// stalled (a step that is merely slow but still emitting heartbeats has a recent
// updated_at and is never selected).
func (w *Watchdog) detectStuckSteps(ctx context.Context, cutoff time.Time) (result []stuckStep, err error) {
	if len(w.steps) == 0 {
		return nil, nil
	}

	statusCols := make([]string, len(w.steps))
	for i, name := range w.steps {
		statusCols[i] = fmt.Sprintf("%s_status", name)
	}

	// Latest active version per correlation_id (mirrors the store's non-FINAL
	// idiom: WHERE sign = 1 + LIMIT 1 BY correlation_id ORDER BY version DESC).
	// Terminal slips are excluded via status NOT IN (...) — see SlipStatus.IsTerminal.
	query := fmt.Sprintf(`
		SELECT correlation_id, updated_at, %s
		FROM %s.routing_slips
		WHERE sign = 1
		  AND updated_at < {cutoff:DateTime64(9, 'UTC')}
		  AND status NOT IN ('completed', 'compensated', 'abandoned', 'promoted')
		ORDER BY correlation_id, version DESC
		LIMIT 1 BY correlation_id
		LIMIT {batchLimit:UInt32}`,
		strings.Join(statusCols, ", "),
		w.database,
	)

	rows, err := w.session.QueryWithArgs(ctx, query,
		ch.Named("cutoff", cutoff.UTC()),
		ch.Named("batchLimit", uint32(w.batchLimit)),
	)
	if err != nil {
		return nil, fmt.Errorf("watchdog: detection query failed: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("watchdog: closing detection rows: %w", closeErr)
		}
	}()

	for rows.Next() {
		var correlationID string
		var updatedAt time.Time
		statusVals := make([]string, len(statusCols))

		dest := make([]any, 0, len(statusCols)+2)
		dest = append(dest, &correlationID, &updatedAt)
		for i := range statusVals {
			dest = append(dest, &statusVals[i])
		}
		if scanErr := rows.Scan(dest...); scanErr != nil {
			return nil, fmt.Errorf("watchdog: scanning detection row: %w", scanErr)
		}

		for i, val := range statusVals {
			if val == runningStatus {
				result = append(result, stuckStep{
					CorrelationID: correlationID,
					StepName:      w.steps[i],
					UpdatedAt:     updatedAt,
				})
			}
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("watchdog: iterating detection rows: %w", rowsErr)
	}

	return result, nil
}

// stepStillRunning re-reads the latest pipeline-level status for a step from the
// event log (argMax(status, timestamp) over slip_component_states with the empty
// component) and reports whether it is still "running". This is the primary race
// guard: if a genuine terminal event landed since detection, the step is no
// longer running and the watchdog must skip it.
func (w *Watchdog) stepStillRunning(ctx context.Context, correlationID, stepName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT argMax(status, timestamp)
		FROM %s.%s
		WHERE correlation_id = {correlationId:String}
		  AND step = {step:String}
		  AND component = {component:String}`,
		w.database, componentStatesTable,
	)

	row := w.session.QueryRow(ctx, query,
		ch.Named("correlationId", correlationID),
		ch.Named("step", stepName),
		ch.Named("component", pipelineComponent),
	)

	var latest string
	if scanErr := row.Scan(&latest); scanErr != nil {
		return false, fmt.Errorf("watchdog: re-check status read failed: %w", scanErr)
	}

	// An empty result (no event-log rows for the pipeline-level step) means we
	// cannot confirm it is running via the event log; the materialized column is
	// the wedge in that case (Shape B). Treat empty as "still running" so the
	// watchdog can self-heal the stuck materialization — the threshold + the
	// materialized-running detection already established staleness.
	if latest == "" {
		return true, nil
	}
	return latest == runningStatus, nil
}

// initInstruments creates the OTel metric instruments. Creation errors are
// non-fatal — the instrument stays nil and metric calls are guarded — but they
// are logged so a misconfigured meter is visible.
func (w *Watchdog) initInstruments() {
	meter := otel.Meter(meterName)
	var err error
	if w.stuckCounter, err = meter.Int64Counter("watchdog.sweep.stuck_steps",
		metric.WithDescription("Steps detected stuck in running past the max duration")); err != nil {
		w.logger.Warn("watchdog: stuck_steps counter init failed", "error", err)
	}
	if w.failedCounter, err = meter.Int64Counter("watchdog.sweep.failed_steps",
		metric.WithDescription("Stuck steps the watchdog failed in enforce mode")); err != nil {
		w.logger.Warn("watchdog: failed_steps counter init failed", "error", err)
	}
	if w.errorCounter, err = meter.Int64Counter("watchdog.sweep.errors",
		metric.WithDescription("Watchdog sweep errors")); err != nil {
		w.logger.Warn("watchdog: errors counter init failed", "error", err)
	}
	if w.sweepDuration, err = meter.Float64Histogram("watchdog.sweep.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Watchdog sweep wall-clock duration")); err != nil {
		w.logger.Warn("watchdog: sweep_duration histogram init failed", "error", err)
	}
}
