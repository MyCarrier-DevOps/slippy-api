package infrastructure

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// writerTracerName is the instrumentation scope for write operations.
const writerTracerName = "slippy-api/writer"

// SlipWriterAdapter adapts the upstream slippy.Client to the domain.SlipWriter
// interface. It wraps the high-level business client (not the raw store) so that
// operations like CreateSlipForPush include ancestry resolution and step updates
// include atomic history appends.
type SlipWriterAdapter struct {
	client *slippy.Client
	// locker serializes CreateSlipForPush across processes on a repo:sha key to
	// prevent duplicate GitHub push webhooks from creating two routing slips
	// ("phantom slip"). A nil locker disables dedup (cache disabled / ping failed)
	// and preserves the original lock-free behavior.
	locker Locker
	// reader is used on the lock-miss path to poll for the in-flight slip so a
	// suppressed duplicate returns the SAME slip (true idempotency). Only consulted
	// when locker is non-nil and the lock was not acquired.
	reader domain.SlipReader
	// lockTTL / lockWait tune the dedup lock. Zero values fall back to defaults.
	lockTTL  time.Duration
	lockWait time.Duration
}

// NewSlipWriterAdapter wraps a slippy.Client as a SlipWriter.
//
// locker and reader enable cross-process slip-creation deduplication. Pass a nil
// locker to disable dedup entirely (the original behavior) — for example when the
// Dragonfly/Redis cache is not configured or its startup ping failed (fail-open).
// reader is only consulted on the lock-miss path; it may be the cache-decorated
// reader so the poll observes committed rows.
func NewSlipWriterAdapter(client *slippy.Client, locker Locker, reader domain.SlipReader) *SlipWriterAdapter {
	return &SlipWriterAdapter{
		client:   client,
		locker:   locker,
		reader:   reader,
		lockTTL:  DefaultLockTTL,
		lockWait: DefaultLockWait,
	}
}

// Compile-time interface compliance check.
var _ domain.SlipWriter = (*SlipWriterAdapter)(nil)

func (a *SlipWriterAdapter) CreateSlipForPush(
	ctx context.Context,
	opts domain.PushOptions,
) (*domain.CreateSlipResult, error) {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.CreateSlipForPush",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", opts.CorrelationID),
			attribute.String("slip.repository", opts.Repository),
			attribute.String("slip.commit_sha", opts.CommitSHA),
		),
	)
	defer span.End()

	// Dedup disabled (no cache / ping failed) → behave exactly as before.
	if a.locker == nil {
		result, err := a.client.CreateSlipForPush(ctx, opts)
		if err != nil {
			recordWriterError(span, err)
			return nil, err
		}
		return result, nil
	}

	key := DedupKey(opts.Repository, opts.CommitSHA)
	span.SetAttributes(attribute.String("dedup.key", key))

	acquired, token, lockErr := a.locker.TryAcquire(ctx, key, a.lockTTL)
	if lockErr != nil {
		// FAIL-OPEN: never block CI on a cache outage. Proceed unlocked.
		span.AddEvent("dedup_lock_unavailable",
			trace.WithAttributes(attribute.String("error", lockErr.Error())))
		result, err := a.client.CreateSlipForPush(ctx, opts)
		if err != nil {
			recordWriterError(span, err)
			return nil, err
		}
		return result, nil
	}

	if acquired {
		span.AddEvent("dedup_lock_acquired")
		result, err := a.client.CreateSlipForPush(ctx, opts)
		if err != nil {
			// Release on failure so a genuine retry can re-acquire and proceed.
			if relErr := a.locker.Release(ctx, key, token); relErr != nil {
				span.AddEvent("dedup_lock_release_failed",
					trace.WithAttributes(attribute.String("error", relErr.Error())))
			}
			recordWriterError(span, err)
			return nil, err
		}
		// SUCCESS: do NOT release. Let the TTL expire so a near-simultaneous
		// duplicate stays blocked through the ClickHouse async-insert visibility
		// window. The lib's handlePushRetry is idempotent once the slip is visible.
		return result, nil
	}

	// Lock not acquired → a duplicate is in flight or was just created. Return the
	// SAME slip (idempotent) by polling LoadByCommit until it becomes visible.
	span.AddEvent("dedup_duplicate_suppressed")
	return a.awaitExistingSlip(ctx, span, key, opts)
}

func (a *SlipWriterAdapter) StartStep(ctx context.Context, correlationID, stepName, componentName string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.StartStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	writtenAt := time.Now()
	if err := a.client.StartStep(ctx, correlationID, stepName, componentName); err != nil {
		recordWriterError(span, err)
		return err
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(
			ctx, correlationID, stepName, slippy.StepStatusRunning, writtenAt,
		); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) CompleteStep(ctx context.Context, correlationID, stepName, componentName string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.CompleteStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	// Pipeline-level terminal events route directly: steps.go:101 guard fires
	// checkPipelineCompletion automatically, saving a redundant Load.
	// Component events MUST go through RunPostExecution to drive aggregate recomputation.
	writtenAt := time.Now()
	if componentName != "" {
		if _, err := a.client.RunPostExecution(ctx, slippy.PostExecutionOptions{
			CorrelationID:     correlationID,
			StepName:          stepName,
			ComponentName:     componentName,
			WorkflowSucceeded: true,
		}); err != nil {
			recordWriterError(span, err)
			return err
		}
	} else {
		if err := a.client.CompleteStep(ctx, correlationID, stepName, componentName); err != nil {
			recordWriterError(span, err)
			return err
		}
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(
			ctx, correlationID, stepName, slippy.StepStatusCompleted, writtenAt,
		); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) FailStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.FailStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	// Pipeline-level terminal events route directly: steps.go:101 guard fires
	// checkPipelineCompletion automatically, saving a redundant Load.
	// Component events MUST go through RunPostExecution to drive aggregate recomputation.
	writtenAt := time.Now()
	if componentName != "" {
		if _, err := a.client.RunPostExecution(ctx, slippy.PostExecutionOptions{
			CorrelationID:     correlationID,
			StepName:          stepName,
			ComponentName:     componentName,
			WorkflowSucceeded: false,
			FailureMessage:    reason,
		}); err != nil {
			recordWriterError(span, err)
			return err
		}
	} else {
		if err := a.client.FailStep(ctx, correlationID, stepName, componentName, reason); err != nil {
			recordWriterError(span, err)
			return err
		}
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(
			ctx, correlationID, stepName, slippy.StepStatusFailed, writtenAt,
		); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) SkipStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.SkipStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	writtenAt := time.Now()
	if err := a.client.SkipStep(ctx, correlationID, stepName, componentName, reason); err != nil {
		recordWriterError(span, err)
		return err
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(
			ctx, correlationID, stepName, slippy.StepStatusSkipped, writtenAt,
		); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) SetComponentImageTag(
	ctx context.Context,
	correlationID, componentName, imageTag string,
) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.SetComponentImageTag",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.component_name", componentName),
			attribute.String("slip.image_tag", imageTag),
		),
	)
	defer span.End()

	if err := a.client.SetComponentImageTag(ctx, correlationID, componentName, imageTag); err != nil {
		recordWriterError(span, err)
		return err
	}
	return nil
}

func (a *SlipWriterAdapter) PromoteSlip(ctx context.Context, correlationID, promotedTo string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.PromoteSlip",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.promoted_to", promotedTo),
		),
	)
	defer span.End()

	if err := a.client.PromoteSlip(ctx, correlationID, promotedTo); err != nil {
		recordWriterError(span, err)
		return err
	}
	return nil
}

func (a *SlipWriterAdapter) AbandonSlip(ctx context.Context, correlationID, supersededBy string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.AbandonSlip",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.superseded_by", supersededBy),
		),
	)
	defer span.End()

	if err := a.client.AbandonSlip(ctx, correlationID, supersededBy); err != nil {
		recordWriterError(span, err)
		return err
	}
	return nil
}

// awaitExistingSlip polls the reader for an already-in-flight slip matching the
// dedup key after a lock miss. It returns a CreateSlipResult wrapping the existing
// non-terminal slip (mirroring the lib's handlePushRetry result shape) so the
// handler is unchanged. If no non-terminal slip becomes visible before the wait
// deadline, it returns a retryable error rather than creating a second slip.
func (a *SlipWriterAdapter) awaitExistingSlip(
	ctx context.Context,
	span trace.Span,
	key string,
	opts domain.PushOptions,
) (*domain.CreateSlipResult, error) {
	if a.reader == nil {
		// No reader to poll with — degrade to a retryable error; the caller (and
		// upstream webhook delivery) can retry once the first create lands.
		return nil, fmt.Errorf("dedup: slip for %s creation in progress, retry", key)
	}

	deadline := time.Now().Add(a.lockWait)
	backoff := 250 * time.Millisecond
	const maxBackoff = time.Second

	for {
		existing, err := a.reader.LoadByCommit(ctx, opts.Repository, opts.CommitSHA)
		if err == nil && existing != nil && !existing.Status.IsTerminal() {
			span.AddEvent("dedup_existing_slip_returned",
				trace.WithAttributes(attribute.String("slip.correlation_id", existing.CorrelationID)))
			return &domain.CreateSlipResult{
				Slip:             existing,
				Warnings:         make([]error, 0),
				AncestryResolved: len(existing.Ancestry) > 0,
			}, nil
		}

		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	// Deadline exceeded without a visible non-terminal slip. Do NOT create a second
	// slip — return a retryable error so the duplicate is not materialized.
	span.AddEvent("dedup_wait_timeout")
	return nil, fmt.Errorf("dedup: slip for %s creation in progress, retry", key)
}

// isPipelineStep reports whether a step transition should trigger a post-write
// hydration. Returns true only when componentName is empty (not a component-level
// event) and the step is not an aggregate step (aggregate steps invoke
// updateAggregateStatusFromComponentStatesWithHistory internally, which already
// calls Load + Update, so we must not duplicate that work).
func (a *SlipWriterAdapter) isPipelineStep(stepName, componentName string) bool {
	if componentName != "" {
		return false
	}
	if pipelineCfg := a.client.PipelineConfig(); pipelineCfg != nil {
		return !pipelineCfg.IsAggregateStep(stepName)
	}
	return true
}

// hydrateAndPersist reads the current slip state from ClickHouse (which invokes
// hydrateSlip to recompute all *_status columns from slip_component_states), then
// persists the hydrated row back via Update. This flushes correct *_status values
// for pipeline steps, whose AppendHistory path copies columns verbatim and would
// otherwise leave *_status stale until the next aggregate write.
//
// Read-your-own-writes overlay (I5 safety net):
// With ClickHouse async_insert=1 active server-side, the INSERT emitted by the
// preceding client call (StartStep / CompleteStep / FailStep / SkipStep) may not
// be visible to the SELECT inside Load's hydrateSlip. That would cause the
// just-written status to be overwritten with the pre-insert stale value, leaving
// routing_slips permanently stuck (I5 violation). To close this window, after
// Load returns we overlay the just-written step state directly into the in-memory
// slip before calling Update. This mirrors the overlayComponentState pattern from
// goLibMyCarrier PR #59, applied here to pipeline-level steps.
//
// Only pipeline-level steps are handled here (callers are guarded by
// isPipelineStep which filters out aggregate and component steps). The aggregate
// race is handled inside goLibMyCarrier by overlayComponentState in the
// updateAggregateStatusFromComponentStates* functions.
//
// Update is called directly on the store rather than through Client because Client
// does not expose an Update method. This intentionally bypasses any future
// Client-level hooks around Update; revisit if Client gains such hooks.
//
// Errors are returned to the caller but treated as non-fatal — the step event is
// already durably recorded in slip_component_states.
func (a *SlipWriterAdapter) hydrateAndPersist(
	ctx context.Context,
	correlationID, stepName string,
	status slippy.StepStatus,
	writtenAt time.Time,
) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.hydrateAndPersist",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.status", string(status)),
		),
	)
	defer span.End()

	slip, err := a.client.Load(ctx, correlationID)
	if err != nil {
		recordWriterError(span, err)
		return fmt.Errorf("hydrateAndPersist: load failed: %w", err)
	}

	// Read-your-own-writes overlay: merge the just-written step state into the
	// in-memory slip so Update writes the authoritative value even when the
	// async-insert buffer has not yet flushed. This directly addresses the I5
	// materialization race for pipeline-level steps (componentName == "").
	//
	// Strengthens I5: routing_slips.<step>_status == event-log-derived status.
	// Does not affect I1–I4 (those are enforced by checkPipelineCompletion inside
	// the library, which runs before this hydration path).
	overlayPipelineStep(slip, stepName, status, writtenAt)

	if err := a.client.Store().Update(ctx, slip); err != nil {
		recordWriterError(span, err)
		return fmt.Errorf("hydrateAndPersist: update failed: %w", err)
	}

	return nil
}

// overlayPipelineStep applies a just-written pipeline-step state to an in-memory
// slip, acting as a read-your-own-writes safety net for ClickHouse async-insert
// visibility lag.
//
// Only pipeline-level steps (componentName == "") are handled. Aggregate steps
// are handled inside goLibMyCarrier by overlayComponentState.
//
// Overlay rule: if the step exists in slip.Steps and the writtenAt timestamp is
// strictly after the existing CompletedAt (or CompletedAt is nil), the new status
// wins. This is the sentinel path from goLibMyCarrier's overlayComponentState
// (clickhouse_store.go) applied to slippy.Step rather than ComponentStepData.
//
// Note: ApplyStatusTransition sets CompletedAt only on the first terminal
// transition (when CompletedAt is nil). On a re-run path (failed → running →
// completed) where a prior CompletedAt exists, the Status is updated correctly
// but CompletedAt retains the prior run's timestamp. This is intentional and
// consistent with ComponentStepData.ApplyStatusTransition in goLibMyCarrier.
//
// Mirrors the sentinel-path logic of goLibMyCarrier's overlayComponentState
// (clickhouse_store.go:2233-2241). Duplication is intentional:
// overlayComponentState is an unexported function and cannot be called from
// this module. Both functions must stay in sync.
func overlayPipelineStep(slip *slippy.Slip, stepName string, status slippy.StepStatus, writtenAt time.Time) {
	if slip == nil {
		return
	}
	if slip.Steps == nil {
		return
	}
	step, ok := slip.Steps[stepName]
	if !ok {
		return
	}
	// Never overwrite a terminal status with a non-terminal one. This handles the
	// case where a StartStep arrives after a concurrent (or out-of-order re-trigger)
	// terminal event has already completed and become visible to Load(). Without
	// this guard, writtenAt = time.Now() would be after the past CompletedAt and
	// the overlay would clobber terminal → running, violating I5 and I2.
	if !status.IsTerminal() && step.CompletedAt != nil {
		return
	}
	if step.CompletedAt == nil || writtenAt.After(*step.CompletedAt) {
		step.ApplyStatusTransition(status, writtenAt)
		slip.Steps[stepName] = step
	}
}

// recordWriterError records an error on a span, distinguishing client errors
// from server/infrastructure errors.
func recordWriterError(span trace.Span, err error) {
	span.RecordError(err)
	switch {
	case isClientError(err):
		span.SetStatus(codes.Unset, err.Error())
	default:
		span.SetStatus(codes.Error, fmt.Sprintf("write operation failed: %v", err))
	}
}
