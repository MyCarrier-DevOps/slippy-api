package infrastructure

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
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

// defaultWriteOpTimeout is the default bound for a single ClickHouse write
// (insert + hydrate). Sized to be safely above observed CH async-insert
// wait (≈120 s) plus merge time (200–500 s), while remaining below the
// goLib CH max_execution_time (300 s set by the goLib fix). 240 s gives
// headroom for transient CH slowness without tying up the request indefinitely.
const defaultWriteOpTimeout = 240 * time.Second

// writeOpTimeout bounds a single ClickHouse write (insert + hydrate). The
// derived context detaches from the HTTP request ctx so a client disconnect
// or LB idle-timeout mid-request does not abort an in-flight write — the
// authoritative `slip_component_states` row must land regardless of whether
// the response makes it back to the caller. Span context is preserved (via
// context.WithoutCancel), only cancellation is decoupled.
//
// Override at runtime with SLIPPY_WRITE_OP_TIMEOUT (Go duration string,
// e.g. "300s"). Exposed as a var (not const) so tests can shorten it.
var writeOpTimeout = initWriteOpTimeout()

// initWriteOpTimeout reads SLIPPY_WRITE_OP_TIMEOUT from the environment.
// Valid values are Go duration strings (e.g. "240s", "5m"). On parse error
// the default is used and a warning is logged to stderr at startup.
func initWriteOpTimeout() time.Duration {
	if v := os.Getenv("SLIPPY_WRITE_OP_TIMEOUT"); v != "" {
		// Accept bare seconds as well as full duration strings.
		if secs, err := strconv.ParseFloat(v, 64); err == nil {
			return time.Duration(secs * float64(time.Second))
		}
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		// Warn but don't fatal — the server can still start with the default.
		slog.Warn("SLIPPY_WRITE_OP_TIMEOUT is not a valid duration; using default",
			"value", v,
			"default", defaultWriteOpTimeout,
		)
	}
	return defaultWriteOpTimeout
}

// writeContext returns a context for a single durable write: detached from
// the request ctx's cancellation signal, bounded by writeOpTimeout, otel
// span context preserved.
func writeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), writeOpTimeout)
}

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
		// CreateSlipForPush deliberately runs on the raw request ctx — do NOT
		// wrap in writeContext like the step/terminal writers below. A cancelled
		// create returns an error here, triggers the WithoutCancel-detached lock
		// release path below, and the webhook redelivery (pushhookparser is the
		// only in-process creator) re-acquires the lock and re-creates. The
		// awaitExistingSlip lock-miss path also relies on ctx.Done() to bound
		// itself against lockWait / lock TTL — detaching that would change dedup
		// semantics. Direct POST /slips callers should expect a one-shot
		// disconnect mid-create to lose the write with no recovery.
		result, err := a.client.CreateSlipForPush(ctx, opts)
		if err != nil {
			// Release on failure so a genuine retry can re-acquire and proceed.
			// Decouple the release from the request ctx: on client disconnect /
			// write-timeout the request ctx is cancelled, and go-redis short-circuits
			// Eval on a cancelled ctx, so the CAS-del would never run and the lock
			// would linger the full TTL — 409-ing legitimate retries of a slip that
			// was never created. context.WithoutCancel + a short timeout guarantees
			// the release attempt actually reaches Redis/Dragonfly.
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			if relErr := a.locker.Release(releaseCtx, key, token); relErr != nil {
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
	// SAME slip (idempotent) by polling LoadByCommitExact (exact-SHA lookup, no
	// ancestry resolution) until the in-flight slip becomes visible.
	span.AddEvent("dedup_duplicate_suppressed")
	return a.awaitExistingSlip(ctx, span, key, opts)
}

func (a *SlipWriterAdapter) StartStep(ctx context.Context, correlationID, stepName, componentName string) error {
	return a.instrumentedWrite(ctx, "writer.StartStep",
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		},
		func(wctx context.Context, span trace.Span) error {
			writtenAt := time.Now()
			if err := a.client.StartStep(wctx, correlationID, stepName, componentName); err != nil {
				return err
			}
			if a.isPipelineStep(stepName, componentName) {
				if err := a.hydrateAndPersist(
					wctx, correlationID, stepName, slippy.StepStatusRunning, writtenAt,
				); err != nil {
					span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
					slog.WarnContext(wctx, "writer: cache writeback failed (non-fatal); slip self-heals on next Load",
						"op", "StartStep",
						"correlation_id", correlationID,
						"step_name", stepName,
						"error", err,
					)
				}
			}
			return nil
		},
	)
}

func (a *SlipWriterAdapter) CompleteStep(ctx context.Context, correlationID, stepName, componentName string) error {
	return a.instrumentedWrite(ctx, "writer.CompleteStep",
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		},
		func(wctx context.Context, span trace.Span) error {
			// Pipeline-level terminal events route directly: steps.go:101 guard fires
			// checkPipelineCompletion automatically, saving a redundant Load.
			// Component events MUST go through RunPostExecution to drive aggregate recomputation.
			writtenAt := time.Now()
			if componentName != "" {
				if _, err := a.client.RunPostExecution(wctx, slippy.PostExecutionOptions{
					CorrelationID:     correlationID,
					StepName:          stepName,
					ComponentName:     componentName,
					WorkflowSucceeded: true,
				}); err != nil {
					return err
				}
			} else {
				if err := a.client.CompleteStep(wctx, correlationID, stepName, componentName); err != nil {
					return err
				}
			}
			if a.isPipelineStep(stepName, componentName) {
				if err := a.hydrateAndPersist(
					wctx, correlationID, stepName, slippy.StepStatusCompleted, writtenAt,
				); err != nil {
					span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
					slog.WarnContext(wctx, "writer: cache writeback failed (non-fatal); slip self-heals on next Load",
						"op", "CompleteStep",
						"correlation_id", correlationID,
						"step_name", stepName,
						"error", err,
					)
				}
			}
			return nil
		},
	)
}

func (a *SlipWriterAdapter) FailStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	return a.instrumentedWrite(ctx, "writer.FailStep",
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		},
		func(wctx context.Context, span trace.Span) error {
			// Pipeline-level terminal events route directly: steps.go:101 guard fires
			// checkPipelineCompletion automatically, saving a redundant Load.
			// Component events MUST go through RunPostExecution to drive aggregate recomputation.
			writtenAt := time.Now()
			if componentName != "" {
				if _, err := a.client.RunPostExecution(wctx, slippy.PostExecutionOptions{
					CorrelationID:     correlationID,
					StepName:          stepName,
					ComponentName:     componentName,
					WorkflowSucceeded: false,
					FailureMessage:    reason,
				}); err != nil {
					return err
				}
			} else {
				if err := a.client.FailStep(wctx, correlationID, stepName, componentName, reason); err != nil {
					return err
				}
			}
			if a.isPipelineStep(stepName, componentName) {
				if err := a.hydrateAndPersist(
					wctx, correlationID, stepName, slippy.StepStatusFailed, writtenAt,
				); err != nil {
					span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
					slog.WarnContext(wctx, "writer: cache writeback failed (non-fatal); slip self-heals on next Load",
						"op", "FailStep",
						"correlation_id", correlationID,
						"step_name", stepName,
						"error", err,
					)
				}
			}
			return nil
		},
	)
}

func (a *SlipWriterAdapter) SkipStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	return a.instrumentedWrite(ctx, "writer.SkipStep",
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		},
		func(wctx context.Context, span trace.Span) error {
			writtenAt := time.Now()
			if err := a.client.SkipStep(wctx, correlationID, stepName, componentName, reason); err != nil {
				return err
			}
			if a.isPipelineStep(stepName, componentName) {
				if err := a.hydrateAndPersist(
					wctx, correlationID, stepName, slippy.StepStatusSkipped, writtenAt,
				); err != nil {
					span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
					slog.WarnContext(wctx, "writer: cache writeback failed (non-fatal); slip self-heals on next Load",
						"op", "SkipStep",
						"correlation_id", correlationID,
						"step_name", stepName,
						"error", err,
					)
				}
			}
			return nil
		},
	)
}

func (a *SlipWriterAdapter) SetComponentImageTag(
	ctx context.Context,
	correlationID, componentName, imageTag string,
) error {
	return a.instrumentedWrite(ctx, "writer.SetComponentImageTag",
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.component_name", componentName),
			attribute.String("slip.image_tag", imageTag),
		},
		func(wctx context.Context, _ trace.Span) error {
			return a.client.SetComponentImageTag(wctx, correlationID, componentName, imageTag)
		},
	)
}

func (a *SlipWriterAdapter) PromoteSlip(ctx context.Context, correlationID, promotedTo string) error {
	return a.instrumentedWrite(ctx, "writer.PromoteSlip",
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.promoted_to", promotedTo),
		},
		func(wctx context.Context, _ trace.Span) error {
			return a.client.PromoteSlip(wctx, correlationID, promotedTo)
		},
	)
}

func (a *SlipWriterAdapter) AbandonSlip(ctx context.Context, correlationID, supersededBy string) error {
	return a.instrumentedWrite(ctx, "writer.AbandonSlip",
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.superseded_by", supersededBy),
		},
		func(wctx context.Context, _ trace.Span) error {
			return a.client.AbandonSlip(wctx, correlationID, supersededBy)
		},
	)
}

// instrumentedWrite is the single entry point for durable step/terminal
// writes. It starts a tracer span, derives a cancellation-detached write
// context via writeContext, and invokes op with that ctx and the span.
// All adapter methods that mutate slip state route through here, so the
// WithoutCancel + writeOpTimeout guarantee can't be silently dropped by a
// future method that forgets the wrap — adding a new write method without
// this helper is the only way to lose the guarantee, and that omission is
// loud in review.
//
// The closure receives only wctx for use with the upstream client and
// hydrateAndPersist — the caller-supplied ctx is exclusively for span
// scoping. Do NOT pass the outer ctx to client.* calls; that would defeat
// the point of this indirection.
func (a *SlipWriterAdapter) instrumentedWrite(
	ctx context.Context,
	spanName string,
	attrs []attribute.KeyValue,
	op func(wctx context.Context, span trace.Span) error,
) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
	defer span.End()

	wctx, cancel := writeContext(ctx)
	defer cancel()

	if err := op(wctx, span); err != nil {
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
		return nil, fmt.Errorf("dedup: slip for %s creation in progress, retry: %w", key, domain.ErrCreationInProgress)
	}

	deadline := time.Now().Add(a.lockWait)
	// Start small (50ms) so the common near-simultaneous-duplicate case — where the
	// winner's slip becomes visible almost immediately — resolves with minimal added
	// latency. The backoff still doubles up to maxBackoff for the slower async-insert
	// visibility window.
	backoff := 50 * time.Millisecond
	const maxBackoff = time.Second
	attempts := 0

	for {
		attempts++
		existing, err := a.reader.LoadByCommitExact(ctx, opts.Repository, opts.CommitSHA)
		// Deliberate choice: a TERMINAL existing slip for this (repo, sha) is NOT
		// returned as an in-flight result. Returning a finished slip as if creation
		// were still "in progress" would be misleading, and a genuinely new push for
		// the same sha should not silently alias an old terminal slip. The duplicate
		// instead falls through to ErrCreationInProgress (→ HTTP 409); this self-heals
		// once the dedup lock TTL expires and the next attempt re-acquires the lock.
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
	//
	// Observability: WARN log on deadline-exhaust acts as the early-warning signal
	// for a class-of-bug regression (e.g. winner crashing pre-insert, async-insert
	// window blown past lockWait, or ancestry-resolution drift causing the await
	// path to miss a slip that does exist). If this fires steadily, investigate
	// before adjusting lockWait — increasing the wait masks the underlying issue.
	deadlineMs := a.lockWait.Milliseconds()
	span.AddEvent("dedup_wait_timeout",
		trace.WithAttributes(
			attribute.Int("dedup.attempts", attempts),
			attribute.Int64("dedup.deadline_ms", deadlineMs),
		),
	)
	slog.WarnContext(ctx, "dedup: awaitExistingSlip deadline exhausted",
		"correlation_id", opts.CorrelationID,
		"repository", opts.Repository,
		"commit_sha", opts.CommitSHA,
		"deadline_ms", deadlineMs,
		"attempts", attempts,
		"dedup_key", key,
	)
	return nil, fmt.Errorf("dedup: slip for %s creation in progress, retry: %w", key, domain.ErrCreationInProgress)
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
