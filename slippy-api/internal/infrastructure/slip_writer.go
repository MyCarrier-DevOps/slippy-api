package infrastructure

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// slippyI5LockEnabledEnv gates per-correlationID lock acquisition for the
// adapter's mutating methods. Plan v3 §G.1 mandates the flag default OFF so
// PR 2 (this code) ships without behavior change, then is enabled in a
// staged rollout only AFTER PR 3 (Slippy CLI 409 retry-with-jitter) lands —
// otherwise legitimate same-correlationID contention surfaces as workflow
// step failures with no Argo retry coverage (plan v3 §M.7).
const slippyI5LockEnabledEnv = "SLIPPY_I5_LOCK_ENABLED"

// corrIDLockEnabled reports whether the per-correlationID write lock should be
// applied for this process. The value is sampled at adapter construction (NOT
// every call) so flipping the env var at runtime has no effect — operators
// must restart the pod after toggling. This is intentional: a mid-flight
// flip mixed with already-acquired locks would create a confused state.
func corrIDLockEnabled() bool {
	switch os.Getenv(slippyI5LockEnabledEnv) {
	case "1", "true", "TRUE", "True":
		return true
	}
	return false
}

// writerTracerName is the instrumentation scope for write operations.
const writerTracerName = "slippy-api/writer"

// SlipWriterAdapter adapts the upstream slippy.Client to the domain.SlipWriter
// interface. It wraps the high-level business client (not the raw store) so that
// operations like CreateSlipForPush include ancestry resolution and step updates
// include atomic history appends.
type SlipWriterAdapter struct {
	client *slippy.Client
	// locker serializes BOTH CreateSlipForPush across processes on a repo:sha
	// key (prevents phantom-slip duplicates from GitHub push webhooks) AND step
	// mutations across processes on a sliplock:cid:<corrID> key (prevents
	// concurrent Load → mutate → Update races that materialize the I5 bug).
	// A nil locker disables BOTH lock paths (cache disabled / ping failed)
	// and preserves the original lock-free behavior.
	locker Locker
	// reader is used on the dedup lock-miss path to poll for the in-flight slip
	// so a suppressed duplicate returns the SAME slip (true idempotency). Only
	// consulted when locker is non-nil and the lock was not acquired.
	reader domain.SlipReader
	// lockTTL / lockWait tune the repo:sha dedup lock. Zero values fall back to
	// defaults. The per-correlationID lock uses its own (shorter) TTL constants.
	lockTTL  time.Duration
	lockWait time.Duration
	// corrIDLockOn captures the SLIPPY_I5_LOCK_ENABLED state at adapter
	// construction. Sampled once to avoid mid-flight flag flips producing a
	// confused state (some calls under the lock, some not).
	corrIDLockOn bool
	// corrIDLockTTL bounds the per-correlationID lock hold. Distinct from
	// lockTTL because the corr-id lock guards the Load+mutate+Update path
	// (~5–30 ms typical) whereas the repo:sha lock spans the full
	// CreateSlipForPush including ancestry resolution and async-insert
	// visibility waits.
	corrIDLockTTL time.Duration
	// log is the structured logger used for fail-open warnings (corr-id lock
	// acquire/release failures, observability seam for the LatestStepStatus
	// query failure). Defaults to slog.Default() when not injected.
	log *slog.Logger
}

// NewSlipWriterAdapter wraps a slippy.Client as a SlipWriter.
//
// locker and reader enable cross-process slip-creation deduplication AND the
// per-correlationID write lock (plan v3 §M). Pass a nil locker to disable both
// lock paths (the original behavior) — for example when the Dragonfly/Redis
// cache is not configured or its startup ping failed (fail-open). reader is
// only consulted on the dedup lock-miss path; it may be the cache-decorated
// reader so the poll observes committed rows.
//
// SLIPPY_I5_LOCK_ENABLED is sampled once here and persisted on the adapter.
// Defaults OFF (plan v3 §G.1) — production enablement BLOCKED until PR 3
// (Slippy CLI 409 retry-with-jitter) lands, per plan v3 §M.7. Until then,
// turning the flag on would expose legitimate lock-contention as workflow
// step failures with no Argo retry coverage.
func NewSlipWriterAdapter(client *slippy.Client, locker Locker, reader domain.SlipReader) *SlipWriterAdapter {
	return &SlipWriterAdapter{
		client:        client,
		locker:        locker,
		reader:        reader,
		lockTTL:       DefaultLockTTL,
		lockWait:      DefaultLockWait,
		corrIDLockOn:  corrIDLockEnabled(),
		corrIDLockTTL: DefaultCorrIDLockTTL,
		log:           slog.Default(),
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
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.StartStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	return a.withCorrIDLock(ctx, correlationID, span, func(ctx context.Context) error {
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
	})
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

	return a.withCorrIDLock(ctx, correlationID, span, func(ctx context.Context) error {
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
	})
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

	return a.withCorrIDLock(ctx, correlationID, span, func(ctx context.Context) error {
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
	})
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

	return a.withCorrIDLock(ctx, correlationID, span, func(ctx context.Context) error {
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
	})
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

// PromoteSlip is wrapped by the per-correlationID lock (plan v3 §C.10.1, Mod 2)
// because client.go:182 PromoteSlip executes store.Update(slip) with the full
// slip including step columns — the same Load → mutate → Update race that
// motivated the lock for StartStep/CompleteStep applies here. Pod-A promoting
// could overwrite Pod-B's concurrent valid CompleteStep otherwise.
func (a *SlipWriterAdapter) PromoteSlip(ctx context.Context, correlationID, promotedTo string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.PromoteSlip",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.promoted_to", promotedTo),
		),
	)
	defer span.End()

	return a.withCorrIDLock(ctx, correlationID, span, func(ctx context.Context) error {
		if err := a.client.PromoteSlip(ctx, correlationID, promotedTo); err != nil {
			recordWriterError(span, err)
			return err
		}
		return nil
	})
}

// AbandonSlip is wrapped by the per-correlationID lock for the same reason as
// PromoteSlip — client.go:217 AbandonSlip also performs store.Update with the
// full slip (plan v3 §C.10.1, Mod 2).
func (a *SlipWriterAdapter) AbandonSlip(ctx context.Context, correlationID, supersededBy string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.AbandonSlip",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.superseded_by", supersededBy),
		),
	)
	defer span.End()

	return a.withCorrIDLock(ctx, correlationID, span, func(ctx context.Context) error {
		if err := a.client.AbandonSlip(ctx, correlationID, supersededBy); err != nil {
			recordWriterError(span, err)
			return err
		}
		return nil
	})
}

// withCorrIDLock serializes mutating operations on a single correlationID
// across processes by acquiring a Redis lock keyed on the corrID. This is the
// second half of the I5 fix (plan v3 §M): the goLib INSERT-time gate closes
// the slip_component_states event-log race, the per-corrID lock closes the
// routing_slips aggregate-write-back race (Pod-A Load → Pod-B Update overwrites
// Pod-A's mutation).
//
// Contract & invariants:
//   - Nil locker → fail-open, behave exactly as before the lock was added.
//     This matches the dedup-lock contract and keeps CI uptime independent of
//     cache uptime (plan v3 §C.10).
//   - SLIPPY_I5_LOCK_ENABLED=false (default) → fail-open. Until PR 3 (Slippy
//     CLI 409 retry) lands, enabling the lock would surface contention as
//     workflow failures (plan v3 §M.7, §G.1).
//   - Invalid correlationID (non-UUID) → ErrInvalidCorrelationID. Validation
//     defense-in-depth on top of the handler-boundary check (plan v3 §M.1.2,
//     Mod 5).
//   - TryAcquire failure (Redis transport error) → fail-open with WARN log;
//     never block the request on cache outage (matches RedisLocker pattern).
//     Plan v3 MISS-V2-2/MISS-V2-3 closure: ctx propagates into TryAcquire so
//     client cancellation aborts the acquire; release uses
//     context.WithoutCancel + 2s timeout so a cancelled request still releases.
//   - acquired=false → ErrCorrIDWriteInProgress (HTTP 409 at handler boundary).
//     CLI retries with backoff. No internal poll-wait (caller owns the retry).
//   - Successful acquire → fn(ctx) runs under the lock; release runs in a defer
//     with a fresh context so request cancellation cannot leak ghost locks.
//
// Rollback path (plan v3 §G.1 pre-flip checklist for PR 3): set
// SLIPPY_I5_LOCK_ENABLED=false on the deployment, restart pods. All adapter
// calls fall through unlocked immediately; no state migration required.
func (a *SlipWriterAdapter) withCorrIDLock(
	ctx context.Context,
	correlationID string,
	span trace.Span,
	fn func(ctx context.Context) error,
) error {
	// Feature flag OFF or nil locker → behave exactly as pre-lock code path.
	if !a.corrIDLockOn || a.locker == nil {
		return fn(ctx)
	}

	key := CorrIDLockKey(correlationID)
	if key == "" {
		// Defense in depth — handler boundary should have rejected this.
		return domain.ErrInvalidCorrelationID
	}
	span.SetAttributes(attribute.String("corrid_lock.key", key))

	acquired, token, lockErr := a.locker.TryAcquire(ctx, key, a.corrIDLockTTL)
	if lockErr != nil {
		// Fail-open: cache outage MUST NOT block CI writes. Mirrors the
		// dedup-lock pattern. Surface via WARN log + span event so a
		// degraded-cache window is observable in production telemetry.
		span.AddEvent("corrid_lock_unavailable",
			trace.WithAttributes(attribute.String("error", lockErr.Error())))
		a.log.WarnContext(ctx,
			"corr-id lock acquire failed; proceeding without lock (fail-open)",
			"correlation_id", correlationID, "key", key, "error", lockErr)
		return fn(ctx)
	}
	if !acquired {
		// Lock held by a concurrent writer. The handler maps this to 409.
		span.AddEvent("corrid_lock_held")
		return domain.ErrCorrIDWriteInProgress
	}

	span.AddEvent("corrid_lock_acquired")
	// Release MUST run even on context cancellation — otherwise a cancelled
	// request leaves the lock pinned for the full TTL, 409-ing legitimate
	// retries. context.WithoutCancel + bounded timeout keeps the release
	// reachable while still bounding goroutine accumulation under sustained
	// cache degradation (plan v3 §M.3 MISS-V2-3 callout).
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if relErr := a.locker.Release(releaseCtx, key, token); relErr != nil {
			span.AddEvent("corrid_lock_release_failed",
				trace.WithAttributes(attribute.String("error", relErr.Error())))
			a.log.WarnContext(ctx,
				"corr-id lock release failed; TTL will drain",
				"correlation_id", correlationID, "key", key, "error", relErr)
		}
	}()

	return fn(ctx)
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
	//
	// R1 (ADO #82468): the terminal-wins guard inside overlayPipelineStep no
	// longer relies on the (potentially stale) in-memory CompletedAt. Instead it
	// consults the event log via store.LatestStepStatusFromEvents — guaranteed
	// to reflect just-written truth because the preceding library write was
	// synchronous under wait_for_async_insert=1 (asserted at startup; see
	// clickhouse_assertions.go).
	// latestFn is the R1 event-log seam (ADO #82468). Wrapping the raw store call
	// gives us a single place to emit fail-open observability when the query
	// errors — the overlay falls back to the in-memory CompletedAt guard, but
	// production needs to KNOW when that fallback fired so a steady stream of
	// query failures (which would silently degrade I5 protection back to the
	// pre-R1 in-memory guard) can be alerted on. Pattern modeled after the
	// dedup_wait_timeout WARN+span event below.
	latestFn := func() (slippy.StepStatus, bool, error) {
		status, found, err := a.client.Store().LatestStepStatusFromEvents(ctx, correlationID, stepName)
		if err != nil {
			span.AddEvent("r1.event_log_query_failed",
				trace.WithAttributes(attribute.String("error", err.Error())))
			a.log.WarnContext(ctx,
				"r1: LatestStepStatusFromEvents query failed; overlay guard falls back to in-memory CompletedAt",
				"correlation_id", correlationID, "step", stepName, "error", err)
		}
		return status, found, err
	}
	applied := overlayPipelineStep(slip, stepName, status, writtenAt, latestFn)

	// R2 Option D: when the overlay applied (caller's status is the authoritative
	// truth for THIS step), pin the routing_slips.<step>_status column and the
	// step_details.<step>.status JSON value to the caller-supplied status via
	// StepStatusOverride. When the overlay was dropped (event log already
	// terminal, caller wrote non-terminal), pass no override so Update falls back
	// to slip.Steps[name].Status — which still reflects event-log truth from
	// Load because overlay was skipped without mutation.
	var overrides []slippy.StepStatusOverride
	if applied {
		overrides = []slippy.StepStatusOverride{
			{ColumnName: slippy.StepStatusColumnName(stepName), Status: status},
		}
	}
	if err := a.client.Store().Update(ctx, slip, overrides...); err != nil {
		recordWriterError(span, err)
		return fmt.Errorf("hydrateAndPersist: update failed: %w", err)
	}

	return nil
}

// latestStepStatusFn is the callback seam used by overlayPipelineStep to consult
// the event log (slip_component_states) for the authoritative latest status of
// the step being written. Returning (status, true, nil) signals an event row
// exists; ("", false, nil) signals no events yet and the in-memory guard remains
// in effect; (_, _, err) signals a query failure and triggers the fail-open
// fallback to the in-memory guard.
type latestStepStatusFn func() (slippy.StepStatus, bool, error)

// overlayPipelineStep applies a just-written pipeline-step state to an in-memory
// slip, acting as a read-your-own-writes safety net for ClickHouse async-insert
// visibility lag. Returns true when ApplyStatusTransition was called (the
// caller's status WON), false when the overlay was dropped (event log already
// holds a terminal status that must not be clobbered by a non-terminal write).
//
// Only pipeline-level steps (componentName == "") are handled. Aggregate steps
// are handled inside goLibMyCarrier by overlayComponentState.
//
// Role under Option 1 (plan v3 §C.3, defense-in-depth):
//
// With the Option 1 INSERT-time gate (enforceTerminalMonotonicity in
// goLibMyCarrier slippy/clickhouse_store.go) in place, the FIRST line of
// defense against terminal regression now lives upstream: UpdateStep and
// UpdateStepWithHistory pre-flight a same-row argMax SELECT and refuse the
// INSERT with ErrTerminalAlreadyExists if the incoming status would violate
// the §D matrix. The adapter sees the sentinel, mapWriteError returns 409,
// and the bad transition never reaches the event log at all.
//
// This overlay remains active as the SECOND line of defense because the gate
// SELECT and the INSERT are not atomic in ClickHouse — two same-microsecond
// concurrent writers can both observe an empty event log, both INSERT, and
// only post-hoc reconciliation (argMax tiebreak + this overlay) decides which
// status materializes in routing_slips. Keeping the overlay closes that
// residual window without depending on flag state.
//
// Rollback note (plan v3 §G.1): if the Option 1 gate is rolled back via
// SLIPPY_I5_GATE_ENABLED=false on the goLib side, this overlay is the SOLE
// I5 protection again — same as the pre-Option-1 baseline. Do not delete it
// without ratifying §G.1 rollback. Pre-flip checklist for PR 3 (Slippy CLI
// 409 retry-with-jitter): the per-correlationID lock can only ship enabled
// AFTER PR 3 lands; until then, this overlay + the gate close I5 and the
// lock defaults OFF (plan v3 §M.7 / §F.3 measurement gate).
//
// R1 terminal-wins guard (ADO #82468): the guard consults the event log via
// latestStatus rather than the in-memory CompletedAt. This eliminates the
// 436cc68c-style failure mode where Load returned a stale snapshot whose
// CompletedAt did not yet reflect a just-written terminal event. The event log
// row is durable under wait_for_async_insert=1 (asserted at startup), so the
// SELECT is guaranteed to observe it.
//
// Fail-open policy: if latestStatus returns an error, fall through to the
// existing in-memory guard. If latestStatus reports !found (no events for this
// step yet), the overlay applies — this preserves first-event behaviour.
//
// Overlay rule once the guard has cleared: writtenAt-vs-CompletedAt monotonicity
// is preserved (the new status wins iff CompletedAt is nil or writtenAt is
// strictly after it). ApplyStatusTransition sets CompletedAt only on the first
// terminal transition.
//
// Mirrors the sentinel-path logic of goLibMyCarrier's overlayComponentState.
// Duplication is intentional: overlayComponentState is an unexported function
// and cannot be called from this module. Both functions must stay in sync.
func overlayPipelineStep(
	slip *slippy.Slip,
	stepName string,
	status slippy.StepStatus,
	writtenAt time.Time,
	latestStatus latestStepStatusFn,
) (applied bool) {
	if slip == nil {
		return false
	}
	if slip.Steps == nil {
		return false
	}
	step, ok := slip.Steps[stepName]
	if !ok {
		return false
	}

	// R1: consult the event log. Three branches:
	//   err != nil → fail-open; fall through to in-memory CompletedAt guard.
	//   !found     → no event rows yet → apply overlay (first-event).
	//   found      → if event-log terminal AND caller non-terminal → DROP.
	if latestStatus != nil {
		eventStatus, found, err := latestStatus()
		switch {
		case err != nil:
			// fail-open: rely on the in-memory CompletedAt guard below
		case !found:
			// no events yet — overlay applies as first transition
		case eventStatus.IsTerminal() && !status.IsTerminal():
			// the I5 fix: event log says terminal, caller is writing non-terminal.
			// Drop the overlay so the caller-side R2 logic also skips the override.
			return false
		}
	}

	// Defensive in-memory guard (fail-open fallback when latestStatus errored or
	// is nil, e.g. in older unit-test fixtures that have not been migrated yet).
	if !status.IsTerminal() && step.CompletedAt != nil {
		return false
	}
	if step.CompletedAt == nil || writtenAt.After(*step.CompletedAt) {
		step.ApplyStatusTransition(status, writtenAt)
		slip.Steps[stepName] = step
		return true
	}
	return false
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
