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

// slippyI5LockEnabledEnv gates per-correlationID lock acquisition for the
// adapter's mutating methods. Plan v3 §G.1 mandates the flag default OFF so
// PR 2 (this code) ships without behavior change, then is enabled in a
// staged rollout only AFTER PR 3 (Slippy CLI 409 retry-with-jitter) lands —
// otherwise legitimate same-correlationID contention surfaces as workflow
// step failures with no Argo retry coverage (plan v3 §M.7).
const slippyI5LockEnabledEnv = "SLIPPY_I5_LOCK_ENABLED"

// ParseI5LockFlag reads SLIPPY_I5_LOCK_ENABLED from the process environment and
// returns whether the per-correlationID write lock should be active for this
// adapter. Pure parser — no side effects. Composition root (main.go) MUST call
// this once at startup, log the resulting state, and pass the bool into
// NewSlipWriterAdapter for dependency injection.
//
// Splitting the side-effect (the slog.Info) out of the constructor keeps the
// adapter SRP-clean (no env or logging coupling at construction) and lets
// tests instantiate adapters in parallel without t.Setenv races.
//
// Parsing uses strconv.ParseBool so the accepted truthy/falsy set is the
// canonical Go convention (1/t/T/TRUE/true/True/0/f/F/FALSE/false/False).
// Any other value (including empty) is treated as OFF — the plan v3 §G.1
// default.
func ParseI5LockFlag() (enabled bool, raw string) {
	raw = os.Getenv(slippyI5LockEnabledEnv)
	v, err := strconv.ParseBool(raw)
	return err == nil && v, raw
}

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

// minWriteOpTimeout is the minimum accepted value for SLIPPY_WRITE_OP_TIMEOUT.
// Any parsed value below this floor (including 0 and negatives) is rejected:
// a zero-or-negative timeout makes context.WithTimeout expire instantly,
// causing every write to fail before the ClickHouse driver even sends the query.
const minWriteOpTimeout = 1 * time.Second

// maxWriteOpTimeout is the ceiling for SLIPPY_WRITE_OP_TIMEOUT. Values above
// this are almost certainly a misconfiguration (e.g. unit confusion); cap them
// to keep requests from hanging indefinitely.
const maxWriteOpTimeout = 600 * time.Second

// initWriteOpTimeout reads SLIPPY_WRITE_OP_TIMEOUT from the environment.
// Valid values are Go duration strings (e.g. "240s", "5m") or bare seconds.
// On parse error, or when the parsed value is outside [minWriteOpTimeout,
// maxWriteOpTimeout], the default is used and a warning is logged at startup.
func initWriteOpTimeout() time.Duration {
	if v := os.Getenv("SLIPPY_WRITE_OP_TIMEOUT"); v != "" {
		var d time.Duration
		parsed := false
		// Accept bare seconds as well as full duration strings.
		if secs, err := strconv.ParseFloat(v, 64); err == nil {
			d = time.Duration(secs * float64(time.Second))
			parsed = true
		} else if dur, err := time.ParseDuration(v); err == nil {
			d = dur
			parsed = true
		}

		if !parsed {
			// Warn but don't fatal — the server can still start with the default.
			slog.Warn("SLIPPY_WRITE_OP_TIMEOUT is not a valid duration; using default",
				"value", v,
				"default", defaultWriteOpTimeout,
			)
			return defaultWriteOpTimeout
		}

		if d < minWriteOpTimeout {
			slog.Warn("SLIPPY_WRITE_OP_TIMEOUT is below minimum floor; using default",
				"value", v,
				"parsed", d,
				"floor", minWriteOpTimeout,
				"default", defaultWriteOpTimeout,
			)
			return defaultWriteOpTimeout
		}

		if d > maxWriteOpTimeout {
			slog.Warn("SLIPPY_WRITE_OP_TIMEOUT exceeds maximum ceiling; using default",
				"value", v,
				"parsed", d,
				"ceiling", maxWriteOpTimeout,
				"default", defaultWriteOpTimeout,
			)
			return defaultWriteOpTimeout
		}

		return d
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
// i5LockEnabled is the resolved SLIPPY_I5_LOCK_ENABLED state. The composition
// root (main.go) MUST compute it once via ParseI5LockFlag and log it once
// alongside the other startup banner lines, then pass it here. This DI seam
// keeps the constructor free of env / logging side effects so tests can run
// in parallel without t.Setenv races.
//
// Defaults OFF (plan v3 §G.1) — production enablement BLOCKED until PR 3
// (Slippy CLI 409 retry-with-jitter) lands, per plan v3 §M.7. Until then,
// turning the flag on would expose legitimate lock-contention as workflow
// step failures with no Argo retry coverage.
func NewSlipWriterAdapter(
	client *slippy.Client,
	locker Locker,
	reader domain.SlipReader,
	i5LockEnabled bool,
) *SlipWriterAdapter {
	return &SlipWriterAdapter{
		client:        client,
		locker:        locker,
		reader:        reader,
		lockTTL:       DefaultLockTTL,
		lockWait:      DefaultLockWait,
		corrIDLockOn:  i5LockEnabled,
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
	return a.instrumentedWrite(ctx, "writer.StartStep", correlationID,
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
					// STATUS self-heals on next Load (hydrateSlip recomputes from event log).
					// state_history is NOT reconstructed — this transition's audit entry is lost.
					slog.WarnContext(
						wctx,
						"writer: cache writeback failed (non-fatal); status self-heals, state_history lost",
						"op",
						"StartStep",
						"correlation_id",
						correlationID,
						"step_name",
						stepName,
						"error",
						err,
					)
				}
			}
			return nil
		},
	)
}

func (a *SlipWriterAdapter) CompleteStep(ctx context.Context, correlationID, stepName, componentName string) error {
	return a.instrumentedWrite(ctx, "writer.CompleteStep", correlationID,
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
					// STATUS self-heals on next Load (hydrateSlip recomputes from event log).
					// state_history is NOT reconstructed — this transition's audit entry is lost.
					slog.WarnContext(
						wctx,
						"writer: cache writeback failed (non-fatal); status self-heals, state_history lost",
						"op",
						"CompleteStep",
						"correlation_id",
						correlationID,
						"step_name",
						stepName,
						"error",
						err,
					)
				}
			}
			return nil
		},
	)
}

func (a *SlipWriterAdapter) FailStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	return a.instrumentedWrite(ctx, "writer.FailStep", correlationID,
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
					// STATUS self-heals on next Load (hydrateSlip recomputes from event log).
					// state_history is NOT reconstructed — this transition's audit entry is lost.
					slog.WarnContext(
						wctx,
						"writer: cache writeback failed (non-fatal); status self-heals, state_history lost",
						"op",
						"FailStep",
						"correlation_id",
						correlationID,
						"step_name",
						stepName,
						"error",
						err,
					)
				}
			}
			return nil
		},
	)
}

func (a *SlipWriterAdapter) SkipStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	return a.instrumentedWrite(ctx, "writer.SkipStep", correlationID,
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
					// STATUS self-heals on next Load (hydrateSlip recomputes from event log).
					// state_history is NOT reconstructed — this transition's audit entry is lost.
					slog.WarnContext(
						wctx,
						"writer: cache writeback failed (non-fatal); status self-heals, state_history lost",
						"op",
						"SkipStep",
						"correlation_id",
						correlationID,
						"step_name",
						stepName,
						"error",
						err,
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
	return a.instrumentedWrite(ctx, "writer.SetComponentImageTag", correlationID,
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

// PromoteSlip is wrapped by the per-correlationID lock (plan v3 §C.10.1, Mod 2)
// because client.go:182 PromoteSlip executes store.Update(slip) with the full
// slip including step columns — the same Load → mutate → Update race that
// motivated the lock for StartStep/CompleteStep applies here. Pod-A promoting
// could overwrite Pod-B's concurrent valid CompleteStep otherwise.
func (a *SlipWriterAdapter) PromoteSlip(ctx context.Context, correlationID, promotedTo string) error {
	return a.instrumentedWrite(ctx, "writer.PromoteSlip", correlationID,
		[]attribute.KeyValue{
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.promoted_to", promotedTo),
		},
		func(wctx context.Context, _ trace.Span) error {
			return a.client.PromoteSlip(wctx, correlationID, promotedTo)
		},
	)
}

// AbandonSlip is wrapped by the per-correlationID lock for the same reason as
// PromoteSlip — client.go:217 AbandonSlip also performs store.Update with the
// full slip (plan v3 §C.10.1, Mod 2).
func (a *SlipWriterAdapter) AbandonSlip(ctx context.Context, correlationID, supersededBy string) error {
	return a.instrumentedWrite(ctx, "writer.AbandonSlip", correlationID,
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
// context via writeContext, applies the per-correlationID lock (I5 R1 fix —
// plan v3 §M), and invokes op with that ctx and the span. All adapter
// methods that mutate slip state route through here, so the WithoutCancel
// + writeOpTimeout + corrID-lock guarantees can't be silently dropped by a
// future method that forgets the wrap — adding a new write method without
// this helper is the only way to lose the guarantees, and that omission is
// loud in review.
//
// correlationID may be empty for writes that don't target a specific slip
// (none currently — all step/terminal/dedup writes have one). When empty,
// the lock is bypassed; the handler-boundary validator
// handler.validateCorrelationID (slip_write_handler.go, added in commit
// f101d77 per PR #39 review) is the first line of defense and rejects
// malformed UUIDs with HTTP 400 before any write path runs. This bypass
// keeps the adapter resilient if a future non-HTTP caller forwards an
// empty corrID — the WARN log below makes the regression observable.
//
// The closure receives only wctx for use with the upstream client and
// hydrateAndPersist — the caller-supplied ctx is exclusively for span
// scoping. Do NOT pass the outer ctx to client.* calls; that would defeat
// the point of this indirection.
func (a *SlipWriterAdapter) instrumentedWrite(
	ctx context.Context,
	spanName string,
	correlationID string,
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

	runOp := func(opCtx context.Context) error {
		return op(opCtx, span)
	}

	var err error
	if correlationID == "" {
		// Defense-in-depth: handler boundary must reject empty/invalid
		// correlation IDs (see slip_write_handler UUID validation). If we
		// land here with the lock flag ON it means a write path bypassed
		// that gate — log a WARN so the regression is observable, then
		// fall through unlocked (matches pre-lock behavior).
		if a.corrIDLockOn {
			a.log.WarnContext(ctx, "I5_lock_skipped_empty_corrID",
				slog.String("span", spanName),
			)
		}
		err = runOp(wctx)
	} else {
		err = a.withCorrIDLock(wctx, correlationID, span, runOp)
	}
	if err != nil {
		recordWriterError(span, err)
		return err
	}
	return nil
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
		// Layered with handler.validateCorrelationID (slip_write_handler.go,
		// added in commit f101d77 per PR #39 review) — the handler rejects
		// malformed UUIDs with HTTP 400 before any write path runs. This
		// adapter-side check is the residual safety net for non-HTTP
		// callers (e.g. dedup-driven internal flows).
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
	applied, resolved := overlayPipelineStep(slip, stepName, status, writtenAt, latestFn)

	// R2 Option D: when the overlay applied, pin the routing_slips.<step>_status
	// column and the step_details.<step>.status JSON value to the RESOLVED
	// status (NOT the original caller-supplied status). resolved equals the
	// caller's status in the common case; in the divergent-terminal race
	// (R2 PR #39: both writers terminal but different), resolved is the
	// event-log status — the argMax winner — so the *_status column stays
	// aligned with the materialized event-log row.
	//
	// When the overlay was dropped (event log already terminal, caller wrote
	// non-terminal), pass no override so Update falls back to
	// slip.Steps[name].Status — which still reflects event-log truth from
	// Load because overlay was skipped without mutation.
	var overrides []slippy.StepStatusOverride
	if applied {
		overrides = []slippy.StepStatusOverride{
			{ColumnName: slippy.StepStatusColumnName(stepName), Status: resolved},
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
// Returns (applied, resolved):
//   - applied: true when ApplyStatusTransition ran and the step in slip.Steps
//     reflects the just-written truth.
//   - resolved: the status that was actually pinned. Equals the caller's
//     `status` argument except in the divergent-terminal branch, where it
//     equals the event-log status (the argMax winner). Callers MUST use
//     resolved (NOT their original `status`) when building the
//     StepStatusOverride for the *_status column, otherwise the column
//     drifts from argMax in the divergent-terminal race window.
//   - When applied=false, resolved is unspecified; callers MUST NOT use it.
func overlayPipelineStep(
	slip *slippy.Slip,
	stepName string,
	status slippy.StepStatus,
	writtenAt time.Time,
	latestStatus latestStepStatusFn,
) (applied bool, resolved slippy.StepStatus) {
	if slip == nil {
		return false, ""
	}
	if slip.Steps == nil {
		return false, ""
	}
	step, ok := slip.Steps[stepName]
	if !ok {
		return false, ""
	}

	// R1: consult the event log. Four branches:
	//   err != nil → fail-open; fall through to in-memory CompletedAt guard.
	//   !found     → no event rows yet → apply overlay (first-event).
	//   found      → if event-log terminal AND caller non-terminal → DROP.
	//   found      → if BOTH terminal but DIFFER → pin event-log truth (R2 PR #39).
	//
	// The both-terminal-divergence branch closes a same-µs race where two
	// terminal writers (e.g. completed vs failed) race the argMax tiebreak
	// (`timestamp_micros*100 + toUInt8(status)`). The argMax resolves to one
	// status (failed=5 > completed=4) but the OTHER writer's overlay would
	// still pin its caller-supplied terminal status into the routing_slips
	// *_status column, briefly disagreeing with argMax truth until the next
	// hydrate. Pinning the event-log status here keeps the overlay aligned
	// with argMax even in the divergent-terminal race window.
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
			return false, ""
		case eventStatus.IsTerminal() && status.IsTerminal() && eventStatus != status:
			// Both terminal but disagree (same-µs race). Event log is authoritative
			// (it reflects the argMax tiebreak). Substitute eventStatus for the
			// caller's terminal so the overlay pin matches the argMax-resolved
			// *_status column. WARN log gives operators a signal to investigate
			// the upstream producer that emitted the losing terminal.
			slog.Warn("I5_overlay_terminal_divergence",
				slog.String("step", stepName),
				slog.String("caller_status", string(status)),
				slog.String("event_log_status", string(eventStatus)),
				slog.Time("written_at", writtenAt),
			)
			status = eventStatus
		}
	}

	// Defensive in-memory guard (fail-open fallback when latestStatus errored or
	// is nil, e.g. in older unit-test fixtures that have not been migrated yet).
	if !status.IsTerminal() && step.CompletedAt != nil {
		return false, ""
	}
	if step.CompletedAt == nil || writtenAt.After(*step.CompletedAt) {
		step.ApplyStatusTransition(status, writtenAt)
		slip.Steps[stepName] = step
		return true, status
	}
	return false, ""
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
