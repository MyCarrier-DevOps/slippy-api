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

// defaultWriteOpTimeout bounds a single Postgres slip write. Writes commit in
// milliseconds under MVCC, so a small bound is ample; it exists only to cap a
// stuck connection or failover, not to accommodate ClickHouse merge lag.
const defaultWriteOpTimeout = 30 * time.Second

// writeOpTimeout bounds a single Postgres slip write. The
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
// causing every write to fail before the Postgres driver even sends the query.
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
		// duplicate stays blocked through the window between the winner acquiring the
		// lock and committing its Postgres transaction — until that commit, the loser's
		// LoadByCommitExact sees no row under MVCC read-committed. The lib's
		// handlePushRetry is idempotent once the slip is visible (right after commit).
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
		func(wctx context.Context, _ trace.Span) error {
			// Postgres writes the step-status column atomically; no post-write hydration.
			return a.client.StartStep(wctx, correlationID, stepName, componentName)
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
		func(wctx context.Context, _ trace.Span) error {
			// Pipeline-level terminal events route directly: steps.go:101 guard fires
			// checkPipelineCompletion automatically. Component events MUST go through
			// RunPostExecution to drive aggregate recomputation. Postgres writes the
			// status column atomically, so no post-write hydration is needed.
			if componentName != "" {
				_, err := a.client.RunPostExecution(wctx, slippy.PostExecutionOptions{
					CorrelationID:     correlationID,
					StepName:          stepName,
					ComponentName:     componentName,
					WorkflowSucceeded: true,
				})
				return err
			}
			return a.client.CompleteStep(wctx, correlationID, stepName, componentName)
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
		func(wctx context.Context, _ trace.Span) error {
			// Pipeline-level terminal events route directly; component events go through
			// RunPostExecution to drive aggregate recomputation. Postgres writes the status
			// column atomically, so no post-write hydration is needed.
			if componentName != "" {
				_, err := a.client.RunPostExecution(wctx, slippy.PostExecutionOptions{
					CorrelationID:     correlationID,
					StepName:          stepName,
					ComponentName:     componentName,
					WorkflowSucceeded: false,
					FailureMessage:    reason,
				})
				return err
			}
			return a.client.FailStep(wctx, correlationID, stepName, componentName, reason)
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
		func(wctx context.Context, _ trace.Span) error {
			// Postgres writes the step-status column atomically; no post-write hydration.
			return a.client.SkipStep(wctx, correlationID, stepName, componentName, reason)
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
// The closure receives only wctx for use with the upstream client — the
// caller-supplied ctx is exclusively for span scoping. Do NOT pass the outer
// ctx to client.* calls; that would defeat the point of this indirection.
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
	// winner's slip becomes visible almost immediately after its transaction commits —
	// resolves with minimal added latency. The backoff still doubles up to maxBackoff to
	// cover a slower winner (a longer pre-commit window).
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
	// for a class-of-bug regression (e.g. winner crashing pre-commit, its transaction
	// staying open past lockWait, or ancestry-resolution drift causing the await
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
