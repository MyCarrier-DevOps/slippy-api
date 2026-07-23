package infrastructure

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// storeTracerName is the instrumentation scope for slip-store operations. These now execute
// against Postgres (the slip store is PostgresStore); ClickHouse is used elsewhere in the
// process only by the non-slip readers.
const storeTracerName = "slippy-api/store"

// SlipStoreAdapter adapts the upstream slippy.SlipStore (read+write) to the
// read-only domain.SlipReader interface. This ensures the API layer cannot
// accidentally invoke write operations.
type SlipStoreAdapter struct {
	store slippy.SlipStore
}

// NewSlipStoreAdapter wraps an upstream SlipStore as a read-only SlipReader.
func NewSlipStoreAdapter(store slippy.SlipStore) *SlipStoreAdapter {
	return &SlipStoreAdapter{store: store}
}

// Compile-time interface compliance check.
var _ domain.SlipReader = (*SlipStoreAdapter)(nil)

func (a *SlipStoreAdapter) Load(ctx context.Context, correlationID string) (*domain.Slip, error) {
	ctx, span := otel.Tracer(storeTracerName).Start(ctx, "postgres.Load",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "Load"),
			attribute.String("slip.correlation_id", correlationID),
		),
	)
	defer span.End()

	slip, err := a.store.Load(ctx, correlationID)
	if err != nil {
		recordStoreError(span, err)
		return nil, err
	}
	return slip, nil
}

func (a *SlipStoreAdapter) LoadByCommit(ctx context.Context, repository, commitSHA string) (*domain.Slip, error) {
	ctx, span := otel.Tracer(storeTracerName).Start(ctx, "postgres.LoadByCommit",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "LoadByCommit"),
			attribute.String("slip.repository", repository),
			attribute.String("slip.commit_sha", commitSHA),
		),
	)
	defer span.End()

	slip, err := a.store.LoadByCommit(ctx, repository, commitSHA)
	if err != nil {
		recordStoreError(span, err)
		return nil, err
	}
	return slip, nil
}

// LoadByCommitExact delegates to the underlying SlipStore.LoadLiveByCommit which
// returns the most recent LIVE (non-terminal) slip for the exact (repository, commitSHA).
// Returns slippy.ErrSlipNotFound when no live slip exists. Bypasses ancestry resolution.
func (a *SlipStoreAdapter) LoadByCommitExact(ctx context.Context, repository, commitSHA string) (*domain.Slip, error) {
	// Span name and db.operation are both "LoadLiveByCommit" — the actual query
	// invoked on the underlying Postgres store. The adapter method is named
	// LoadByCommitExact (the domain-side semantic: bypass ancestry resolution),
	// but the span identifies the executed work, not the caller-facing name, so
	// trace consumers correlate it directly with the store-level LoadLiveByCommit query.
	ctx, span := otel.Tracer(storeTracerName).Start(ctx, "postgres.LoadLiveByCommit",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "LoadLiveByCommit"),
			attribute.String("slip.repository", repository),
			attribute.String("slip.commit_sha", commitSHA),
		),
	)
	defer span.End()

	slip, err := a.store.LoadLiveByCommit(ctx, repository, commitSHA)
	if err != nil {
		recordStoreError(span, err)
		return nil, err
	}
	return slip, nil
}

func (a *SlipStoreAdapter) FindByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) (foundSlip *domain.Slip, matchedCommit string, err error) {
	ctx, span := otel.Tracer(storeTracerName).Start(ctx, "postgres.FindByCommits",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "FindByCommits"),
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	slip, matched, err := a.store.FindByCommits(ctx, repository, commits)
	if err != nil {
		recordStoreError(span, err)
		return nil, "", err
	}
	span.SetAttributes(attribute.String("slip.matched_commit", matched))
	return slip, matched, nil
}

func (a *SlipStoreAdapter) FindAllByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	ctx, span := otel.Tracer(storeTracerName).Start(ctx, "postgres.FindAllByCommits",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "FindAllByCommits"),
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	results, err := a.store.FindAllByCommits(ctx, repository, commits)
	if err != nil {
		recordStoreError(span, err)
		return nil, err
	}
	span.SetAttributes(attribute.Int("slip.results_count", len(results)))
	return results, nil
}

// Close releases resources held by the underlying store.
func (a *SlipStoreAdapter) Close() error {
	return a.store.Close()
}

// recordStoreError records an error on a span, distinguishing client errors
// (not-found, invalid input) from server/infrastructure errors.
func recordStoreError(span trace.Span, err error) {
	span.RecordError(err)
	// Mark not-found / validation errors as unset (expected outcomes),
	// and everything else as an actual error.
	switch {
	case isClientError(err):
		span.SetStatus(codes.Unset, err.Error())
	default:
		span.SetStatus(codes.Error, fmt.Sprintf("slip store query failed: %v", err))
	}
}

// isClientError returns true for errors that are expected client-side failures
// (not-found, bad input) rather than infrastructure errors.
func isClientError(err error) bool {
	return errIs(err,
		slippy.ErrSlipNotFound,
		slippy.ErrInvalidCorrelationID,
		slippy.ErrInvalidRepository,
	)
}

// errIs checks if err matches any of the targets.
func errIs(err error, targets ...error) bool {
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
