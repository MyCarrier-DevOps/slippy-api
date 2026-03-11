package infrastructure

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

const ancestryTracerName = "slippy-api/ancestry"

// SlipResolver abstracts the slippy library's ResolveSlip functionality.
// This interface enables testing without a real slippy.Client.
type SlipResolver interface {
	ResolveSlip(ctx context.Context, opts slippy.ResolveOptions) (*slippy.ResolveResult, error)
}

// SlipResolverAdapter wraps the slippy library's ResolveSlip for ancestry-based
// slip resolution. For LoadByCommit, it delegates to the library's commit ancestry
// walker. For all other methods, it delegates to the underlying reader chain.
//
// When ancestry resolution fails with ErrSlipNotFound, the adapter falls back to
// the reader chain (which includes fork-aware resolution) for repo name mismatches.
type SlipResolverAdapter struct {
	resolver SlipResolver
	reader   domain.SlipReader
}

// NewSlipResolverAdapter creates a decorator that resolves slips via the slippy
// library's ResolveSlip (commit ancestry + image tag fallback). The reader is
// used for passthrough methods and as a fork-aware fallback.
func NewSlipResolverAdapter(
	resolver SlipResolver,
	reader domain.SlipReader,
) *SlipResolverAdapter {
	return &SlipResolverAdapter{
		resolver: resolver,
		reader:   reader,
	}
}

// Compile-time interface compliance check.
var _ domain.SlipReader = (*SlipResolverAdapter)(nil)

func (a *SlipResolverAdapter) Load(ctx context.Context, correlationID string) (*domain.Slip, error) {
	return a.reader.Load(ctx, correlationID)
}

func (a *SlipResolverAdapter) LoadByCommit(
	ctx context.Context,
	repository, commitSHA string,
) (*domain.Slip, error) {
	ctx, span := otel.Tracer(ancestryTracerName).Start(ctx, "ancestry.LoadByCommit.resolve",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.String("slip.commit_sha", commitSHA),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "ancestry: resolving slip via library",
		"repository", repository, "commit_sha", commitSHA)

	result, err := a.resolver.ResolveSlip(ctx, slippy.ResolveOptions{
		Repository: repository,
		Ref:        commitSHA,
	})
	if err == nil {
		slog.InfoContext(ctx, "ancestry: resolved slip",
			"repository", repository,
			"commit_sha", commitSHA,
			"resolved_by", result.ResolvedBy,
			"matched_commit", result.MatchedCommit,
			"correlation_id", result.Slip.CorrelationID)
		span.SetAttributes(
			attribute.String("slip.resolved_by", result.ResolvedBy),
			attribute.String("slip.matched_commit", result.MatchedCommit),
			attribute.String("slip.correlation_id", result.Slip.CorrelationID),
		)
		span.SetStatus(codes.Ok, "resolved via "+result.ResolvedBy)
		return result.Slip, nil
	}

	if !errors.Is(err, slippy.ErrSlipNotFound) {
		slog.WarnContext(ctx, "ancestry: resolver error",
			"repository", repository, "commit_sha", commitSHA, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "resolver error")
		return nil, err
	}

	// Ancestry resolution didn't find a slip. Fall back to the reader chain
	// which includes fork-aware resolution for repo name mismatches.
	slog.InfoContext(ctx, "ancestry: falling back to reader chain",
		"repository", repository, "commit_sha", commitSHA)
	span.SetStatus(codes.Unset, "falling back to reader chain")
	return a.reader.LoadByCommit(ctx, repository, commitSHA)
}

func (a *SlipResolverAdapter) FindByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) (foundSlip *domain.Slip, matchedCommit string, err error) {
	return a.reader.FindByCommits(ctx, repository, commits)
}

func (a *SlipResolverAdapter) FindAllByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	return a.reader.FindAllByCommits(ctx, repository, commits)
}
