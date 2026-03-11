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
// slip resolution. For commit-based lookups (LoadByCommit, FindByCommits,
// FindAllByCommits), it first tries the direct reader, then falls back to the
// library's commit ancestry walker when no slip is found.
type SlipResolverAdapter struct {
	resolver SlipResolver
	reader   domain.SlipReader
}

// NewSlipResolverAdapter creates a decorator that resolves slips via the slippy
// library's ResolveSlip (commit ancestry + image tag fallback). The reader is
// used for direct lookups before falling back to ancestry resolution.
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

	slog.InfoContext(ctx, "ancestry: slip not found via resolver",
		"repository", repository, "commit_sha", commitSHA)
	span.SetStatus(codes.Unset, "not found")
	return nil, slippy.ErrSlipNotFound
}

func (a *SlipResolverAdapter) FindByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) (foundSlip *domain.Slip, matchedCommit string, err error) {
	// Try the direct ClickHouse lookup first.
	slip, matched, err := a.reader.FindByCommits(ctx, repository, commits)
	if err == nil {
		return slip, matched, nil
	}
	if !errors.Is(err, slippy.ErrSlipNotFound) {
		return nil, "", err
	}

	// Direct lookup found nothing — try ancestry resolution for each commit.
	ctx, span := otel.Tracer(ancestryTracerName).Start(ctx, "ancestry.FindByCommits.resolve",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "ancestry: resolving slip for commits via library",
		"repository", repository, "commits_count", len(commits))

	for _, commit := range commits {
		result, resolveErr := a.resolver.ResolveSlip(ctx, slippy.ResolveOptions{
			Repository: repository,
			Ref:        commit,
		})
		if resolveErr == nil {
			slog.InfoContext(ctx, "ancestry: resolved slip for commit",
				"repository", repository,
				"input_commit", commit,
				"resolved_by", result.ResolvedBy,
				"matched_commit", result.MatchedCommit,
				"correlation_id", result.Slip.CorrelationID)
			span.SetAttributes(
				attribute.String("slip.resolved_by", result.ResolvedBy),
				attribute.String("slip.matched_commit", result.MatchedCommit),
				attribute.String("slip.input_commit", commit),
				attribute.String("slip.correlation_id", result.Slip.CorrelationID),
			)
			span.SetStatus(codes.Ok, "resolved via "+result.ResolvedBy)
			return result.Slip, commit, nil
		}

		if !errors.Is(resolveErr, slippy.ErrSlipNotFound) {
			slog.WarnContext(ctx, "ancestry: resolver error for commit",
				"repository", repository, "commit", commit, "error", resolveErr)
			span.RecordError(resolveErr)
			span.SetStatus(codes.Error, "resolver error")
			return nil, "", resolveErr
		}
	}

	slog.InfoContext(ctx, "ancestry: no slip found for any commit",
		"repository", repository, "commits_count", len(commits))
	span.SetStatus(codes.Unset, "not found")
	return nil, "", slippy.ErrSlipNotFound
}

func (a *SlipResolverAdapter) FindAllByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	// Try the direct ClickHouse lookup first.
	results, err := a.reader.FindAllByCommits(ctx, repository, commits)
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		return results, nil
	}

	// Direct lookup found nothing — try ancestry resolution for each commit.
	ctx, span := otel.Tracer(ancestryTracerName).Start(ctx, "ancestry.FindAllByCommits.resolve",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "ancestry: resolving all slips for commits via library",
		"repository", repository, "commits_count", len(commits))

	var allResults []domain.SlipWithCommit
	for _, commit := range commits {
		result, resolveErr := a.resolver.ResolveSlip(ctx, slippy.ResolveOptions{
			Repository: repository,
			Ref:        commit,
		})
		if resolveErr == nil {
			allResults = append(allResults, domain.SlipWithCommit{
				Slip:          result.Slip,
				MatchedCommit: commit,
			})
			continue
		}
		if !errors.Is(resolveErr, slippy.ErrSlipNotFound) {
			slog.WarnContext(ctx, "ancestry: resolver error for commit",
				"repository", repository, "commit", commit, "error", resolveErr)
			span.RecordError(resolveErr)
			span.SetStatus(codes.Error, "resolver error")
			return nil, resolveErr
		}
	}

	slog.InfoContext(ctx, "ancestry: resolved slips for find-all",
		"repository", repository, "results_count", len(allResults))
	span.SetAttributes(attribute.Int("slip.results_count", len(allResults)))
	span.SetStatus(codes.Ok, "resolved")
	return allResults, nil
}
