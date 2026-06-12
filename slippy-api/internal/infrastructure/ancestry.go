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
// slip resolution. FindByCommits and FindAllByCommits attempt a direct ClickHouse
// reader lookup first and fall back to the library's commit-ancestry walker on miss.
// LoadByCommit is resolver-first by design — it always walks ancestry — for callers
// that want "the slip for THIS commit's lineage" (e.g. image-tag fallback, historical
// lookup, rerun continuity). Callers needing exact-SHA semantics MUST use
// LoadByCommitExact, which bypasses the resolver and returns ErrSlipNotFound when
// no live slip exists for the exact SHA.
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
	ctx, span := otel.Tracer(ancestryTracerName).Start(ctx, "ancestry.Load",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
		),
	)
	defer span.End()

	slip, err := a.reader.Load(ctx, correlationID)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, slippy.ErrSlipNotFound) {
			span.SetStatus(codes.Unset, "not found")
		} else {
			span.SetStatus(codes.Error, "load failed")
			slog.ErrorContext(ctx, "ancestry: load by correlation_id failed",
				"correlation_id", correlationID, "error", err)
		}
		return nil, err
	}
	span.SetAttributes(
		attribute.String("slip.slip_repository", slip.Repository),
		attribute.String("slip.status", string(slip.Status)),
	)
	span.SetStatus(codes.Ok, "")
	return slip, nil
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
		"requested_repository", repository, "commit_sha", commitSHA)

	result, err := a.resolver.ResolveSlip(ctx, slippy.ResolveOptions{
		Repository: repository,
		Ref:        commitSHA,
	})
	if err == nil {
		slog.InfoContext(ctx, "ancestry: resolved slip",
			"requested_repository", repository,
			"slip_repository", result.Slip.Repository,
			"commit_sha", commitSHA,
			"resolved_by", result.ResolvedBy,
			"matched_commit", result.MatchedCommit,
			"correlation_id", result.Slip.CorrelationID)
		span.SetAttributes(
			attribute.String("slip.resolved_by", result.ResolvedBy),
			attribute.String("slip.matched_commit", result.MatchedCommit),
			attribute.String("slip.correlation_id", result.Slip.CorrelationID),
			attribute.String("slip.slip_repository", result.Slip.Repository),
		)
		span.SetStatus(codes.Ok, "resolved via "+result.ResolvedBy)
		return result.Slip, nil
	}

	if !errors.Is(err, slippy.ErrSlipNotFound) {
		slog.WarnContext(ctx, "ancestry: resolver error",
			"requested_repository", repository, "commit_sha", commitSHA, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "resolver error")
		return nil, err
	}

	slog.InfoContext(ctx, "ancestry: slip not found via resolver",
		"requested_repository", repository, "commit_sha", commitSHA)
	span.SetStatus(codes.Unset, "not found")
	return nil, slippy.ErrSlipNotFound
}

// LoadByCommitExact bypasses the resolver-first ancestry walk and returns the LIVE
// slip for the exact (repository, commitSHA) via the direct ClickHouse store. Returns
// slippy.ErrSlipNotFound when no live slip exists. Use only for dedup-loser polling
// and other in-flight paths that require exact-SHA semantics.
func (a *SlipResolverAdapter) LoadByCommitExact(
	ctx context.Context,
	repository, commitSHA string,
) (*domain.Slip, error) {
	ctx, span := otel.Tracer(ancestryTracerName).Start(ctx, "ancestry.LoadByCommitExact",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.String("slip.commit_sha", commitSHA),
		),
	)
	defer span.End()

	slip, err := a.reader.LoadByCommitExact(ctx, repository, commitSHA)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, slippy.ErrSlipNotFound) {
			span.SetStatus(codes.Unset, "not found")
		} else {
			span.SetStatus(codes.Error, "load failed")
			slog.ErrorContext(ctx, "ancestry: load-exact by commit failed",
				"repository", repository, "commit_sha", commitSHA, "error", err)
		}
		return nil, err
	}
	span.SetAttributes(
		attribute.String("slip.correlation_id", slip.CorrelationID),
		attribute.String("slip.slip_repository", slip.Repository),
		attribute.String("slip.status", string(slip.Status)),
	)
	span.SetStatus(codes.Ok, "")
	return slip, nil
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
		"requested_repository", repository, "commits_count", len(commits))

	for _, commit := range commits {
		result, resolveErr := a.resolver.ResolveSlip(ctx, slippy.ResolveOptions{
			Repository: repository,
			Ref:        commit,
		})
		if resolveErr == nil {
			slog.InfoContext(ctx, "ancestry: resolved slip for commit",
				"requested_repository", repository,
				"slip_repository", result.Slip.Repository,
				"input_commit", commit,
				"resolved_by", result.ResolvedBy,
				"matched_commit", result.MatchedCommit,
				"correlation_id", result.Slip.CorrelationID)
			span.SetAttributes(
				attribute.String("slip.resolved_by", result.ResolvedBy),
				attribute.String("slip.matched_commit", result.MatchedCommit),
				attribute.String("slip.input_commit", commit),
				attribute.String("slip.correlation_id", result.Slip.CorrelationID),
				attribute.String("slip.slip_repository", result.Slip.Repository),
			)
			span.SetStatus(codes.Ok, "resolved via "+result.ResolvedBy)
			return result.Slip, commit, nil
		}

		if !errors.Is(resolveErr, slippy.ErrSlipNotFound) {
			slog.WarnContext(ctx, "ancestry: resolver error for commit",
				"requested_repository", repository, "commit", commit, "error", resolveErr)
			span.RecordError(resolveErr)
			span.SetStatus(codes.Error, "resolver error")
			return nil, "", resolveErr
		}
	}

	slog.InfoContext(ctx, "ancestry: no slip found for any commit",
		"requested_repository", repository, "commits_count", len(commits))
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
		"requested_repository", repository, "commits_count", len(commits))

	var allResults []domain.SlipWithCommit
	for _, commit := range commits {
		result, resolveErr := a.resolver.ResolveSlip(ctx, slippy.ResolveOptions{
			Repository: repository,
			Ref:        commit,
		})
		if resolveErr == nil {
			slog.InfoContext(ctx, "ancestry: resolved slip in find-all",
				"requested_repository", repository,
				"slip_repository", result.Slip.Repository,
				"input_commit", commit,
				"resolved_by", result.ResolvedBy,
				"matched_commit", result.MatchedCommit,
				"correlation_id", result.Slip.CorrelationID)
			allResults = append(allResults, domain.SlipWithCommit{
				Slip:          result.Slip,
				MatchedCommit: commit,
			})
			continue
		}
		if !errors.Is(resolveErr, slippy.ErrSlipNotFound) {
			slog.WarnContext(ctx, "ancestry: resolver error for commit",
				"requested_repository", repository, "commit", commit, "error", resolveErr)
			span.RecordError(resolveErr)
			span.SetStatus(codes.Error, "resolver error")
			return nil, resolveErr
		}
	}

	slog.InfoContext(ctx, "ancestry: resolved slips for find-all",
		"requested_repository", repository, "results_count", len(allResults))
	span.SetAttributes(attribute.Int("slip.results_count", len(allResults)))
	span.SetStatus(codes.Ok, "resolved")
	return allResults, nil
}
