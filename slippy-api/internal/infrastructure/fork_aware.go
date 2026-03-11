package infrastructure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

const forkAwareTracerName = "slippy-api/fork-aware"

// ForkAwareSlipReader wraps a SlipReader with fallback logic for forked repositories.
// When a commit-based lookup fails because the caller provides a fork's repository
// name but the slip was stored under the parent (or vice versa), the decorator
// resolves the actual repository via a lightweight commit_sha query and retries.
type ForkAwareSlipReader struct {
	reader   domain.SlipReader
	session  ch.ClickhouseSessionInterface
	database string
}

// NewForkAwareSlipReader creates a fork-aware decorator around reader.
// session and database are used for fallback repository resolution queries.
func NewForkAwareSlipReader(
	reader domain.SlipReader,
	session ch.ClickhouseSessionInterface,
	database string,
) *ForkAwareSlipReader {
	return &ForkAwareSlipReader{
		reader:   reader,
		session:  session,
		database: database,
	}
}

// Compile-time interface compliance check.
var _ domain.SlipReader = (*ForkAwareSlipReader)(nil)

func (f *ForkAwareSlipReader) Load(ctx context.Context, correlationID string) (*domain.Slip, error) {
	return f.reader.Load(ctx, correlationID)
}

func (f *ForkAwareSlipReader) LoadByCommit(ctx context.Context, repository, commitSHA string) (*domain.Slip, error) {
	slip, err := f.reader.LoadByCommit(ctx, repository, commitSHA)
	if err == nil || !errors.Is(err, slippy.ErrSlipNotFound) {
		return slip, err
	}

	ctx, span := otel.Tracer(forkAwareTracerName).Start(ctx, "forkAware.LoadByCommit.fallback",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.String("slip.commit_sha", commitSHA),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "fork-aware fallback: resolving repository for commit",
		"repository", repository, "commit_sha", commitSHA)

	actualRepo, resolveErr := f.resolveRepository(ctx, commitSHA)
	if resolveErr != nil {
		slog.WarnContext(ctx, "fork-aware fallback: resolve query failed",
			"commit_sha", commitSHA, "error", resolveErr)
		span.SetStatus(codes.Unset, "no cross-repo match")
		return nil, err
	}

	slog.InfoContext(ctx, "fork-aware fallback: resolved repository",
		"input_repository", repository, "resolved_repository", actualRepo)

	if strings.EqualFold(actualRepo, repository) {
		slog.InfoContext(ctx, "fork-aware fallback: resolved same repository, skipping retry",
			"repository", repository)
		span.SetStatus(codes.Unset, "same repository")
		return nil, err
	}

	span.SetAttributes(attribute.String("slip.resolved_repository", actualRepo))
	slip, retryErr := f.reader.LoadByCommit(ctx, actualRepo, commitSHA)
	if retryErr != nil {
		slog.WarnContext(ctx, "fork-aware fallback: retry with resolved repository failed",
			"resolved_repository", actualRepo, "error", retryErr)
		span.RecordError(retryErr)
		span.SetStatus(codes.Error, retryErr.Error())
		return nil, retryErr
	}
	slog.InfoContext(ctx, "fork-aware fallback: resolved via fork fallback",
		"input_repository", repository, "resolved_repository", actualRepo)
	span.SetStatus(codes.Ok, "resolved via fork fallback")
	return slip, nil
}

func (f *ForkAwareSlipReader) FindByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) (foundSlip *domain.Slip, matchedCommit string, err error) {
	slip, matched, err := f.reader.FindByCommits(ctx, repository, commits)
	if err == nil || !errors.Is(err, slippy.ErrSlipNotFound) {
		return slip, matched, err
	}

	ctx, span := otel.Tracer(forkAwareTracerName).Start(ctx, "forkAware.FindByCommits.fallback",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "fork-aware fallback: resolving repositories for commits",
		"repository", repository, "commits_count", len(commits))

	repos, resolveErr := f.resolveRepositories(ctx, commits)
	if resolveErr != nil || len(repos) == 0 {
		slog.WarnContext(ctx, "fork-aware fallback: no repositories resolved",
			"error", resolveErr, "repos_count", len(repos))
		span.SetStatus(codes.Unset, "no cross-repo match")
		return nil, "", err
	}

	slog.InfoContext(ctx, "fork-aware fallback: resolved repositories",
		"input_repository", repository, "resolved_repositories", repos)

	for _, repo := range repos {
		if strings.EqualFold(repo, repository) {
			continue
		}
		span.SetAttributes(attribute.String("slip.resolved_repository", repo))
		slip, matched, retryErr := f.reader.FindByCommits(ctx, repo, commits)
		if retryErr == nil {
			slog.InfoContext(ctx, "fork-aware fallback: resolved via fork fallback",
				"input_repository", repository, "resolved_repository", repo)
			span.SetStatus(codes.Ok, "resolved via fork fallback")
			return slip, matched, nil
		}
		slog.WarnContext(ctx, "fork-aware fallback: retry failed for resolved repository",
			"resolved_repository", repo, "error", retryErr)
	}

	slog.InfoContext(ctx, "fork-aware fallback: no match in resolved repositories",
		"input_repository", repository, "resolved_repositories", repos)
	span.SetStatus(codes.Unset, "no match in resolved repositories")
	return nil, "", err
}

func (f *ForkAwareSlipReader) FindAllByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	results, err := f.reader.FindAllByCommits(ctx, repository, commits)
	if err != nil || len(results) > 0 {
		return results, err
	}

	ctx, span := otel.Tracer(forkAwareTracerName).Start(ctx, "forkAware.FindAllByCommits.fallback",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "fork-aware fallback: resolving repositories for find-all",
		"repository", repository, "commits_count", len(commits))

	repos, resolveErr := f.resolveRepositories(ctx, commits)
	if resolveErr != nil || len(repos) == 0 {
		slog.WarnContext(ctx, "fork-aware fallback: no repositories resolved for find-all",
			"error", resolveErr, "repos_count", len(repos))
		span.SetStatus(codes.Unset, "no cross-repo match")
		return results, nil
	}

	slog.InfoContext(ctx, "fork-aware fallback: resolved repositories for find-all",
		"input_repository", repository, "resolved_repositories", repos)

	var allResults []domain.SlipWithCommit
	for _, repo := range repos {
		if strings.EqualFold(repo, repository) {
			continue
		}
		span.SetAttributes(attribute.String("slip.resolved_repository", repo))
		repoResults, retryErr := f.reader.FindAllByCommits(ctx, repo, commits)
		if retryErr == nil && len(repoResults) > 0 {
			slog.InfoContext(ctx, "fork-aware fallback: found results for resolved repository",
				"resolved_repository", repo, "results_count", len(repoResults))
			allResults = append(allResults, repoResults...)
		}
	}

	if len(allResults) > 0 {
		span.SetStatus(codes.Ok, "resolved via fork fallback")
		return allResults, nil
	}

	slog.InfoContext(ctx, "fork-aware fallback: no results from any resolved repository",
		"input_repository", repository, "resolved_repositories", repos)
	span.SetStatus(codes.Unset, "no match in resolved repositories")
	return results, nil
}

// resolveRepository finds the stored repository name for a commit SHA.
// The database prefix is set at construction time from server-side configuration.
func (f *ForkAwareSlipReader) resolveRepository(ctx context.Context, commitSHA string) (string, error) {
	query := fmt.Sprintf(
		"SELECT repository FROM %s.routing_slips WHERE commit_sha = ? AND sign = 1 ORDER BY version DESC LIMIT 1",
		f.database,
	)
	row := f.session.QueryRow(ctx, query, commitSHA)
	var repo string
	if err := row.Scan(&repo); err != nil {
		return "", err
	}
	return repo, nil
}

// resolveRepositories finds all distinct stored repository names for the given commits.
// The database prefix is set at construction time from server-side configuration.
func (f *ForkAwareSlipReader) resolveRepositories(ctx context.Context, commits []string) (repos []string, err error) {
	query := fmt.Sprintf(
		"SELECT DISTINCT repository FROM %s.routing_slips WHERE commit_sha IN ({shas:Array(String)}) AND sign = 1",
		f.database,
	)
	rows, err := f.session.QueryWithArgs(ctx, query, ch.Named("shas", commits))
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := rows.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	for rows.Next() {
		var repo string
		if err := rows.Scan(&repo); err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	return repos, rows.Err()
}
