package infrastructure

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// buildinfoTracerName is the instrumentation scope for buildinfo/image-tag operations.
const buildinfoTracerName = "slippy-api/buildinfo"

// errNoBuildInfoRows is returned when ci.buildinfo has no rows for a correlation ID.
var errNoBuildInfoRows = errors.New("no ci.buildinfo rows found for correlation ID")

// BuildInfoReader resolves per-component image tags by querying ci.buildinfo and
// ci.repoproperties in ClickHouse, using the routing slip to derive context.
type BuildInfoReader struct {
	session ch.ClickhouseSessionInterface
	reader  domain.SlipReader
}

// NewBuildInfoReader creates an ImageTagReader backed by ClickHouse.
// The reader is used to load the routing slip for slip-tag computation.
func NewBuildInfoReader(session ch.ClickhouseSessionInterface, reader domain.SlipReader) *BuildInfoReader {
	return &BuildInfoReader{
		session: session,
		reader:  reader,
	}
}

// Compile-time interface compliance check.
var _ domain.ImageTagReader = (*BuildInfoReader)(nil)

// ResolveImageTags loads the slip, determines build_scope from ci.repoproperties,
// and returns per-component image tags from ci.buildinfo.
func (b *BuildInfoReader) ResolveImageTags(ctx context.Context, correlationID string) (*domain.ImageTagResult, error) {
	ctx, span := otel.Tracer(buildinfoTracerName).Start(ctx, "buildinfo.ResolveImageTags",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.operation", "ResolveImageTags"),
			attribute.String("slip.correlation_id", correlationID),
		),
	)
	defer span.End()

	// Step 1: Load the slip to get repository, commit SHA, and created_at.
	slip, err := b.reader.Load(ctx, correlationID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// Step 2: Compute the slip tag (YY.WW.SHA7).
	slipTag := computeSlipTag(slip)
	span.SetAttributes(
		attribute.String("slip.repository", slip.Repository),
		attribute.String("slip.branch", slip.Branch),
		attribute.String("image_tag.slip_tag", slipTag),
	)

	// Step 3: Parse owner/repo from the slip's repository field.
	org, repoName, err := parseOwnerRepo(slip.Repository)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("invalid repository format %q: %w", slip.Repository, err)
	}

	// Step 4: Look up build_scope from ci.repoproperties.
	buildScope, err := b.queryBuildScope(ctx, org, repoName)
	if err != nil {
		// Default to "all" if the property lookup fails — safest fallback.
		buildScope = domain.BuildScopeAll
		span.AddEvent("build_scope lookup failed, defaulting to 'all'",
			trace.WithAttributes(attribute.String("error", err.Error())))
	}
	span.SetAttributes(attribute.String("image_tag.build_scope", buildScope))

	// Step 5: Query ci.buildinfo by CorrelationId for component details.
	componentTags, err := b.queryBuildInfo(ctx, correlationID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	// Step 6: Build the result based on build_scope.
	result := &domain.ImageTagResult{
		Tags:       make(map[string]string, len(componentTags)),
		BuildScope: buildScope,
		SlipTag:    slipTag,
	}

	switch buildScope {
	case domain.BuildScopeAll:
		// All components share the slip-computed tag.
		for component := range componentTags {
			result.Tags[component] = slipTag
		}
	default:
		// Each component carries its actual tag from ci.buildinfo.
		for component, tag := range componentTags {
			result.Tags[component] = tag
		}
	}

	span.SetAttributes(attribute.Int("image_tag.component_count", len(result.Tags)))
	span.SetStatus(codes.Ok, "")
	return result, nil
}

// queryBuildScope queries ci.repoproperties for the build_scope of a repository.
// Returns "all" or "modified". Falls back to "all" if no row is found.
func (b *BuildInfoReader) queryBuildScope(
	ctx context.Context,
	org, repoName string,
) (scope string, err error) {
	query := `SELECT build_scope FROM ci.repoproperties
		WHERE repository ILIKE ?
		AND organization = ?
		LIMIT 1`

	rows, err := b.session.QueryWithArgs(ctx, query, repoName, org)
	if err != nil {
		return "", fmt.Errorf("failed to query ci.repoproperties: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	if rows.Next() {
		var buildScope string
		if err := rows.Scan(&buildScope); err != nil {
			return "", fmt.Errorf("failed to scan build_scope: %w", err)
		}
		// Validate the value; default to "all" for unknown values.
		if buildScope != domain.BuildScopeAll && buildScope != domain.BuildScopeModified {
			return domain.BuildScopeAll, nil
		}
		return buildScope, nil
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating ci.repoproperties rows: %w", err)
	}

	// No row found — default to "all" (safest assumption).
	return domain.BuildScopeAll, nil
}

// queryBuildInfo queries ci.buildinfo by CorrelationId and returns
// a map of component → ImageTag for all rows with BuildStatus = 'OK'.
func (b *BuildInfoReader) queryBuildInfo(
	ctx context.Context,
	correlationID string,
) (tags map[string]string, err error) {
	// Use ROW_NUMBER to get the latest tag per component (same pattern as
	// offload-generator's ClickHouseImageTagResolver).
	query := `SELECT Component, ImageTag FROM (
		SELECT Component, ImageTag, BuildStatus,
		       ROW_NUMBER() OVER (PARTITION BY Component ORDER BY Timestamp DESC) AS rn
		FROM ci.buildinfo
		WHERE CorrelationId = ?
	) WHERE rn = 1 AND BuildStatus = 'OK'`

	rows, err := b.session.QueryWithArgs(ctx, query, correlationID)
	if err != nil {
		return nil, fmt.Errorf("failed to query ci.buildinfo: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	result := make(map[string]string)
	for rows.Next() {
		var component, imageTag string
		if err := rows.Scan(&component, &imageTag); err != nil {
			return nil, fmt.Errorf("failed to scan ci.buildinfo row: %w", err)
		}
		result[component] = imageTag
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("error iterating ci.buildinfo rows: %w", rowsErr)
	}

	if len(result) == 0 {
		return nil, errNoBuildInfoRows
	}

	return result, nil
}

// computeSlipTag computes the image tag from a slip's creation date and commit SHA.
// Format: YY.WW.SHA7 (e.g., "26.09.aef1234").
func computeSlipTag(slip *domain.Slip) string {
	if slip == nil || slip.CommitSHA == "" {
		return ""
	}
	year, week := slip.CreatedAt.ISOWeek()
	yy := year % 100
	sha7 := slip.CommitSHA
	if len(sha7) > 7 {
		sha7 = sha7[:7]
	}
	return fmt.Sprintf("%02d.%02d.%s", yy, week, sha7)
}

// parseOwnerRepo splits a "owner/repo" string into its two components.
func parseOwnerRepo(repository string) (org, repo string, err error) {
	parts := strings.SplitN(repository, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("expected 'owner/repo' format, got %q", repository)
	}
	return parts[0], parts[1], nil
}
