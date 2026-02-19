package infrastructure

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// cacheTracerName is the instrumentation scope for cache operations.
const cacheTracerName = "slippy-api/cache"

// CachedSlipReader is a decorator that adds Dragonfly/Redis caching around a
// domain.SlipReader. Cache misses fall through to the underlying reader.
type CachedSlipReader struct {
	reader domain.SlipReader
	client redis.Cmdable
	ttl    time.Duration
}

// NewCachedSlipReader wraps reader with an optional caching layer.
// If client is nil the decorator still works — every call falls through to reader.
func NewCachedSlipReader(reader domain.SlipReader, client redis.Cmdable, ttl time.Duration) *CachedSlipReader {
	return &CachedSlipReader{
		reader: reader,
		client: client,
		ttl:    ttl,
	}
}

// Compile-time interface compliance check.
var _ domain.SlipReader = (*CachedSlipReader)(nil)

// cacheKey builds a deterministic cache key for a given operation.
func cacheKey(operation, repository string, commits []string) string {
	return "slippy:" + operation + ":" + repository + ":" + strings.Join(commits, ",")
}

// ---------------------------------------------------------------------------
// SlipReader delegation — cache logic will be layered on in a later iteration.
// ---------------------------------------------------------------------------

func (c *CachedSlipReader) Load(ctx context.Context, correlationID string) (*domain.Slip, error) {
	ctx, span := otel.Tracer(cacheTracerName).Start(ctx, "cache.Load",
		trace.WithAttributes(
			attribute.String("cache.system", "dragonfly"),
			attribute.String("cache.operation", "Load"),
			attribute.String("slip.correlation_id", correlationID),
		),
	)
	defer span.End()

	// Cache passthrough — always delegates to underlying reader.
	span.SetAttributes(attribute.String("cache.result", "passthrough"))
	slip, err := c.reader.Load(ctx, correlationID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return slip, nil
}

func (c *CachedSlipReader) LoadByCommit(ctx context.Context, repository, commitSHA string) (*domain.Slip, error) {
	ctx, span := otel.Tracer(cacheTracerName).Start(ctx, "cache.LoadByCommit",
		trace.WithAttributes(
			attribute.String("cache.system", "dragonfly"),
			attribute.String("cache.operation", "LoadByCommit"),
			attribute.String("slip.repository", repository),
			attribute.String("slip.commit_sha", commitSHA),
		),
	)
	defer span.End()

	span.SetAttributes(attribute.String("cache.result", "passthrough"))
	slip, err := c.reader.LoadByCommit(ctx, repository, commitSHA)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return slip, nil
}

func (c *CachedSlipReader) FindByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) (*domain.Slip, string, error) {
	ctx, span := otel.Tracer(cacheTracerName).Start(ctx, "cache.FindByCommits",
		trace.WithAttributes(
			attribute.String("cache.system", "dragonfly"),
			attribute.String("cache.operation", "FindByCommits"),
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	span.SetAttributes(attribute.String("cache.result", "passthrough"))
	slip, matched, err := c.reader.FindByCommits(ctx, repository, commits)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, "", err
	}
	return slip, matched, nil
}

func (c *CachedSlipReader) FindAllByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	ctx, span := otel.Tracer(cacheTracerName).Start(ctx, "cache.FindAllByCommits",
		trace.WithAttributes(
			attribute.String("cache.system", "dragonfly"),
			attribute.String("cache.operation", "FindAllByCommits"),
			attribute.String("slip.repository", repository),
			attribute.Int("slip.commits_count", len(commits)),
		),
	)
	defer span.End()

	span.SetAttributes(attribute.String("cache.result", "passthrough"))
	results, err := c.reader.FindAllByCommits(ctx, repository, commits)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return results, nil
}
