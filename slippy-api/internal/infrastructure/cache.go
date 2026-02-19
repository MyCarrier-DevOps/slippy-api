package infrastructure

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

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
	return c.reader.Load(ctx, correlationID)
}

func (c *CachedSlipReader) LoadByCommit(ctx context.Context, repository, commitSHA string) (*domain.Slip, error) {
	return c.reader.LoadByCommit(ctx, repository, commitSHA)
}

func (c *CachedSlipReader) FindByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) (*domain.Slip, string, error) {
	return c.reader.FindByCommits(ctx, repository, commits)
}

func (c *CachedSlipReader) FindAllByCommits(
	ctx context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	return c.reader.FindAllByCommits(ctx, repository, commits)
}
