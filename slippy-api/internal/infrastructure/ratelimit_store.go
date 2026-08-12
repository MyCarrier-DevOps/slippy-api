package infrastructure

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MyCarrier-DevOps/slippy-api/internal/middleware"
)

// Record fields. n is the consecutive failure count, u the unix second the lockout expires.
const (
	rateLimitFieldFailures = "n"
	rateLimitFieldUntil    = "u"
)

// RedisRateLimitStore implements middleware.RateLimitStore on the same redis.Cmdable that
// already backs the cache reader and the dedup lock. It introduces no new dependency.
//
// Shared state is required rather than convenient: the service runs two replicas, so
// per-process counters would hand an attacker one budget per pod and reset the ladder on
// every deploy.
//
// Native commands only, no server-side scripting. The one thing that must be atomic is the
// failure count, and HINCRBY already is; everything after it is derived from the returned n
// and computed in Go, where the ladder is unit-tested. Concurrent failures from a single
// identity can interleave such that a lower-n write lands last and sets a lockout one rung
// short — which the next attempt immediately re-extends, since blocked attempts count too.
// That is the entire race, and it is not worth a script to close: it costs an attacker
// nothing to have a 10-second penalty instead of a 20-second one, and buying it back would
// mean putting execution on the shared cache that also serves slip reads and the dedup lock.
type RedisRateLimitStore struct {
	client redis.Cmdable
}

// Compile-time interface compliance check.
var _ middleware.RateLimitStore = (*RedisRateLimitStore)(nil)

// NewRedisRateLimitStore wraps an existing redis.Cmdable as a rate-limit store.
func NewRedisRateLimitStore(client redis.Cmdable) *RedisRateLimitStore {
	return &RedisRateLimitStore{client: client}
}

// Fail records one failed attempt and returns the resulting standing.
//
// Two round trips, and only ever on the failure path — a successful authentication never
// reaches here.
func (s *RedisRateLimitStore) Fail(ctx context.Context, key string) (middleware.RateLimitState, error) {
	failures, err := s.client.HIncrBy(ctx, key, rateLimitFieldFailures, 1).Result()
	if err != nil {
		return middleware.RateLimitState{}, fmt.Errorf("rate limit increment: %w", err)
	}

	// The policy lives here, in the language it is tested in.
	lockout := middleware.LockoutFor(int(failures))
	ttl := middleware.RecordTTL(lockout)
	now := time.Now()

	// Pipelined so the deadline and the TTL refresh cost one round trip rather than two.
	if _, err := s.client.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, key, rateLimitFieldUntil, now.Add(lockout).Unix())
		p.Expire(ctx, key, ttl)
		return nil
	}); err != nil {
		return middleware.RateLimitState{}, fmt.Errorf("rate limit record: %w", err)
	}

	return middleware.RateLimitState{
		Failures:   int(failures),
		RetryAfter: lockout,
		ResetAt:    now.Add(ttl),
		Exists:     true,
	}, nil
}

// Peek returns the current standing without recording an attempt. This is the only call on
// the success path, so it is one pipelined round trip.
func (s *RedisRateLimitStore) Peek(ctx context.Context, key string) (middleware.RateLimitState, error) {
	var (
		fields *redis.SliceCmd
		ttl    *redis.DurationCmd
	)
	if _, err := s.client.Pipelined(ctx, func(p redis.Pipeliner) error {
		fields = p.HMGet(ctx, key, rateLimitFieldFailures, rateLimitFieldUntil)
		ttl = p.TTL(ctx, key)
		return nil
	}); err != nil {
		return middleware.RateLimitState{}, fmt.Errorf("rate limit peek: %w", err)
	}

	vals := fields.Val()
	if len(vals) < 2 || vals[0] == nil {
		return middleware.RateLimitState{}, nil // no record: a clean identity
	}

	lockUntil := time.Unix(int64(toInt(vals[1])), 0)
	return middleware.RateLimitState{
		Failures:   toInt(vals[0]),
		RetryAfter: max(time.Until(lockUntil), 0),
		ResetAt:    time.Now().Add(ttl.Val()),
		Exists:     true,
	}, nil
}

// Clear forgets an identity, called when it authenticates successfully.
func (s *RedisRateLimitStore) Clear(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("rate limit clear: %w", err)
	}
	return nil
}

// toInt coerces a hash field, which Redis returns as a string.
func toInt(v any) int {
	switch t := v.(type) {
	case int64:
		return int(t)
	case int:
		return t
	case string:
		n, err := strconv.Atoi(t)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}
