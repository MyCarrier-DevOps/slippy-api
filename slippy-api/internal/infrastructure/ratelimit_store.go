package infrastructure

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MyCarrier-DevOps/slippy-api/internal/middleware"
)

// failScript records one failed attempt and returns the resulting lockout, atomically.
//
// One round trip, and atomic across both replicas: the increment, the lockout extension and
// the TTL refresh cannot interleave with a concurrent attempt from the same identity.
//
// It contains no policy. Both the ladder and the TTL rule arrive as a lookup table built by
// middleware.LadderArgs, so this script only indexes — the Fibonacci sequence and the
// "10x, floored at an hour" rule have exactly one definition, in Go, where they are
// unit-tested. Restating either in Lua would be a copy free to drift.
//
//	KEYS[1]  record key
//	ARGV[1]  now, unix seconds
//	ARGV[2]  free failures before the ladder starts
//	ARGV[3+] (lockout, ttl) pairs in seconds, rung k at ARGV[3+2k] and ARGV[4+2k]
const failScript = `
local n = redis.call('HINCRBY', KEYS[1], 'n', 1)
local now = tonumber(ARGV[1])
local free = tonumber(ARGV[2])

local k = n - free
if k < 0 then k = 0 end
local idx = 3 + 2 * k
if idx + 1 > #ARGV then idx = #ARGV - 1 end

local delay = tonumber(ARGV[idx])
local ttl = tonumber(ARGV[idx + 1])

redis.call('HSET', KEYS[1], 'u', now + delay)
redis.call('EXPIRE', KEYS[1], ttl)
return {n, delay, now + delay, ttl}
`

// RedisRateLimitStore implements middleware.RateLimitStore on the same redis.Cmdable that
// already backs the cache reader and the dedup lock. It introduces no new dependency.
//
// Shared state is required rather than convenient: the service runs two replicas, so
// per-process counters would hand an attacker one budget per pod and reset the ladder on
// every deploy.
type RedisRateLimitStore struct {
	client redis.Cmdable
	ladder []any // precomputed script arguments, built once
}

// Compile-time interface compliance check.
var _ middleware.RateLimitStore = (*RedisRateLimitStore)(nil)

// NewRedisRateLimitStore wraps an existing redis.Cmdable as a rate-limit store.
func NewRedisRateLimitStore(client redis.Cmdable) *RedisRateLimitStore {
	return &RedisRateLimitStore{client: client, ladder: middleware.LadderArgs()}
}

func (s *RedisRateLimitStore) Fail(ctx context.Context, key string) (middleware.RateLimitState, error) {
	now := time.Now()
	args := append([]any{now.Unix()}, s.ladder...)

	raw, err := s.client.Eval(ctx, failScript, []string{key}, args...).Slice()
	if err != nil {
		return middleware.RateLimitState{}, fmt.Errorf("rate limit fail script: %w", err)
	}
	if len(raw) < 4 {
		return middleware.RateLimitState{}, fmt.Errorf("rate limit fail script returned %d values, want 4", len(raw))
	}

	failures := toInt(raw[0])
	lockUntil := time.Unix(int64(toInt(raw[2])), 0)
	ttl := time.Duration(toInt(raw[3])) * time.Second

	return middleware.RateLimitState{
		Failures:   failures,
		RetryAfter: max(time.Until(lockUntil), 0),
		ResetAt:    now.Add(ttl),
		Exists:     true,
	}, nil
}

func (s *RedisRateLimitStore) Peek(ctx context.Context, key string) (middleware.RateLimitState, error) {
	vals, err := s.client.HMGet(ctx, key, "n", "u").Result()
	if err != nil {
		return middleware.RateLimitState{}, fmt.Errorf("rate limit peek: %w", err)
	}
	if len(vals) < 2 || vals[0] == nil {
		return middleware.RateLimitState{}, nil // no record: a clean identity
	}

	failures := toInt(vals[0])
	lockUntil := time.Unix(int64(toInt(vals[1])), 0)

	ttl, err := s.client.TTL(ctx, key).Result()
	if err != nil {
		return middleware.RateLimitState{}, fmt.Errorf("rate limit peek ttl: %w", err)
	}

	return middleware.RateLimitState{
		Failures:   failures,
		RetryAfter: max(time.Until(lockUntil), 0),
		ResetAt:    time.Now().Add(ttl),
		Exists:     true,
	}, nil
}

func (s *RedisRateLimitStore) Clear(ctx context.Context, key string) error {
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("rate limit clear: %w", err)
	}
	return nil
}

// toInt coerces the values Redis returns, which arrive as int64 from a script and as
// strings from a hash read.
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
