package infrastructure

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Default dedup-lock tunables. Overridable from config via NewRedisLocker is not
// needed — the writer adapter owns the TTL/wait values and passes them per call.
const (
	// dedupKeyPrefix namespaces dedup locks in the shared Redis/Dragonfly keyspace.
	dedupKeyPrefix = "sliplock:"

	// DefaultLockTTL is how long an acquired dedup lock lives. It must exceed the
	// worst-case CreateSlipForPush latency plus the ClickHouse async-insert
	// visibility window so a near-simultaneous duplicate stays blocked until the
	// first slip is durably visible to LoadByCommit.
	DefaultLockTTL = 120 * time.Second

	// DefaultLockWait is how long the lock-miss path polls LoadByCommit for the
	// in-flight slip to become visible before giving up with a retryable error.
	DefaultLockWait = 10 * time.Second
)

// casDelScript releases a lock only when the stored value still matches the
// caller's token (compare-and-delete). This prevents a process from releasing a
// lock that has since expired and been re-acquired by another process.
const casDelScript = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`

// Locker provides a cross-process dedup lock backed by Redis/Dragonfly.
//
// A nil Locker is a valid no-op (cache disabled) — callers MUST nil-check the
// interface value before use to preserve today's lock-free behavior when no
// cache is configured.
type Locker interface {
	// TryAcquire attempts an atomic SET key token NX PX ttl.
	// Returns (acquired, token, err). When acquired is false the lock is already
	// held by another caller. On error, callers should fail-open.
	TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, string, error)

	// Release deletes key only when its value still equals token (Lua CAS-del).
	// Releasing a lock that was never acquired (empty token) is a no-op.
	Release(ctx context.Context, key, token string) error
}

// DedupKey builds the dedup-lock key for a (repository, commitSHA) pair.
//
// The key is repo:sha — NOT correlationID (two duplicate webhooks get distinct
// correlation IDs but share repo+sha) and NOT X-GitHub-Delivery (GitHub assigns
// a fresh delivery GUID per redelivery). repo:sha matches the identity used by
// LoadByCommit, so it catches both GitHub redelivery and internal re-emit.
//
// Both components are lowercased for consistency; duplicate requests carry
// identical values, so normalization only guards against accidental case drift.
func DedupKey(repository, commitSHA string) string {
	return dedupKeyPrefix + strings.ToLower(repository) + ":" + strings.ToLower(commitSHA)
}

// RedisLocker implements Locker on top of the existing redis.Cmdable client that
// already backs the cache reader. It introduces no new redis dependency.
type RedisLocker struct {
	client redis.Cmdable
}

// NewRedisLocker wraps an existing redis.Cmdable as a Locker.
func NewRedisLocker(client redis.Cmdable) *RedisLocker {
	return &RedisLocker{client: client}
}

// Compile-time interface compliance check.
var _ Locker = (*RedisLocker)(nil)

// TryAcquire issues a single SET NX PX round-trip with a freshly generated token.
func (l *RedisLocker) TryAcquire(
	ctx context.Context,
	key string,
	ttl time.Duration,
) (acquired bool, token string, err error) {
	token, err = newLockToken()
	if err != nil {
		return false, "", err
	}
	acquired, err = l.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return false, "", err
	}
	if !acquired {
		return false, "", nil
	}
	return true, token, nil
}

// Release runs the CAS-del Lua script so a stale token never deletes a lock that
// a different process now owns. An empty token is treated as "nothing to release".
func (l *RedisLocker) Release(ctx context.Context, key, token string) error {
	if token == "" {
		return nil
	}
	return l.client.Eval(ctx, casDelScript, []string{key}, token).Err()
}

// newLockToken returns a random 16-byte hex string used as the lock fencing token.
func newLockToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
