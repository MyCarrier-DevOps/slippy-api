package infrastructure

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/slippy-api/internal/middleware"
)

func newRateLimitStore(t *testing.T) (*RedisRateLimitStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisRateLimitStore(client), mr
}

// The Lua script indexes a ladder computed in Go, so this is where the two halves meet: if
// the script's indexing is off by one, the durations here diverge from lockoutFor.
func TestRedisRateLimitStore_FailWalksTheLadder(t *testing.T) {
	store, _ := newRateLimitStore(t)
	ctx := context.Background()
	const key = "rl:auth:test"

	for _, want := range []struct {
		failures   int
		retryAfter time.Duration
	}{
		{1, 0},                // free
		{2, 0},                // free
		{3, 10 * time.Second}, // first rung
		{4, 20 * time.Second},
		{5, 30 * time.Second},
		{6, 50 * time.Second},
		{7, 80 * time.Second},
	} {
		st, err := store.Fail(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, want.failures, st.Failures)
		// Allow a second of slack: RetryAfter is derived from a wall-clock deadline.
		assert.InDelta(t, want.retryAfter.Seconds(), st.RetryAfter.Seconds(), 1.0,
			"failure %d", want.failures)
	}
}

func TestRedisRateLimitStore_PeekDoesNotRecord(t *testing.T) {
	store, _ := newRateLimitStore(t)
	ctx := context.Background()
	const key = "rl:auth:test"

	st, err := store.Peek(ctx, key)
	require.NoError(t, err)
	assert.False(t, st.Exists, "a clean identity has no record")
	assert.False(t, st.Locked())

	_, err = store.Fail(ctx, key)
	require.NoError(t, err)

	for range 5 {
		st, err = store.Peek(ctx, key)
		require.NoError(t, err)
	}
	assert.Equal(t, 1, st.Failures, "peeking must never charge a rung")
	assert.True(t, st.Exists)
}

func TestRedisRateLimitStore_ClearForgetsTheIdentity(t *testing.T) {
	store, _ := newRateLimitStore(t)
	ctx := context.Background()
	const key = "rl:auth:test"

	for range 4 {
		_, err := store.Fail(ctx, key)
		require.NoError(t, err)
	}
	require.NoError(t, store.Clear(ctx, key))

	st, err := store.Peek(ctx, key)
	require.NoError(t, err)
	assert.False(t, st.Exists, "a cleared identity starts fresh")
	assert.Zero(t, st.Failures)
}

// The record must outlive its own lockout by 10x, floored at an hour. Tying the TTL to the
// lockout would let a patient attacker wait one lockout, lose the record and reset to zero,
// so the ladder could never climb past its first rung.
func TestRedisRateLimitStore_TTLOutlivesTheLockout(t *testing.T) {
	store, mr := newRateLimitStore(t)
	ctx := context.Background()
	const key = "rl:auth:test"

	// Free allowance: no lockout, so the floor applies.
	_, err := store.Fail(ctx, key)
	require.NoError(t, err)
	assert.InDelta(t, time.Hour.Seconds(), mr.TTL(key).Seconds(), 2.0,
		"the free-allowance phase must still be remembered for the floor")

	// Climb until the 10x rule overtakes the floor.
	for range 11 {
		_, err = store.Fail(ctx, key)
		require.NoError(t, err)
	}
	st, err := store.Peek(ctx, key)
	require.NoError(t, err)
	assert.Greater(t, mr.TTL(key).Seconds(), st.RetryAfter.Seconds(),
		"a record that expires with its lockout resets the ladder")
}

// Attempts made while already locked out must charge a rung and push the deadline further
// out, so hammering a lockout makes it worse rather than passing the time.
func TestRedisRateLimitStore_LockedAttemptsExtend(t *testing.T) {
	store, _ := newRateLimitStore(t)
	ctx := context.Background()
	const key = "rl:auth:test"

	for range 4 {
		_, err := store.Fail(ctx, key)
		require.NoError(t, err)
	}
	before, err := store.Peek(ctx, key)
	require.NoError(t, err)
	require.True(t, before.Locked())

	_, err = store.Fail(ctx, key)
	require.NoError(t, err)
	after, err := store.Peek(ctx, key)
	require.NoError(t, err)

	assert.Greater(t, after.RetryAfter, before.RetryAfter, "the lockout must grow")
	assert.Greater(t, after.Failures, before.Failures)
}

// Compile-time proof the store satisfies what the middleware declares it needs.
func TestRedisRateLimitStore_ImplementsTheInterface(t *testing.T) {
	var _ middleware.RateLimitStore = (*RedisRateLimitStore)(nil)
}
