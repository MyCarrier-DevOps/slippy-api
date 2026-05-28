package infrastructure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// newTestLocker spins up an in-memory miniredis (no Docker) and returns a
// RedisLocker backed by it, plus the raw client for assertions.
func newTestLocker(t *testing.T) (*RedisLocker, redis.Cmdable, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisLocker(rdb), rdb, mr
}

func TestDedupKey_Normalizes(t *testing.T) {
	// Mixed-case repo + SHA must collapse to a single lowercase key so case drift
	// between duplicate webhooks cannot bypass the lock.
	got := DedupKey("Org/My-Service", "DEADBEEF")
	assert.Equal(t, "sliplock:org/my-service:deadbeef", got)

	// Identity: already-lowercase input is unchanged.
	assert.Equal(t, "sliplock:org/repo:abc123", DedupKey("org/repo", "abc123"))

	// Two case variants of the same logical (repo, sha) produce the same key.
	assert.Equal(t, DedupKey("Org/Repo", "ABC"), DedupKey("org/repo", "abc"))
}

func TestRedisLocker_TryAcquire_OnceThenFalse(t *testing.T) {
	locker, _, _ := newTestLocker(t)
	ctx := context.Background()
	key := DedupKey("org/repo", "sha-acquire-once")

	acquired, token, err := locker.TryAcquire(ctx, key, time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired, "first acquire should succeed")
	assert.NotEmpty(t, token, "successful acquire must return a non-empty token")

	// Second acquire of the same key, before release/expiry, must fail.
	acquired2, token2, err := locker.TryAcquire(ctx, key, time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired2, "second acquire of held key should fail")
	assert.Empty(t, token2, "failed acquire returns empty token")
}

func TestRedisLocker_TryAcquire_DistinctKeysIndependent(t *testing.T) {
	locker, _, _ := newTestLocker(t)
	ctx := context.Background()

	a1, _, err := locker.TryAcquire(ctx, DedupKey("org/repo", "sha-a"), time.Minute)
	require.NoError(t, err)
	a2, _, err := locker.TryAcquire(ctx, DedupKey("org/repo", "sha-b"), time.Minute)
	require.NoError(t, err)
	assert.True(t, a1)
	assert.True(t, a2, "different repo:sha keys must not collide")
}

func TestRedisLocker_Release_OnlyOnTokenMatch(t *testing.T) {
	locker, rdb, _ := newTestLocker(t)
	ctx := context.Background()
	key := DedupKey("org/repo", "sha-cas")

	acquired, token, err := locker.TryAcquire(ctx, key, time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	// Release with a WRONG token must NOT delete the key (CAS-del guard).
	require.NoError(t, locker.Release(ctx, key, "not-the-token"))
	exists, err := rdb.Exists(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "wrong-token release must not delete the lock")

	// Release with the CORRECT token deletes the key.
	require.NoError(t, locker.Release(ctx, key, token))
	exists, err = rdb.Exists(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "matching-token release must delete the lock")

	// After release, the key can be acquired again.
	reacquired, _, err := locker.TryAcquire(ctx, key, time.Minute)
	require.NoError(t, err)
	assert.True(t, reacquired, "key should be acquirable after a CAS-del release")
}

func TestRedisLocker_Release_EmptyTokenIsNoOp(t *testing.T) {
	locker, rdb, _ := newTestLocker(t)
	ctx := context.Background()
	key := DedupKey("org/repo", "sha-empty")

	_, _, err := locker.TryAcquire(ctx, key, time.Minute)
	require.NoError(t, err)

	// Releasing with an empty token (e.g. fail-open path never acquired) is a no-op
	// and must never touch an existing lock.
	require.NoError(t, locker.Release(ctx, key, ""))
	exists, err := rdb.Exists(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "empty-token release must not delete the lock")
}

func TestRedisLocker_TryAcquire_ExpiresAfterTTL(t *testing.T) {
	locker, _, mr := newTestLocker(t)
	ctx := context.Background()
	key := DedupKey("org/repo", "sha-ttl")

	acquired, _, err := locker.TryAcquire(ctx, key, 100*time.Millisecond)
	require.NoError(t, err)
	require.True(t, acquired)

	// Advance miniredis' clock past the TTL; the lock should be gone and the key
	// re-acquirable, which models the "late duplicate after slip is visible" case.
	mr.FastForward(200 * time.Millisecond)

	reacquired, _, err := locker.TryAcquire(ctx, key, time.Minute)
	require.NoError(t, err)
	assert.True(t, reacquired, "lock should be re-acquirable after TTL expiry")
}

// --- SlipWriterAdapter dedup branch coverage (stub Locker) -----------------

// stubLocker is a configurable Locker for unit-testing the adapter's dedup paths
// without a Redis backend.
type stubLocker struct {
	acquireFn func(ctx context.Context, key string, ttl time.Duration) (bool, string, error)
	releaseFn func(ctx context.Context, key, token string) error
	released  int
}

func (s *stubLocker) TryAcquire(ctx context.Context, key string, ttl time.Duration) (bool, string, error) {
	return s.acquireFn(ctx, key, ttl)
}

func (s *stubLocker) Release(ctx context.Context, key, token string) error {
	s.released++
	if s.releaseFn != nil {
		return s.releaseFn(ctx, key, token)
	}
	return nil
}

// stubReader is a minimal domain.SlipReader for the lock-miss poll path.
type stubReader struct {
	loadByCommitFn func(ctx context.Context, repo, sha string) (*slippy.Slip, error)
}

func (r *stubReader) Load(_ context.Context, _ string) (*slippy.Slip, error) {
	return nil, slippy.ErrSlipNotFound
}
func (r *stubReader) LoadByCommit(ctx context.Context, repo, sha string) (*slippy.Slip, error) {
	return r.loadByCommitFn(ctx, repo, sha)
}
func (r *stubReader) FindByCommits(_ context.Context, _ string, _ []string) (*slippy.Slip, string, error) {
	return nil, "", slippy.ErrSlipNotFound
}
func (r *stubReader) FindAllByCommits(_ context.Context, _ string, _ []string) ([]slippy.SlipWithCommit, error) {
	return nil, nil
}

// newWriterAdapterWithDeps builds an adapter wired with the given locker + reader.
// reader is a domain.SlipReader so callers can pass an untyped nil to exercise the
// "no reader configured" branch without an accidental typed-nil interface.
func newWriterAdapterWithDeps(store slippy.SlipStore, locker Locker, reader domain.SlipReader) *SlipWriterAdapter {
	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(testPipelineConfigJSON))
	if err != nil {
		panic("failed to parse test pipeline config: " + err.Error())
	}
	client := slippy.NewClientWithDependencies(store, &mockGitHubAPI{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})
	a := NewSlipWriterAdapter(client, locker, reader)
	// Shrink the poll wait so the timeout test is fast.
	a.lockWait = 300 * time.Millisecond
	return a
}

func dedupTestStore() *mockSlipStore {
	return &mockSlipStore{
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
		createFn: func(_ context.Context, _ *slippy.Slip) error { return nil },
	}
}

func dedupPushOpts() domain.PushOptions {
	return domain.PushOptions{CorrelationID: "corr-1", Repository: "Org/Repo", CommitSHA: "DEAD"}
}

func TestSlipWriterAdapter_Dedup_AcquiredSuccess_NoRelease(t *testing.T) {
	locker := &stubLocker{
		acquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return true, "tok", nil
		},
	}
	adapter := newWriterAdapterWithDeps(dedupTestStore(), locker, nil)

	result, err := adapter.CreateSlipForPush(context.Background(), dedupPushOpts())
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "corr-1", result.Slip.CorrelationID)
	assert.Equal(t, 0, locker.released, "successful create must NOT release the lock (TTL covers async-insert window)")
}

func TestSlipWriterAdapter_Dedup_AcquiredCreateFails_Releases(t *testing.T) {
	store := &mockSlipStore{
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
		createFn: func(_ context.Context, _ *slippy.Slip) error {
			return errors.New("clickhouse down")
		},
	}
	locker := &stubLocker{
		acquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return true, "tok", nil
		},
	}
	adapter := newWriterAdapterWithDeps(store, locker, nil)

	result, err := adapter.CreateSlipForPush(context.Background(), dedupPushOpts())
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Equal(t, 1, locker.released, "create failure must release the lock so a genuine retry can proceed")
}

func TestSlipWriterAdapter_Dedup_LockMiss_ReturnsExistingSlip(t *testing.T) {
	locker := &stubLocker{
		acquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", nil // duplicate in flight
		},
	}
	existing := &slippy.Slip{CorrelationID: "winner-corr", Repository: "Org/Repo", CommitSHA: "DEAD"}
	reader := &stubReader{
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return existing, nil // immediately visible, non-terminal (zero status)
		},
	}
	adapter := newWriterAdapterWithDeps(dedupTestStore(), locker, reader)

	result, err := adapter.CreateSlipForPush(context.Background(), dedupPushOpts())
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "winner-corr", result.Slip.CorrelationID, "lock-miss must return the in-flight winner's slip")
}

func TestSlipWriterAdapter_Dedup_LockMiss_TerminalExistingReturnsRetryable(t *testing.T) {
	// Deliberate design: on a lock-miss, a TERMINAL existing slip for the same
	// (repo, sha) is NOT returned as in-flight. The duplicate must instead get a
	// retryable error wrapping domain.ErrCreationInProgress (→ HTTP 409), which
	// self-heals once the lock TTL expires. See awaitExistingSlip's !IsTerminal guard.
	locker := &stubLocker{
		acquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", nil // duplicate in flight
		},
	}
	terminal := &slippy.Slip{
		CorrelationID: "terminal-corr",
		Repository:    "Org/Repo",
		CommitSHA:     "DEAD",
		Status:        slippy.SlipStatusCompleted, // terminal — must NOT be returned
	}
	reader := &stubReader{
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return terminal, nil
		},
	}
	adapter := newWriterAdapterWithDeps(dedupTestStore(), locker, reader)

	result, err := adapter.CreateSlipForPush(context.Background(), dedupPushOpts())
	assert.Nil(t, result, "terminal existing slip must not be returned as in-flight")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creation in progress, retry")
	assert.True(t, errors.Is(err, domain.ErrCreationInProgress),
		"lock-miss with a terminal existing slip must wrap domain.ErrCreationInProgress")
}

func TestSlipWriterAdapter_Dedup_LockMiss_TimeoutReturnsRetryable(t *testing.T) {
	locker := &stubLocker{
		acquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", nil
		},
	}
	reader := &stubReader{
		loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound // never becomes visible
		},
	}
	adapter := newWriterAdapterWithDeps(dedupTestStore(), locker, reader)

	result, err := adapter.CreateSlipForPush(context.Background(), dedupPushOpts())
	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creation in progress, retry")
	// Downstream (handler mapWriteError) classifies this as 409 via errors.Is.
	assert.True(t, errors.Is(err, domain.ErrCreationInProgress),
		"lock-miss timeout error must wrap domain.ErrCreationInProgress")
}

func TestSlipWriterAdapter_Dedup_LockMiss_NoReaderReturnsRetryable(t *testing.T) {
	locker := &stubLocker{
		acquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", nil
		},
	}
	adapter := newWriterAdapterWithDeps(dedupTestStore(), locker, nil) // nil reader

	result, err := adapter.CreateSlipForPush(context.Background(), dedupPushOpts())
	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creation in progress, retry")
	// Downstream (handler mapWriteError) classifies this as 409 via errors.Is.
	assert.True(t, errors.Is(err, domain.ErrCreationInProgress),
		"lock-miss (nil reader) error must wrap domain.ErrCreationInProgress")
}

func TestSlipWriterAdapter_Dedup_FailOpenOnLockerError(t *testing.T) {
	locker := &stubLocker{
		acquireFn: func(_ context.Context, _ string, _ time.Duration) (bool, string, error) {
			return false, "", errors.New("redis unavailable")
		},
	}
	adapter := newWriterAdapterWithDeps(dedupTestStore(), locker, nil)

	// FAIL-OPEN: locker error must not block creation.
	result, err := adapter.CreateSlipForPush(context.Background(), dedupPushOpts())
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "corr-1", result.Slip.CorrelationID)
	assert.Equal(t, 0, locker.released)
}
