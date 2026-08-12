package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The ladder is the whole control, so it is pinned value by value rather than by
// re-deriving Fibonacci in the test — a test that recomputes the implementation proves
// only that two copies of the same mistake agree.
func TestLockoutFor(t *testing.T) {
	for _, tc := range []struct {
		failures int
		want     time.Duration
	}{
		{0, 0},
		{1, 0}, // free allowance
		{2, 0}, // free allowance
		{3, 10 * time.Second},
		{4, 20 * time.Second},
		{5, 30 * time.Second},
		{6, 50 * time.Second},
		{7, 80 * time.Second},
		{8, 130 * time.Second},
		{12, 890 * time.Second},
		{15, 3770 * time.Second},
		{20, 41810 * time.Second},
		{25, 463680 * time.Second},
		{26, rateLimitMaxLockout}, // first rung at the cap
		{27, rateLimitMaxLockout},
		{500, rateLimitMaxLockout}, // n keeps climbing; the duration does not
	} {
		t.Run(time.Duration(tc.failures).String(), func(t *testing.T) {
			assert.Equal(t, tc.want, LockoutFor(tc.failures))
		})
	}
}

// The cap applies to the resulting duration, not to the Fibonacci index, which is what
// makes overflow structurally impossible: the sequence stops being generated once it
// passes the cap rather than being clamped after the fact.
func TestLockoutFor_NeverExceedsTheCapOrGoesNegative(t *testing.T) {
	for n := range 2000 {
		d := LockoutFor(n)
		require.GreaterOrEqual(t, d, time.Duration(0), "failure %d produced a negative lockout", n)
		require.LessOrEqual(t, d, rateLimitMaxLockout, "failure %d exceeded the cap", n)
	}
}

func TestLockoutFor_IsMonotonic(t *testing.T) {
	prev := time.Duration(0)
	for n := range 40 {
		d := LockoutFor(n)
		require.GreaterOrEqual(t, d, prev, "the ladder went backwards at failure %d", n)
		prev = d
	}
}

// The record must outlive its own lockout by a wide margin or the ladder cannot climb: an
// attacker who waits out a 10s lockout would find the record gone, reset to n=0, and
// collect free guesses forever without ever passing the first rung.
func TestRecordTTL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		lockout time.Duration
		want    time.Duration
	}{
		{"free allowance floors at an hour", 0, time.Hour},
		{"short lockout still floors at an hour", 10 * time.Second, time.Hour},
		{"floor still binds below 6 minutes", 5 * time.Minute, time.Hour},
		{"ten times takes over above the floor", 890 * time.Second, 8900 * time.Second},
		{"at the cap", rateLimitMaxLockout, 10 * rateLimitMaxLockout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, RecordTTL(tc.lockout))
		})
	}
}

func TestRecordTTL_AlwaysOutlivesItsLockout(t *testing.T) {
	for n := range 60 {
		lockout := LockoutFor(n)
		require.Greater(t, RecordTTL(lockout), lockout,
			"failure %d: a record that expires with its lockout resets the ladder", n)
	}
}

// Measured against the real edge (see the design doc): Cloudflare APPENDS the true client
// IP to whatever X-Forwarded-For the caller sent, so forged entries land on the LEFT and
// survive. Indexing from the right is the only safe reading, and CF-Connecting-IP cannot be
// forged through Cloudflare at all.
func TestClientIdentity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		want       string
	}{
		{
			name:    "CF-Connecting-IP wins",
			headers: map[string]string{"CF-Connecting-IP": "184.97.153.128"},
			want:    "184.97.153.128",
		},
		{
			name: "CF-Connecting-IP wins even when XFF is forged",
			headers: map[string]string{
				"CF-Connecting-IP": "184.97.153.128",
				"X-Forwarded-For":  "1.2.3.4, 5.6.7.8,184.97.153.128,172.68.35.82",
			},
			want: "184.97.153.128",
		},
		{
			name:    "falls back to XFF indexed from the right",
			headers: map[string]string{"X-Forwarded-For": "184.97.153.128,172.68.35.82"},
			want:    "184.97.153.128",
		},
		{
			name:    "forged XFF entries on the left are ignored",
			headers: map[string]string{"X-Forwarded-For": "1.2.3.4, 5.6.7.8,184.97.153.128,172.68.35.82"},
			want:    "184.97.153.128",
		},
		{
			name:    "a single forged entry cannot shift the index",
			headers: map[string]string{"X-Forwarded-For": "9.9.9.9,184.97.153.128,172.68.35.82"},
			want:    "184.97.153.128",
		},
		{
			name:       "in-cluster caller with no proxy headers",
			remoteAddr: "10.2.15.97:41234",
			want:       "10.2.15.97",
		},
		{
			name:       "XFF too short to index falls back to RemoteAddr",
			headers:    map[string]string{"X-Forwarded-For": "1.2.3.4"},
			remoteAddr: "10.2.15.97:41234",
			want:       "10.2.15.97",
		},
		{
			name:       "empty CF header does not shadow XFF",
			headers:    map[string]string{"CF-Connecting-IP": "   "},
			remoteAddr: "10.2.15.97:41234",
			want:       "10.2.15.97",
		},
		{
			name: "X-Envoy-External-Address is never consulted",
			// Measured: Envoy resolves it to the Cloudflare edge, not the client, and the
			// edge address is not stable per caller. Using it would bucket every external
			// caller together.
			headers:    map[string]string{"X-Envoy-External-Address": "8.8.8.8"},
			remoteAddr: "10.2.15.97:41234",
			want:       "10.2.15.97",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			assert.Equal(t, tc.want, clientIdentity(h, tc.remoteAddr, defaultXFFDepth))
		})
	}
}

// The identity is stored as a truncated digest so a cache compromise does not yield a list
// of client addresses, mirroring keyFingerprint.
func TestIdentityKey(t *testing.T) {
	k := identityKey("184.97.153.128")
	assert.True(t, len(k) > len(rateLimitKeyPrefix), "key must carry a digest")
	assert.NotContains(t, k, "184.97.153.128", "the raw address must not be stored")
	assert.Equal(t, k, identityKey("184.97.153.128"), "must be stable")
	assert.NotEqual(t, k, identityKey("184.97.153.129"), "must distinguish addresses")
}

// --- Integration with the auth middleware ---

// fakeRateLimitStore is an in-memory RateLimitStore applying the real ladder, so the
// middleware tests exercise the actual policy rather than a stub that always says "locked".
type fakeRateLimitStore struct {
	failures map[string]int
	until    map[string]time.Time
	failErr  error
	peekErr  error
	cleared  int
}

func newFakeStore() *fakeRateLimitStore {
	return &fakeRateLimitStore{failures: map[string]int{}, until: map[string]time.Time{}}
}

func (f *fakeRateLimitStore) Fail(_ context.Context, key string) (RateLimitState, error) {
	if f.failErr != nil {
		return RateLimitState{}, f.failErr
	}
	f.failures[key]++
	lockout := LockoutFor(f.failures[key])
	f.until[key] = time.Now().Add(lockout)
	return RateLimitState{
		Failures:   f.failures[key],
		RetryAfter: lockout,
		ResetAt:    time.Now().Add(RecordTTL(lockout)),
		Exists:     true,
	}, nil
}

func (f *fakeRateLimitStore) Peek(_ context.Context, key string) (RateLimitState, error) {
	if f.peekErr != nil {
		return RateLimitState{}, f.peekErr
	}
	n, ok := f.failures[key]
	if !ok {
		return RateLimitState{}, nil
	}
	return RateLimitState{
		Failures:   n,
		RetryAfter: max(time.Until(f.until[key]), 0),
		ResetAt:    time.Now().Add(RecordTTL(LockoutFor(n))),
		Exists:     true,
	}, nil
}

func (f *fakeRateLimitStore) Clear(_ context.Context, key string) error {
	f.cleared++
	delete(f.failures, key)
	delete(f.until, key)
	return nil
}

func setupRateLimitedAPI(t *testing.T, store RateLimitStore) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Test", "1.0.0"))
	api.UseMiddleware(NewAPIKeyAuth("read-key", "write-key", WithRateLimit(NewRateLimiter(store, 0))))

	huma.Register(api, huma.Operation{
		OperationID: "protected", Method: http.MethodGet, Path: "/protected",
		Security: []map[string][]string{{"apiKey": {}}},
	}, func(_ context.Context, _ *struct{}) (*struct{ Body string }, error) {
		return &struct{ Body string }{Body: "ok"}, nil
	})
	huma.Register(api, huma.Operation{
		OperationID: "health-check", Method: http.MethodGet, Path: "/health",
	}, func(_ context.Context, _ *struct{}) (*struct{ Body string }, error) {
		return &struct{ Body string }{Body: "ok"}, nil
	})
	return mux
}

func probe(handler http.Handler, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("CF-Connecting-IP", "203.0.113.7")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

// The free allowance is spent, then the ladder starts. Two wrong keys cost nothing; the
// third is refused outright on the *next* attempt.
func TestRateLimit_LadderEngagesAfterTheFreeAllowance(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	// Failures 1 and 2 are free: rejected on the credential, not the limiter.
	for i := 1; i <= rateLimitFreeFailures; i++ {
		w := probe(h, "wrong-key")
		require.Equal(t, http.StatusForbidden, w.Code, "attempt %d must be a credential rejection", i)
		assert.Empty(t, w.Header().Get("Retry-After"), "attempt %d must not be locked out", i)
	}

	// The third failure charges the first rung.
	w := probe(h, "wrong-key")
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "0", w.Header().Get("X-RateLimit-Remaining"))

	// Now locked: the next attempt never reaches the credential comparison.
	//
	// Retry-After is the SECOND rung (20s), not the first, because that attempt was itself
	// charged before being refused — which is the self-defeating property, visible from the
	// very first 429 rather than only after repeated hammering.
	w = probe(h, "wrong-key")
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "20", w.Header().Get("Retry-After"))
}

// Persistence must be self-defeating: attempts made while already locked count and push the
// lockout further out, rather than idling until it drains.
func TestRateLimit_AttemptsWhileLockedExtendTheLockout(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	for range rateLimitFreeFailures + 1 {
		probe(h, "wrong-key")
	}
	require.Equal(t, http.StatusTooManyRequests, probe(h, "wrong-key").Code)

	first := store.failures[identityKey("203.0.113.7")]
	for range 5 {
		probe(h, "wrong-key")
	}
	after := store.failures[identityKey("203.0.113.7")]

	assert.Greater(t, after, first, "hammering a lockout must charge every attempt")
	w := probe(h, "wrong-key")
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.NotEqual(t, "20", w.Header().Get("Retry-After"), "the lockout must have grown")
}

// A correct key clears the record, so a caller that fixes its credential is serving again
// immediately rather than waiting out a penalty it no longer deserves.
func TestRateLimit_SuccessClearsTheRecord(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	probe(h, "wrong-key")
	require.NotEmpty(t, store.failures)

	w := probe(h, "read-key")
	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, store.failures, "a valid credential must clear the ladder")
	assert.Empty(t, w.Header().Get("X-RateLimit-Limit"),
		"a success advertises no quota — there is none on authenticated traffic")
}

// Normal traffic costs one read and no write: with no record present there is nothing to
// clear, so the common path does not write to the cache on every request.
func TestRateLimit_SuccessWithNoRecordDoesNotWrite(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	require.Equal(t, http.StatusOK, probe(h, "read-key").Code)
	assert.Zero(t, store.cleared, "a clean identity must not trigger a delete")
}

// A cache outage must not become an API outage. Auth still fails closed without the limiter.
func TestRateLimit_FailsOpenWhenTheStoreErrors(t *testing.T) {
	store := newFakeStore()
	store.peekErr = errors.New("dragonfly unreachable")
	h := setupRateLimitedAPI(t, store)

	assert.Equal(t, http.StatusOK, probe(h, "read-key").Code,
		"a valid credential must still be served")
	assert.Equal(t, http.StatusForbidden, probe(h, "wrong-key").Code,
		"an invalid credential must still be refused")
}

// Public routes have no credential to get wrong, so they can never enter the ladder —
// kubelet probes are unaffected at any depth.
func TestRateLimit_PublicRoutesAreNeverLimited(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	for range 20 {
		probe(h, "wrong-key")
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("CF-Connecting-IP", "203.0.113.7")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code, "the probe must survive a locked-out identity")
}

// Identities are tracked separately: one attacker must not throttle anyone else.
func TestRateLimit_IsPerIdentity(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	for range 10 {
		probe(h, "wrong-key") // 203.0.113.7 climbs
	}

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer read-key")
	req.Header.Set("CF-Connecting-IP", "198.51.100.4")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code, "an unrelated caller must be unaffected")
}

// A forged X-Forwarded-For must not let an attacker shed its ladder by rotating the header.
func TestRateLimit_ForgedXFFCannotEscapeTheLadder(t *testing.T) {
	store := newFakeStore()
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Test", "1.0.0"))
	api.UseMiddleware(NewAPIKeyAuth("read-key", "write-key", WithRateLimit(NewRateLimiter(store, 0))))
	huma.Register(api, huma.Operation{
		OperationID: "protected", Method: http.MethodGet, Path: "/protected",
		Security: []map[string][]string{{"apiKey": {}}},
	}, func(_ context.Context, _ *struct{}) (*struct{ Body string }, error) {
		return &struct{ Body string }{Body: "ok"}, nil
	})

	// Same real client each time (2nd from the right), rotating forged entries on the left.
	for i, forged := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5"} {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		req.Header.Set("X-Forwarded-For", forged+",203.0.113.9,172.68.35.82")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if i >= rateLimitFreeFailures+1 {
			assert.Equal(t, http.StatusTooManyRequests, w.Code,
				"rotating the forged left-hand entry must not reset the ladder")
		}
	}
	assert.Len(t, store.failures, 1, "all attempts must land on one identity")
}

// A nil limiter is the disabled configuration, and every entry point must tolerate it —
// that is what SLIPPY_RATE_LIMIT_ENABLED=false produces, and what a cache outage produces.
func TestRateLimiter_NilIsInert(t *testing.T) {
	var l *RateLimiter
	st, err := l.Peek(context.Background(), nil)
	require.NoError(t, err)
	assert.False(t, st.Exists)

	st, err = l.Fail(context.Background(), nil)
	require.NoError(t, err)
	assert.False(t, st.Locked())

	require.NoError(t, l.Clear(context.Background(), nil))
}

func TestRateLimiter_NilStoreIsInert(t *testing.T) {
	l := NewRateLimiter(nil, 0)
	_, err := l.Peek(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, l.Clear(context.Background(), nil))
}

// An out-of-range depth falls back to the measured default rather than indexing off the end
// of the header, where it would silently select nothing and collapse every caller onto one
// identity.
func TestNewRateLimiter_DepthFallback(t *testing.T) {
	for _, depth := range []int{0, -1, -100} {
		assert.Equal(t, defaultXFFDepth, NewRateLimiter(newFakeStore(), depth).xffDepth)
	}
	assert.Equal(t, 3, NewRateLimiter(newFakeStore(), 3).xffDepth)
}

// A failure to clear must not fail the request: the caller authenticated correctly, and a
// stale record only costs it a rung it will shed on the next success.
func TestRateLimit_ClearFailureDoesNotBreakASuccessfulRequest(t *testing.T) {
	store := &erroringClearStore{fakeRateLimitStore: newFakeStore()}
	h := setupRateLimitedAPI(t, store)

	probe(h, "wrong-key") // create a record so Clear is attempted
	assert.Equal(t, http.StatusOK, probe(h, "read-key").Code)
}

type erroringClearStore struct{ *fakeRateLimitStore }

func (e *erroringClearStore) Clear(context.Context, string) error {
	return errors.New("dragonfly write failed")
}

// Retry-After must round UP: a client obeying a truncated value arrives before the lockout
// has expired and is charged another rung for being punctual.
func TestSetRateLimitHeaders_RetryAfterRoundsUp(t *testing.T) {
	for _, tc := range []struct {
		retryAfter time.Duration
		want       string
	}{
		{10 * time.Second, "10"},
		{10500 * time.Millisecond, "11"},
		{1 * time.Millisecond, "1"},
	} {
		t.Run(tc.retryAfter.String(), func(t *testing.T) {
			rec := &stubHumaContext{}
			setRateLimitHeaders(rec, RateLimitState{RetryAfter: tc.retryAfter, Failures: 5})
			assert.Equal(t, tc.want, rec.headers["Retry-After"])
		})
	}
}

// No lockout means no Retry-After: the header is a promise about when to come back, and
// there is nothing to come back from.
func TestSetRateLimitHeaders_NoRetryAfterWhenUnlocked(t *testing.T) {
	rec := &stubHumaContext{}
	setRateLimitHeaders(rec, RateLimitState{Failures: 1})
	assert.Empty(t, rec.headers["Retry-After"])
	assert.Equal(t, "1", rec.headers["X-RateLimit-Remaining"])
}
