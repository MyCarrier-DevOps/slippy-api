package middleware

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
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

// Addresses below are RFC 5737 documentation ranges. 198.51.100.0/24 (TEST-NET-2) stands in
// for the trusted Cloudflare edge; 203.0.113.x (TEST-NET-3) is a real client; 192.0.2.x
// (TEST-NET-1) is an attacker-supplied value. The behaviour these pin was established by
// measuring the real edge — see the design doc.
func testEdgeCIDR(t *testing.T) []*net.IPNet {
	t.Helper()
	_, cidr, err := net.ParseCIDR("198.51.100.0/24")
	require.NoError(t, err)
	return []*net.IPNet{cidr}
}

// The trust anchor is the rightmost X-Forwarded-For entry — the gateway appends its own TCP
// peer there, so a caller can never move it. Forwarded headers are believed only when that
// anchor proves the request transited a trusted edge.
func TestClientIdentity(t *testing.T) {
	edge := testEdgeCIDR(t)
	for _, tc := range []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		trusted    []*net.IPNet
		want       string
		wantOK     bool
	}{
		{
			name:    "via trusted edge, CF-Connecting-IP is the client",
			headers: map[string]string{"CF-Connecting-IP": "203.0.113.7", "X-Forwarded-For": "203.0.113.7,198.51.100.9"},
			trusted: edge,
			want:    "203.0.113.7", wantOK: true,
		},
		{
			name: "via trusted edge, forged left XFF entries do not matter",
			headers: map[string]string{
				"CF-Connecting-IP": "203.0.113.7",
				"X-Forwarded-For":  "192.0.2.1, 192.0.2.2,203.0.113.7,198.51.100.9",
			},
			trusted: edge,
			want:    "203.0.113.7", wantOK: true,
		},
		{
			name:    "via trusted edge, no CF header, XFF indexed from the right",
			headers: map[string]string{"X-Forwarded-For": "203.0.113.7,198.51.100.9"},
			trusted: edge,
			want:    "203.0.113.7", wantOK: true,
		},
		{
			// The bypass the fix closes: an attacker reaching the gateway directly forges
			// CF-Connecting-IP and the left of XFF, but the gateway still appends the
			// attacker's real peer on the right, and that peer is not a trusted edge — so the
			// forged header is ignored and the attacker is attributed to their own address.
			name: "direct to gateway, forged CF header is ignored",
			headers: map[string]string{
				"CF-Connecting-IP": "192.0.2.9",
				"X-Forwarded-For":  "192.0.2.9,203.0.113.50",
			},
			trusted: edge,
			want:    "203.0.113.50", wantOK: true,
		},
		{
			// The victim-lockout the fix closes: pinning CF-Connecting-IP to a victim off the
			// trusted path attributes to the attacker's own peer, not the victim.
			name: "direct to gateway, cannot pin a victim via CF header",
			headers: map[string]string{
				"CF-Connecting-IP": "203.0.113.200", // victim
				"X-Forwarded-For":  "203.0.113.50",  // attacker's real peer, gateway-appended
			},
			trusted: edge,
			want:    "203.0.113.50", wantOK: true,
		},
		{
			// With no trusted proxies configured nothing is believed beyond the rightmost
			// hop: safe, but edge traffic collapses onto the edge address. Documented.
			name:    "no trusted proxies falls back to the rightmost hop",
			headers: map[string]string{"CF-Connecting-IP": "203.0.113.7", "X-Forwarded-For": "203.0.113.7,198.51.100.9"},
			trusted: nil,
			want:    "198.51.100.9", wantOK: true,
		},
		{
			name:       "in-cluster caller uses its unforgeable RemoteAddr",
			remoteAddr: "10.2.15.97:41234",
			trusted:    edge,
			want:       "10.2.15.97", wantOK: true,
		},
		{
			name:    "X-Envoy-External-Address is never consulted",
			headers: map[string]string{"X-Envoy-External-Address": "203.0.113.7", "X-Forwarded-For": "203.0.113.7,198.51.100.9"},
			trusted: edge,
			// Resolves via XFF, not the Envoy header — same answer here, but the Envoy header
			// is not what produced it.
			want: "203.0.113.7", wantOK: true,
		},
		{
			name:    "an unparseable candidate is unattributable, not a shared bucket",
			headers: map[string]string{"X-Forwarded-For": "not-an-ip"},
			trusted: edge,
			want:    "", wantOK: false,
		},
		{
			name:    "an empty request is unattributable",
			trusted: edge,
			want:    "", wantOK: false,
		},
		{
			// A hostile CF-Connecting-IP with no XFF at all cannot invent an identity: with
			// no rightmost hop to anchor trust, only the real TCP peer counts.
			name:       "hostile CF header with no XFF resolves to RemoteAddr",
			headers:    map[string]string{"CF-Connecting-IP": "192.0.2.9"},
			remoteAddr: "10.2.15.97:41234",
			trusted:    edge,
			want:       "10.2.15.97", wantOK: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			got, ok := clientIdentity(h, tc.remoteAddr, defaultXFFDepth, tc.trusted)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The identity is stored as a truncated digest so a cache compromise does not yield a list
// of client addresses, mirroring keyFingerprint.
func TestIdentityKey(t *testing.T) {
	k := identityKey("203.0.113.7")
	assert.True(t, len(k) > len(rateLimitKeyPrefix), "key must carry a digest")
	assert.NotContains(t, k, "203.0.113.7", "the raw address must not be stored")
	assert.Equal(t, k, identityKey("203.0.113.7"), "must be stable")
	assert.NotEqual(t, k, identityKey("203.0.113.8"), "must distinguish addresses")
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
	api.UseMiddleware(NewAPIKeyAuth("read-key", "write-key",
		WithRateLimit(NewRateLimiter(store, 0, testEdgeCIDR(t)))))

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

// probeFrom exercises a caller whose real address is client, arriving through the trusted
// edge (198.51.100.9 is inside the test edge CIDR), so the identity resolves to client.
func probeFrom(handler http.Handler, token, client string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("X-Forwarded-For", client+",198.51.100.9")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func probe(handler http.Handler, token string) *httptest.ResponseRecorder {
	return probeFrom(handler, token, "203.0.113.7")
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

	// The third failure charges the first rung. It is still a 403 (credential rejection), and
	// it carries NO rate-limit headers — those would be an enforcement-state oracle on the
	// pre-lockout responses.
	w := probe(h, "wrong-key")
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Empty(t, w.Header().Get("X-RateLimit-Limit"), "401/403 must not disclose limiter state")

	// Now locked: the next attempt never reaches the credential comparison, and THIS is where
	// the rate-limit headers belong — enforcement is self-evident on a 429.
	//
	// Retry-After is the SECOND rung (20s), not the first, because that attempt was itself
	// charged before being refused — the self-defeating property, visible from the very first
	// 429 rather than only after repeated hammering.
	w = probe(h, "wrong-key")
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "20", w.Header().Get("Retry-After"))
	assert.Equal(t, strconv.Itoa(rateLimitFreeFailures), w.Header().Get("X-RateLimit-Limit"))
	assert.NotEmpty(t, w.Header().Get("X-RateLimit-Reset"))
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
	req.Header.Set("X-Forwarded-For", "203.0.113.7,198.51.100.9")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code, "the probe must survive a locked-out identity")
}

// Identities are tracked separately: one attacker must not throttle anyone else.
func TestRateLimit_IsPerIdentity(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	for range 10 {
		probeFrom(h, "wrong-key", "203.0.113.7") // one client climbs
	}

	// A different client, through the same edge, is unaffected.
	w := probeFrom(h, "read-key", "203.0.113.99")
	assert.Equal(t, http.StatusOK, w.Code, "an unrelated caller must be unaffected")
}

// A forged X-Forwarded-For must not let an attacker shed its ladder by rotating the header.
// The trust anchor is the rightmost hop (the trusted edge), so entries the caller invents on
// the left resolve to the same identity every time.
func TestRateLimit_ForgedXFFCannotEscapeTheLadder(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	// Same real client (203.0.113.7) through the edge, rotating forged entries on the left.
	for i, forged := range []string{"192.0.2.1", "192.0.2.2", "192.0.2.3", "192.0.2.4", "192.0.2.5"} {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		req.Header.Set("X-Forwarded-For", forged+",203.0.113.7,198.51.100.9")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if i >= rateLimitFreeFailures+1 {
			assert.Equal(t, http.StatusTooManyRequests, w.Code,
				"rotating the forged left-hand entry must not reset the ladder")
		}
	}
	assert.Len(t, store.failures, 1, "all attempts must land on one identity")
}

// The bypass the identity fix closes, exercised end to end: an attacker reaching the gateway
// directly and rotating a forged CF-Connecting-IP must still accrue on one identity — their
// own gateway-appended peer — rather than minting a fresh one per request.
func TestRateLimit_ForgedCFHeaderOffEdgeCannotEscapeTheLadder(t *testing.T) {
	store := newFakeStore()
	h := setupRateLimitedAPI(t, store)

	for _, forged := range []string{"192.0.2.10", "192.0.2.11", "192.0.2.12", "192.0.2.13"} {
		req := httptest.NewRequest(http.MethodGet, "/protected", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		req.Header.Set("CF-Connecting-IP", forged)
		// Rightmost is the attacker's real peer, NOT inside the trusted edge, so the forged
		// CF header is ignored and every request lands on 203.0.113.77.
		req.Header.Set("X-Forwarded-For", forged+",203.0.113.77")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		_ = w
	}
	assert.Len(t, store.failures, 1, "a rotating forged CF header must not mint fresh identities")
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
	l := NewRateLimiter(nil, 0, nil)
	_, err := l.Peek(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, l.Clear(context.Background(), nil))
}

// An out-of-range depth falls back to the measured default rather than indexing off the end
// of the header, where it would silently select nothing and collapse every caller onto one
// identity.
func TestNewRateLimiter_DepthFallback(t *testing.T) {
	for _, depth := range []int{0, -1, -100} {
		assert.Equal(t, defaultXFFDepth, NewRateLimiter(newFakeStore(), depth, nil).xffDepth)
	}
	assert.Equal(t, 3, NewRateLimiter(newFakeStore(), 3, nil).xffDepth)
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
			setRateLimitHeaders(rec, RateLimitState{RetryAfter: tc.retryAfter, Failures: 5, Exists: true})
			assert.Equal(t, tc.want, rec.headers["Retry-After"])
		})
	}
}

// No lockout means no Retry-After: the header is a promise about when to come back, and
// there is nothing to come back from.
func TestSetRateLimitHeaders_NoRetryAfterWhenUnlocked(t *testing.T) {
	rec := &stubHumaContext{}
	setRateLimitHeaders(rec, RateLimitState{Failures: 1, Exists: true})
	assert.Empty(t, rec.headers["Retry-After"])
	assert.Equal(t, "1", rec.headers["X-RateLimit-Remaining"])
}

// The enforcement-state oracle: without gating, an enabled-healthy, a disabled (zero-state),
// and an erroring limiter each produced a distinguishable header signature on an identical
// 401/403, telling an unauthenticated caller when the control was off or degraded. A state
// with no record must emit nothing, so those cases are indistinguishable on the wire.
func TestSetRateLimitHeaders_NoRecordEmitsNothing(t *testing.T) {
	rec := &stubHumaContext{}
	setRateLimitHeaders(rec, RateLimitState{}) // disabled / erroring / clean all look like this
	assert.Empty(t, rec.headers, "a stateless response must carry no rate-limit headers")
}

// End to end: a disabled limiter and a healthy one that has not yet recorded a failure must
// return byte-identical headers on a credential rejection, so the flag's state cannot be
// read off the response.
func TestRateLimit_DisabledAndCleanEnabledAreHeaderIdentical(t *testing.T) {
	// Disabled: no WithRateLimit option at all.
	muxOff := http.NewServeMux()
	apiOff := humago.New(muxOff, huma.DefaultConfig("Test", "1.0.0"))
	apiOff.UseMiddleware(NewAPIKeyAuth("read-key", "write-key"))
	huma.Register(apiOff, huma.Operation{
		OperationID: "protected", Method: http.MethodGet, Path: "/protected",
		Security: []map[string][]string{{"apiKey": {}}},
	}, func(_ context.Context, _ *struct{}) (*struct{ Body string }, error) {
		return &struct{ Body string }{Body: "ok"}, nil
	})

	enabled := setupRateLimitedAPI(t, newFakeStore())

	offResp := probeFrom(muxOff, "wrong-key", "203.0.113.7")
	onResp := probeFrom(enabled, "wrong-key", "203.0.113.7") // first failure: no record yet

	for _, hdr := range []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset", "Retry-After"} {
		assert.Equal(t, offResp.Header().Get(hdr), onResp.Header().Get(hdr),
			"%s must not reveal whether the limiter is enabled", hdr)
	}
}

// Coverage for the resolver's edge branches: a RemoteAddr with no port, and a trusted-edge
// chain too short for the configured depth (falls back to the unforgeable rightmost hop).
func TestClientIdentity_EdgeBranches(t *testing.T) {
	edge := testEdgeCIDR(t)

	t.Run("RemoteAddr without a port", func(t *testing.T) {
		got, ok := clientIdentity(http.Header{}, "10.2.15.97", defaultXFFDepth, edge)
		assert.True(t, ok)
		assert.Equal(t, "10.2.15.97", got)
	})

	t.Run("trusted edge chain shorter than the depth uses the rightmost hop", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-Forwarded-For", "198.51.100.9") // only the edge itself
		got, ok := clientIdentity(h, "", 2, edge)
		assert.True(t, ok)
		assert.Equal(t, "198.51.100.9", got)
	})

	t.Run("RemoteAddr that is neither host:port nor an IP is unattributable", func(t *testing.T) {
		got, ok := clientIdentity(http.Header{}, "garbage", defaultXFFDepth, edge)
		assert.False(t, ok)
		assert.Empty(t, got)
	})
}

// The per-request degradation signal: a store that errors on Fail must not break the request
// (fail-open) but must log, so an operator sees the brake is degraded on every request rather
// than only once at boot.
func TestRateLimit_StoreErrorOnFailIsLoggedNotFatal(t *testing.T) {
	store := newFakeStore()
	store.failErr = errors.New("dragonfly write failed")
	h := setupRateLimitedAPI(t, store)

	// A wrong key still gets a clean credential rejection, not a 500.
	assert.Equal(t, http.StatusForbidden, probe(h, "wrong-key").Code)
	// A correct key is still served.
	assert.Equal(t, http.StatusOK, probe(h, "read-key").Code)
}
