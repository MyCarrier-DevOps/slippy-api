package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
)

// Rate-limit ladder parameters. See docs/superpowers/specs/2026-08-12-auth-rate-limiting-design.md
// for the reasoning behind each; the short version is below.
const (
	// rateLimitBase is the unit the Fibonacci multiplier scales. The floor matters far more
	// than the top of the ladder — a first penalty of 10s rather than 1s is the difference
	// between a cost and a rounding error, and every rung inherits it.
	rateLimitBase = 10 * time.Second

	// rateLimitFreeFailures are forgiven outright, covering a key rotation in flight or a
	// runner with a stale secret. A correct key never fails, so legitimate traffic never
	// reaches even this.
	rateLimitFreeFailures = 2

	// rateLimitMaxLockout caps the LOCKOUT DURATION — not the Fibonacci index. That is what
	// makes the computation safe: the sequence stops being generated once it passes this
	// value, so int64 overflow is structurally impossible rather than clamped away.
	rateLimitMaxLockout = 7 * 24 * time.Hour

	// rateLimitMinTTL floors how long a failure is remembered. Without it the free-allowance
	// phase would have a zero TTL (10 x 0), forgetting the first two failures instantly and
	// letting an attacker sit at 2 guesses per expiry forever, never entering the ladder.
	rateLimitMinTTL = time.Hour

	// rateLimitTTLMultiple is how far the record outlives its own lockout. Tying the two
	// together would defeat the ladder: an attacker who waits out a 10s lockout would find
	// the record gone and reset to zero. Waiting out 1x while the record survives 10x means
	// n advances instead, at every depth.
	rateLimitTTLMultiple = 10

	rateLimitKeyPrefix = "rl:auth:"

	// Canonical MIME spellings. http.Header.Get canonicalises its argument anyway, so these
	// are equivalent to the wire spellings "CF-Connecting-IP" and "X-Forwarded-For"; the
	// canonical form is used so the header names are greppable in one shape.
	headerCFConnectingIP = "Cf-Connecting-Ip"
	headerXForwardedFor  = "X-Forwarded-For"

	// defaultXFFDepth indexes X-Forwarded-For from the RIGHT. Measured against the real
	// edge: Cloudflare appends the true client IP to whatever the caller sent, then the
	// gateway appends Cloudflare's own address — so the client sits second from the right
	// and forged entries pile up harmlessly on the left.
	defaultXFFDepth = 2
)

// LockoutFor returns how long an identity is refused after n consecutive failures.
//
// The sequence is generated iteratively and abandoned as soon as it passes the cap, so no
// term large enough to overflow is ever computed. n itself may grow without limit.
func LockoutFor(failures int) time.Duration {
	k := failures - rateLimitFreeFailures
	if k <= 0 {
		return 0
	}
	// 1, 2, 3, 5, 8, ... — the duplicate leading 1 is skipped so consecutive rungs differ.
	prev, cur := 1, 2
	for range k - 1 {
		if time.Duration(cur)*rateLimitBase >= rateLimitMaxLockout {
			return rateLimitMaxLockout
		}
		prev, cur = cur, prev+cur
	}
	d := time.Duration(prev) * rateLimitBase
	return min(d, rateLimitMaxLockout)
}

// RecordTTL returns how long a failure record survives, given the lockout it produced.
func RecordTTL(lockout time.Duration) time.Duration {
	return max(lockout*rateLimitTTLMultiple, rateLimitMinTTL)
}

// clientIdentity resolves the caller this request should be attributed to, and reports
// whether the request could be attributed at all.
//
// The one anchor that cannot be forged is the RIGHTMOST X-Forwarded-For entry: the gateway
// appends the address of its own TCP peer there on every request, so whatever a caller sends
// is always to the LEFT of it. Everything else is only as trustworthy as the proof that the
// request actually transited the edge that set it.
//
//   - If the rightmost entry falls inside a configured trusted-proxy range (Cloudflare's
//     edge), the request provably came through Cloudflare, which sets CF-Connecting-IP to the
//     real client and rejects any attempt to forge it (403 at the edge, measured). So that
//     header — or, absent it, X-Forwarded-For indexed from the right — is the client.
//   - Otherwise the request reached the gateway directly, bypassing Cloudflare, and only the
//     rightmost entry is believable. It is the attacker's own TCP peer as the gateway saw it,
//     so it still identifies them; it just cannot be spoofed to someone else. Falling back to
//     CF-Connecting-IP here would be the bypass — rotate it per request and the ladder never
//     accrues — and falling back to RemoteAddr would collapse every off-edge caller onto the
//     shared ingress peer, one bucket an attacker fills in a couple of dozen requests.
//   - With no X-Forwarded-For at all the caller is in-cluster; its RemoteAddr is the real,
//     unforgeable TCP peer.
//
// Every candidate is validated with net.ParseIP. An unparseable one yields ("", false) so
// the limiter skips it rather than hashing a bogus string into a per-caller bucket — and,
// critically, rather than collapsing every unattributable request onto identityKey("").
//
// X-Envoy-External-Address is never consulted: measured at the pod it is the Cloudflare edge,
// not the client, and not stable per caller.
func clientIdentity(h http.Header, remoteAddr string, xffDepth int, trustedProxies []*net.IPNet) (string, bool) {
	entries := xffEntries(h.Get(headerXForwardedFor))

	if len(entries) > 0 {
		rightmost := entries[len(entries)-1]
		if transitedTrustedEdge(rightmost, trustedProxies) {
			if ip := validIP(h.Get(headerCFConnectingIP)); ip != "" {
				return ip, true
			}
			if idx := len(entries) - xffDepth; idx >= 0 && idx < len(entries) {
				if ip := validIP(entries[idx]); ip != "" {
					return ip, true
				}
			}
			// Depth ran off the front of a short chain; the edge address is coarse but
			// still a real, unforgeable value.
			return firstOK(validIP(rightmost))
		}
		// Direct to the gateway: trust only what the gateway itself appended.
		return firstOK(validIP(rightmost))
	}

	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return firstOK(validIP(host))
	}
	return firstOK(validIP(remoteAddr))
}

// xffEntries splits an X-Forwarded-For value into trimmed, non-empty hops.
func xffEntries(xff string) []string {
	if xff == "" {
		return nil
	}
	out := make([]string, 0, strings.Count(xff, ",")+1)
	for p := range strings.SplitSeq(xff, ",") {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// transitedTrustedEdge reports whether the gateway's TCP peer is a configured trusted proxy.
// With no trusted proxies configured this is always false, so the resolver falls back to the
// unforgeable rightmost entry — coarse for edge traffic, but never a bypass.
func transitedTrustedEdge(rightmost string, trustedProxies []*net.IPNet) bool {
	ip := net.ParseIP(rightmost)
	if ip == nil {
		return false
	}
	for _, cidr := range trustedProxies {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// validIP returns the canonical text of s when it parses as an IP, else "".
func validIP(s string) string {
	if ip := net.ParseIP(strings.TrimSpace(s)); ip != nil {
		return ip.String()
	}
	return ""
}

// firstOK turns a possibly-empty candidate into the (identity, attributable) pair.
func firstOK(candidate string) (string, bool) {
	return candidate, candidate != ""
}

// identityKey is the cache key for an identity, holding a truncated digest rather than the
// address itself so a cache compromise does not yield a list of client IPs. Same reasoning
// and truncation as keyFingerprint.
func identityKey(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return rateLimitKeyPrefix + hex.EncodeToString(sum[:])[:16]
}

// RateLimitState is an identity's standing with the limiter.
type RateLimitState struct {
	// Failures is the consecutive failure count after this call.
	Failures int
	// RetryAfter is the time remaining on the current lockout; zero means not locked.
	RetryAfter time.Duration
	// ResetAt is when the record ages out and the identity starts fresh.
	ResetAt time.Time
	// Exists reports whether a record was present, so a caller can skip a pointless delete.
	Exists bool
}

// Locked reports whether the identity is currently refused.
func (s RateLimitState) Locked() bool { return s.RetryAfter > 0 }

// Remaining is how many further failures are free before backoff begins.
func (s RateLimitState) Remaining() int {
	return max(rateLimitFreeFailures-s.Failures, 0)
}

// RateLimitStore persists per-identity failure counts.
//
// Declared here, with the consumer, and implemented in internal/infrastructure — the
// middleware states what it needs and the adapter satisfies it, so this package carries no
// dependency on Redis and the limiter is testable with a fake.
type RateLimitStore interface {
	// Fail records one failed attempt and returns the resulting state. It is called for
	// attempts made while ALREADY locked out too: persistence must be self-defeating, so a
	// caller hammering through a lockout drives its own ladder up rather than idling until
	// the penalty drains.
	Fail(ctx context.Context, key string) (RateLimitState, error)

	// Peek returns the current state without recording anything.
	Peek(ctx context.Context, key string) (RateLimitState, error)

	// Clear forgets an identity, called when it authenticates successfully.
	Clear(ctx context.Context, key string) error
}

// RateLimiter applies the backoff ladder to a request.
//
// It is deliberately fail-open: when the store errors the limiter stops limiting and says
// so in the log, rather than refusing traffic. A cache outage must not become an API
// outage, and authentication itself still fails closed without it — an unauthenticated
// caller is rejected whether or not the limiter is working.
type RateLimiter struct {
	store          RateLimitStore
	xffDepth       int
	trustedProxies []*net.IPNet
}

// NewRateLimiter builds a limiter over the given store. xffDepth indexes X-Forwarded-For
// from the right; pass 0 for the measured default. trustedProxies are the edge ranges whose
// forwarded headers may be believed — empty means none are, so callers are attributed by the
// unforgeable rightmost hop only (safe, but coarse for edge traffic).
func NewRateLimiter(store RateLimitStore, xffDepth int, trustedProxies []*net.IPNet) *RateLimiter {
	if xffDepth < 1 {
		xffDepth = defaultXFFDepth
	}
	return &RateLimiter{store: store, xffDepth: xffDepth, trustedProxies: trustedProxies}
}

// Peek reports the caller's current standing without recording an attempt.
func (l *RateLimiter) Peek(reqCtx context.Context, ctx huma.Context) (RateLimitState, error) {
	key, ok := l.key(ctx)
	if !ok {
		return RateLimitState{}, nil
	}
	return l.store.Peek(reqCtx, key)
}

// Fail records one failed attempt and returns the resulting standing.
func (l *RateLimiter) Fail(reqCtx context.Context, ctx huma.Context) (RateLimitState, error) {
	key, ok := l.key(ctx)
	if !ok {
		return RateLimitState{}, nil
	}
	return l.store.Fail(reqCtx, key)
}

// Clear forgets the caller after a successful authentication.
func (l *RateLimiter) Clear(reqCtx context.Context, ctx huma.Context) error {
	key, ok := l.key(ctx)
	if !ok {
		return nil
	}
	return l.store.Clear(reqCtx, key)
}

// key resolves the caller and returns the cache key it is tracked under, plus whether the
// request was attributable at all. A disabled limiter (nil receiver or nil store) and an
// unattributable request both report false, so every entry point above short-circuits
// without ever hashing an empty or bogus identity into a single shared bucket.
func (l *RateLimiter) key(ctx huma.Context) (string, bool) {
	if l == nil || l.store == nil {
		return "", false
	}
	h := http.Header{}
	for _, name := range []string{headerCFConnectingIP, headerXForwardedFor} {
		if v := ctx.Header(name); v != "" {
			h.Set(name, v)
		}
	}
	identity, ok := clientIdentity(h, ctx.RemoteAddr(), l.xffDepth, l.trustedProxies)
	if !ok {
		return "", false
	}
	return identityKey(identity), true
}

// setRateLimitHeaders publishes the caller's standing.
//
// Applied to 401, 403 and 429 only, never to a success. These describe a FAILURE budget,
// and there is no request quota on authenticated traffic — "X-RateLimit-Limit: 2" on a
// healthy 200 would tell every CI caller the API accepts two requests, which is false.
//
// Emitted only when there is a real record (st.Exists). This closes an enforcement-state
// oracle: without the guard, a limiter that was enabled-and-healthy, disabled, or erroring
// each produced a distinguishable header signature on an otherwise identical 401/403,
// telling an unauthenticated caller exactly when the control was off or degraded — i.e. when
// it was safe to flood. Gated, the disabled path (zero-value state), the erroring path (no
// call at all), and a clean first attempt all emit nothing identical; headers appear only
// once a caller has an actual record, and what they then disclose is that caller's own
// attempt history, which is not the secret.
func setRateLimitHeaders(ctx huma.Context, st RateLimitState) {
	if !st.Exists {
		return
	}
	ctx.SetHeader("X-RateLimit-Limit", strconv.Itoa(rateLimitFreeFailures))
	ctx.SetHeader("X-RateLimit-Remaining", strconv.Itoa(st.Remaining()))
	if !st.ResetAt.IsZero() {
		ctx.SetHeader("X-RateLimit-Reset", strconv.FormatInt(st.ResetAt.Unix(), 10))
	}
	if st.Locked() {
		// Seconds, rounded up: a client that retries at exactly the boundary must not
		// arrive early and take another rung for its trouble.
		secs := int(st.RetryAfter.Seconds())
		if st.RetryAfter%time.Second != 0 {
			secs++
		}
		ctx.SetHeader("Retry-After", strconv.Itoa(secs))
	}
}
