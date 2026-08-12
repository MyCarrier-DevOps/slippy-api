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

// lockoutFor returns how long an identity is refused after n consecutive failures.
//
// The sequence is generated iteratively and abandoned as soon as it passes the cap, so no
// term large enough to overflow is ever computed. n itself may grow without limit.
func lockoutFor(failures int) time.Duration {
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

// recordTTL returns how long a failure record survives, given the lockout it produced.
func recordTTL(lockout time.Duration) time.Duration {
	return max(lockout*rateLimitTTLMultiple, rateLimitMinTTL)
}

// LadderArgs renders the whole policy as script arguments: the free allowance, then one
// (lockout, ttl) pair per rung, both in seconds, starting at rung zero.
//
// Exported because the store lives in another package, and shaped this way so the store has
// no policy in it at all — it indexes a table rather than recomputing Fibonacci and the TTL
// rule in a second language, where the copy would be free to drift from the one under test.
//
// The table runs from rung zero to the first that reaches the cap; anything beyond indexes
// to the last pair, which is the cap and its TTL.
func LadderArgs() []any {
	args := []any{rateLimitFreeFailures}
	for k := 0; ; k++ {
		d := lockoutFor(k + rateLimitFreeFailures)
		args = append(args, int(d.Seconds()), int(recordTTL(d).Seconds()))
		if d >= rateLimitMaxLockout {
			return args
		}
	}
}

// clientIdentity resolves the caller this request should be attributed to.
//
// Order matters and is evidence-based rather than conventional:
//
//  1. CF-Connecting-IP. Cloudflare rejects a request that tries to set this header (403 at
//     the edge, measured), so it cannot be forged by an internet caller.
//  2. X-Forwarded-For indexed from the RIGHT. Cloudflare appends the true client to
//     whatever the caller sent, so attacker-controlled entries are always to the LEFT of
//     the real one. Reading the leftmost entry — the conventional choice — would let an
//     attacker rotate the header per request and never accrue a penalty at all.
//  3. RemoteAddr. In-cluster callers bypass Cloudflare entirely, and the pod's TCP peer
//     cannot be forged.
//
// X-Envoy-External-Address is deliberately NOT consulted: measured at the pod it carries
// the Cloudflare edge address, not the client, and the edge is not stable per caller — so
// it would bucket unrelated callers together.
func clientIdentity(h http.Header, remoteAddr string, xffDepth int) string {
	if cf := strings.TrimSpace(h.Get(headerCFConnectingIP)); cf != "" {
		return cf
	}

	if xff := h.Get(headerXForwardedFor); xff != "" {
		parts := strings.Split(xff, ",")
		// Index from the right: parts[len-1] is the hop that reached us, parts[len-depth]
		// the address that many hops out.
		if idx := len(parts) - xffDepth; idx >= 0 && idx < len(parts) {
			if v := strings.TrimSpace(parts[idx]); v != "" {
				return v
			}
		}
	}

	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
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
	store    RateLimitStore
	xffDepth int
}

// NewRateLimiter builds a limiter over the given store. xffDepth indexes X-Forwarded-For
// from the right; pass 0 for the measured default.
func NewRateLimiter(store RateLimitStore, xffDepth int) *RateLimiter {
	if xffDepth < 1 {
		xffDepth = defaultXFFDepth
	}
	return &RateLimiter{store: store, xffDepth: xffDepth}
}

// Peek reports the caller's current standing without recording an attempt.
func (l *RateLimiter) Peek(reqCtx context.Context, ctx huma.Context) (RateLimitState, error) {
	if l == nil || l.store == nil {
		return RateLimitState{}, nil
	}
	return l.store.Peek(reqCtx, l.key(ctx))
}

// Fail records one failed attempt and returns the resulting standing.
func (l *RateLimiter) Fail(reqCtx context.Context, ctx huma.Context) (RateLimitState, error) {
	if l == nil || l.store == nil {
		return RateLimitState{}, nil
	}
	return l.store.Fail(reqCtx, l.key(ctx))
}

// Clear forgets the caller after a successful authentication.
func (l *RateLimiter) Clear(reqCtx context.Context, ctx huma.Context) error {
	if l == nil || l.store == nil {
		return nil
	}
	return l.store.Clear(reqCtx, l.key(ctx))
}

// key resolves the caller and returns the cache key it is tracked under.
func (l *RateLimiter) key(ctx huma.Context) string {
	h := http.Header{}
	for _, name := range []string{headerCFConnectingIP, headerXForwardedFor} {
		if v := ctx.Header(name); v != "" {
			h.Set(name, v)
		}
	}
	return identityKey(clientIdentity(h, ctx.RemoteAddr(), l.xffDepth))
}

// setRateLimitHeaders publishes the caller's standing.
//
// Applied to 401, 403 and 429 only, never to a success. These describe a FAILURE budget,
// and there is no request quota on authenticated traffic — "X-RateLimit-Limit: 2" on a
// healthy 200 would tell every CI caller the API accepts two requests, which is false.
func setRateLimitHeaders(ctx huma.Context, st RateLimitState) {
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
