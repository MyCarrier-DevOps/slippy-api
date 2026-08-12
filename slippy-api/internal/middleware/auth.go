package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// authTracerName is the instrumentation scope for authentication operations.
const authTracerName = "slippy-api/auth"

// bearerChallenge is the WWW-Authenticate value on a 401. RFC 9110 §11.6.1 requires the
// header on every 401 so a client is told which scheme to use rather than guessing.
const bearerChallenge = `Bearer realm="slippy-api"`

// keyFingerprint returns a short, non-reversible handle for a presented credential.
//
// There is one shared bearer per tier, so without this the logs cannot distinguish a
// leaked key from the pipeline's own traffic, and a rotation is invisible. A truncated
// SHA-256 gives a stable identifier that separates key populations and makes "which
// credential was this" answerable — while never recording the credential. Truncation is
// deliberate: 48 bits is ample to tell a handful of keys apart and useless for recovering
// one, and recovering a key from the full digest would be no easier than brute-forcing
// the API directly.
func keyFingerprint(token string) string {
	if token == "" {
		return "-"
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])[:12]
}

const (
	// readAPIKeyScheme names the security scheme served at the read tier. It is the
	// only scheme name that does not escalate to the write key — see RequiresWriteKey.
	readAPIKeyScheme = "apiKey"
	// writeAPIKeyScheme names the security scheme served at the write tier.
	writeAPIKeyScheme = "writeApiKey"
)

// publicRoutes is the explicit allowlist of routes that may be served without a
// credential. Authentication is fail-closed: an operation that does not require a
// credential is rejected unless its route appears here, so a route registered
// without a Security declaration fails shut instead of shipping world-readable.
//
// Keys are "METHOD /path" — the same route identity net/http.ServeMux registers,
// because humago builds its mux pattern from the operation's method and its
// post-prefix path (adapters/humago.goAdapter.Handle), and the middleware sees that
// same modified operation. Two entries here are two distinct routes, not two names
// for one: the ("", "/v1") fan-out group registers the health probe on both
// prefixes, so adding a group prefix means adding a key.
//
// Keying on OperationID instead would make an entry a nickname rather than an
// identity, and a nickname can outlive its route. A renamed or regrouped handler
// strands the old ID (the v2 migration in docs/versioning-api.md does exactly this
// to "v1-health-check"), and any later operation that reused the stranded ID and
// omitted Security would be served with no credential. A method+path key cannot go
// stale silently: a stale entry matches no route, an ID collision is irrelevant, and
// a genuine duplicate registration panics in ServeMux before the process serves
// traffic — including for Hidden operations, which never reach the OpenAPI document
// the route audit walks.
//
// The allowlist is compile-time only, deliberately not sourced from config: the
// public surface cannot be widened by an environment variable.
//
// The spec and docs routes need no entry: huma registers those on the adapter
// directly, outside the middleware chain.
var publicRoutes = map[string]struct{}{
	"GET /health":    {},
	"GET /v1/health": {},
}

// AuthOption configures the middleware. Variadic rather than a wider signature so the
// existing call sites and every test fixture keep working unchanged.
type AuthOption func(*authConfig)

type authConfig struct {
	limiter *RateLimiter
}

// WithRateLimit applies per-identity Fibonacci backoff to failed authentication.
//
// Only failures accrue, so legitimate traffic never enters the ladder however much of it
// there is — which is the property that lets a limiter sit in front of a credential shared
// by the entire CI fleet without throttling it.
func WithRateLimit(limiter *RateLimiter) AuthOption {
	return func(c *authConfig) { c.limiter = limiter }
}

// NewAPIKeyAuth returns a huma middleware that validates Bearer tokens using a
// two-key scheme. Operations declaring "apiKey" security accept either the read
// key or the write key. Operations declaring "writeApiKey" security accept only
// the write key. Constant-time comparison is used for all token checks.
//
// Auth is fail-closed: an operation that does not require a credential is rejected
// with 401 unless its route is allowlisted in publicRoutes.
func NewAPIKeyAuth(readKey, writeKey string, opts ...AuthOption) func(ctx huma.Context, next func(huma.Context)) {
	cfg := &authConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	return func(ctx huma.Context, next func(huma.Context)) {
		op := ctx.Operation()

		// Requires no credential: serve it only if the route is explicitly public.
		//
		// Public routes never reach the limiter, and cannot: they have no credential to get
		// wrong, so they can never fail authentication. Kubelet probes are unaffected at
		// any ladder depth.
		if !RequiresCredential(op) {
			if IsPublicRoute(op.Method, op.Path) {
				next(ctx)
				return
			}
			rejectUndeclaredOperation(ctx, op)
			return
		}

		if !authorize(ctx, op, readKey, writeKey, cfg.limiter) {
			return
		}
		next(ctx)
	}
}

// authorize validates the bearer credential against the tier the operation declares,
// and reports whether the request may proceed. It writes the error response itself on
// failure, so a false return means the response is already complete.
//
// next is deliberately NOT called from here. The span this opens closes when the
// function returns, so its recorded duration is the credential check alone. Calling
// next inside the span — the shape this replaced — kept it open across the handler, the
// cache and the database, so auth.validateAPIKey reported total request latency while
// sitting as a fully-overlapping sibling of the handler span rather than its parent.
func authorize(
	ctx huma.Context,
	op *huma.Operation,
	readKey, writeKey string,
	limiter *RateLimiter,
) bool {
	opID := op.OperationID
	spanCtx, span := otel.Tracer(authTracerName).Start(ctx.Context(), "auth.validateAPIKey",
		trace.WithAttributes(
			attribute.String("auth.scheme", "bearer"),
			attribute.String("auth.operation", opID),
		),
	)
	defer span.End()

	token := extractBearerToken(ctx.Header("Authorization"))
	fingerprint := keyFingerprint(token)
	span.SetAttributes(attribute.String("auth.key_fingerprint", fingerprint))

	// The lockout is consulted BEFORE the credential comparison. Checking the credential
	// first would hand an attacker unlimited guesses, because the comparison is the very
	// thing being rate limited.
	limitState, limitErr := limiter.Peek(spanCtx, ctx)
	switch {
	case limitErr != nil:
		// Fail open. A cache outage must not become an API outage, and authentication
		// itself still fails closed below — an unrecognised credential is rejected whether
		// or not the limiter is working.
		slog.WarnContext(spanCtx, "auth: rate limiter unavailable, serving without it",
			"operation", opID, "error", limitErr)
	case limitState.Locked():
		// Attempts made while ALREADY locked count and extend the lockout, so persistence
		// is self-defeating rather than merely futile: a caller hammering through a
		// penalty drives its own ladder up instead of waiting for it to drain.
		if extended, err := limiter.Fail(spanCtx, ctx); err == nil {
			limitState = extended
		}
		span.SetAttributes(
			attribute.String("auth.result", "rate_limited"),
			attribute.Int("auth.failure_count", limitState.Failures),
		)
		span.SetStatus(codes.Error, "rate limited")
		slog.WarnContext(spanCtx, "auth: rate limited",
			"operation", opID, "result", "rate_limited", "key_fingerprint", fingerprint,
			"failures", limitState.Failures,
			"retry_after_seconds", int(limitState.RetryAfter.Seconds()))
		setRateLimitHeaders(ctx, limitState)
		// 429 immediately, never a server-side sleep: holding the connection open to
		// enforce the delay would hand the attacker a resource-exhaustion primitive
		// against the control meant to protect the service.
		writeError(ctx, http.StatusTooManyRequests, "too many failed authentication attempts")
		return false
	}

	// recordFailure charges one rung and publishes the caller's standing. Every credential
	// rejection below goes through it, so no failure path can forget to count.
	recordFailure := func() {
		if st, err := limiter.Fail(spanCtx, ctx); err == nil {
			setRateLimitHeaders(ctx, st)
		}
	}

	if token == "" {
		span.SetAttributes(attribute.String("auth.result", "missing_token"))
		span.SetStatus(codes.Error, "missing or malformed Authorization header")
		slog.WarnContext(spanCtx, "auth: missing bearer token",
			"operation", opID, "result", "missing_token", "key_fingerprint", fingerprint)
		recordFailure()
		writeError(ctx, http.StatusUnauthorized, "missing or malformed Authorization header")
		return false
	}

	if RequiresWriteKey(op) {
		// Write operations: only the write key is accepted.
		if writeKey == "" || subtle.ConstantTimeCompare([]byte(token), []byte(writeKey)) != 1 {
			// Separate a valid read key presented at the write tier from a token this
			// service does not recognise at all. The caller cannot tell the difference —
			// status, body and the absent challenge are identical, deliberately — but the
			// audit trail can, and without it a leaked read key probing mutations looks
			// exactly like ordinary garbage. That attribution is the reason the tier split
			// exists, so the refusal should record which of the two it was.
			result := "invalid_token"
			if readKey != "" && subtle.ConstantTimeCompare([]byte(token), []byte(readKey)) == 1 {
				result = "wrong_tier"
			}
			span.SetAttributes(attribute.String("auth.result", result))
			span.SetStatus(codes.Error, "invalid API key")
			slog.WarnContext(spanCtx, "auth: token refused for write operation",
				"operation", opID, "result", result, "required_level", "write",
				"key_fingerprint", fingerprint)
			recordFailure()
			writeError(ctx, http.StatusForbidden, "invalid API key")
			return false
		}
		span.SetAttributes(
			attribute.String("auth.result", "success"),
			attribute.String("auth.access_level", "write"),
		)
		slog.InfoContext(spanCtx, "auth: token accepted",
			"operation", opID, "access_level", "write", "key_fingerprint", fingerprint)
	} else {
		// Read operations: accept either the read key or the write key.
		readMatch := subtle.ConstantTimeCompare([]byte(token), []byte(readKey))
		writeMatch := 0
		if writeKey != "" {
			writeMatch = subtle.ConstantTimeCompare([]byte(token), []byte(writeKey))
		}
		if readMatch|writeMatch != 1 {
			span.SetAttributes(attribute.String("auth.result", "invalid_token"))
			span.SetStatus(codes.Error, "invalid API key")
			slog.WarnContext(spanCtx, "auth: invalid token for read operation",
				"operation", opID, "result", "invalid_token", "required_level", "read",
				"key_fingerprint", fingerprint)
			recordFailure()
			writeError(ctx, http.StatusForbidden, "invalid API key")
			return false
		}

		level := "read"
		if writeMatch == 1 {
			level = "write"
		}
		span.SetAttributes(
			attribute.String("auth.result", "success"),
			attribute.String("auth.access_level", level),
		)
		slog.InfoContext(spanCtx, "auth: token accepted",
			"operation", opID, "access_level", level, "key_fingerprint", fingerprint)
	}

	// A correct key restores service immediately: the record is dropped rather than left to
	// expire, so an identity that was climbing the ladder is clean the moment it presents a
	// valid credential. Skipped when no record existed, which is the overwhelmingly common
	// case, so normal traffic costs one read and no write.
	if limitState.Exists {
		if err := limiter.Clear(spanCtx, ctx); err != nil {
			slog.WarnContext(spanCtx, "auth: could not clear rate-limit record",
				"operation", opID, "error", err)
		}
	}

	span.SetStatus(codes.Ok, "")
	return true
}

// IsPublicRoute reports whether the given method and post-prefix path name a route
// that may be served without a credential. Exported so route registration can be
// audited against the shipped policy — see the route audit in main_test.go, which
// fails when a route neither requires a credential nor is allowlisted here.
func IsPublicRoute(method, path string) bool {
	_, ok := publicRoutes[routeKey(method, path)]
	return ok
}

// PublicRoutes returns the allowlisted route keys ("METHOD /path"), sorted.
//
// Exported for the reverse direction of the audit: checking that every entry still
// matches a registered route. Nothing else checks that direction, and it is the one
// that catches an entry left stranded by a rename or a regroup — dead allowlist
// config that a forward-only audit reports as correctly configured.
func PublicRoutes() []string {
	keys := make([]string, 0, len(publicRoutes))
	for key := range publicRoutes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// KnownSecuritySchemes returns the security scheme names this middleware tiers
// explicitly, sorted.
//
// Any name outside this set is enforced at the write tier by RequiresWriteKey, so a
// scheme added to the OpenAPI document without being added here silently becomes
// write-only. Exported so the route audit can pin the document's scheme set against
// what the middleware actually understands.
func KnownSecuritySchemes() []string {
	return []string{readAPIKeyScheme, writeAPIKeyScheme}
}

// rejectUndeclaredOperation refuses an operation that requires no credential and is
// not allowlisted as public. No credential can satisfy such an operation, so
// reaching here is a registration defect rather than a caller error: it is logged at
// error level and traced so the misconfiguration surfaces instead of being served or
// silently denied.
//
// The status is deliberately 401 and not 500, even though the diagnosis is
// server-side. A uniform 401 keeps the unauthenticated surface indistinguishable, so
// a prober cannot tell a misconfigured route from one that simply needs a key. The
// operator-facing signal is the error log and the span, not the status code.
func rejectUndeclaredOperation(ctx huma.Context, op *huma.Operation) {
	spanCtx, span := otel.Tracer(authTracerName).Start(ctx.Context(), "auth.rejectUndeclaredOperation",
		trace.WithAttributes(
			attribute.String("auth.operation", op.OperationID),
			attribute.String("auth.route", routeKey(op.Method, op.Path)),
			attribute.String("auth.result", "not_allowlisted"),
		),
	)
	defer span.End()

	span.SetStatus(codes.Error, "operation requires no credential and is not allowlisted as public")
	slog.ErrorContext(spanCtx, "auth: rejecting operation that requires no credential",
		"operation", op.OperationID, "route", routeKey(op.Method, op.Path), "result", "not_allowlisted")
	writeError(ctx, http.StatusUnauthorized, "authentication required")
}

// RequiresCredential reports whether the operation demands a credential at all.
//
// An empty Security list is the syntactic form of "no security". A list containing an
// empty requirement object ({}) is the semantic form: OpenAPI reads {} as "security
// optional", i.e. satisfiable with no credential, and huma documents it as such on
// Operation.Security. Testing only the syntactic form would let `Security: [{}]`
// route around the fail-closed default entirely — past the allowlist, into the read
// tier, with no error log and no rejection span — while the equivalent
// `Security: []` is refused. Both are treated as requiring no credential.
//
// Exported so the startup route check in main.go applies the same predicate the
// middleware enforces at request time, rather than reimplementing it.
func RequiresCredential(op *huma.Operation) bool {
	if len(op.Security) == 0 {
		return false
	}
	for _, req := range op.Security {
		if len(req) == 0 {
			return false
		}
	}
	return true
}

// RequiresWriteKey reports whether the operation must be served the write key.
//
// Exported so the startup route check can ask the middleware for its actual tiering
// decision rather than re-deriving it from scheme names. Re-deriving is the whole failure
// mode it guards against: an operation that should be write-tier but declares only
// "apiKey" is served at the read tier by design, and a caller inspecting the declaration
// itself would duplicate — and could drift from — the rule below.
//
// The default is inverted deliberately: an operation is served at the read tier only
// when every requirement names exactly readAPIKeyScheme and nothing else. Any other
// scheme name — a typo such as "writeAPIKey", or a scheme this middleware does not
// know — escalates to the write tier instead of falling through to the weaker check.
//
// The known set is deliberately not derived from Components.SecuritySchemes: a scheme
// can be present in the document and still be unknown here, and that combination
// must not be served at the read tier.
//
// The cost is availability, not authorization — every declaration this reclassifies
// moves read tier -> write tier, never the reverse. The realistic way to trip it is
// adding a second requirement to a working read route (`{apiKey, oauth2Read}` goes
// from read-key-accepted to 403), so a future read-tier scheme must be added to the
// condition below in the same change that adds it to the document. Scopes are
// ignored, so `{apiKey: ["read"]}` is unaffected.
func RequiresWriteKey(op *huma.Operation) bool {
	for _, req := range op.Security {
		for scheme := range req {
			if scheme != readAPIKeyScheme {
				return true
			}
		}
	}
	return false
}

// routeKey builds the "METHOD /path" allowlist key. The method is upper-cased to
// match net/http.ServeMux pattern conventions and huma's own registration, which
// upper-cases it too.
func routeKey(method, path string) string {
	return strings.ToUpper(method) + " " + path
}

// errorContentType is the media type of every error this middleware writes.
//
// It matches what the generated OpenAPI document declares for the default (error)
// response of every operation, and what huma itself emits for the errors it handles
// (validation failures, unknown routes). A generated client dispatching on media
// type — slippy-client is generated from exactly that document — would otherwise
// see an undeclared type on the two most common errors an API emits, missing and
// wrong credentials. The body already validates as an ErrorModel, which declares no
// required fields, so only the header needed correcting.
const errorContentType = "application/problem+json"

// writeError writes an error response without needing the huma.API reference.
//
// Headers are set BEFORE the status, and the order is load-bearing: humago's
// SetStatus calls http.ResponseWriter.WriteHeader immediately, which flushes the
// header block, so anything set afterwards never reaches the wire. Setting
// Content-Type second — as this did originally — meant every auth response went out
// content-sniffed as text/plain despite carrying a JSON body.
func writeError(ctx huma.Context, status int, msg string) {
	ctx.SetHeader("Content-Type", errorContentType)
	ctx.SetHeader("X-Content-Type-Options", "nosniff")
	// Only on 401, which is the sole status RFC 9110 §11.6.1 attaches the challenge to.
	//
	// A 403 is left unchallenged because the caller has already presented a credential and
	// a challenge would only prompt re-presenting it. Note what this does NOT assert: the
	// middleware never determines whether the credential was *understood*. The write arm
	// refuses a valid read key and an unrecognised string with the same status, the same
	// body and the same silence — only the auth.result attribute (wrong_tier vs
	// invalid_token) and the key fingerprint separate them, and both go to the audit trail
	// rather than the wire.
	if status == http.StatusUnauthorized {
		ctx.SetHeader("WWW-Authenticate", bearerChallenge)
	}
	ctx.SetStatus(status)
	body := fmt.Sprintf(`{"status":%d,"title":%q}`, status, msg)
	if _, writeErr := ctx.BodyWriter().Write([]byte(body)); writeErr != nil {
		log.Printf("warning: failed to write error response: %v", writeErr)
	}
}

// extractBearerToken extracts the token from an "Authorization: Bearer <token>" header.
// Returns empty string if the header is missing or malformed.
func extractBearerToken(header string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
