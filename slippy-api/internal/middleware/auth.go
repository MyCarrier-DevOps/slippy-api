package middleware

import (
	"crypto/subtle"
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

const (
	// readAPIKeyScheme names the security scheme served at the read tier. It is the
	// only scheme name that does not escalate to the write key — see requiresWriteKey.
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

// NewAPIKeyAuth returns a huma middleware that validates Bearer tokens using a
// two-key scheme. Operations declaring "apiKey" security accept either the read
// key or the write key. Operations declaring "writeApiKey" security accept only
// the write key. Constant-time comparison is used for all token checks.
//
// Auth is fail-closed: an operation that does not require a credential is rejected
// with 401 unless its route is allowlisted in publicRoutes.
func NewAPIKeyAuth(readKey, writeKey string) func(ctx huma.Context, next func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		op := ctx.Operation()
		opID := op.OperationID

		// Requires no credential: serve it only if the route is explicitly public.
		if !requiresCredential(op) {
			if IsPublicRoute(op.Method, op.Path) {
				next(ctx)
				return
			}
			rejectUndeclaredOperation(ctx, op)
			return
		}

		// Start a span for the authentication check.
		reqCtx := ctx.Context()
		spanCtx, span := otel.Tracer(authTracerName).Start(reqCtx, "auth.validateAPIKey",
			trace.WithAttributes(
				attribute.String("auth.scheme", "bearer"),
				attribute.String("auth.operation", opID),
			),
		)
		defer span.End()

		token := extractBearerToken(ctx.Header("Authorization"))
		if token == "" {
			span.SetAttributes(attribute.String("auth.result", "missing_token"))
			span.SetStatus(codes.Error, "missing or malformed Authorization header")
			slog.WarnContext(spanCtx, "auth: missing bearer token",
				"operation", opID, "result", "missing_token")
			writeError(ctx, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		if requiresWriteKey(op) {
			// Write operations: only the write key is accepted.
			if writeKey == "" || subtle.ConstantTimeCompare([]byte(token), []byte(writeKey)) != 1 {
				span.SetAttributes(attribute.String("auth.result", "invalid_token"))
				span.SetStatus(codes.Error, "invalid API key")
				slog.WarnContext(spanCtx, "auth: invalid token for write operation",
					"operation", opID, "result", "invalid_token", "required_level", "write")
				writeError(ctx, http.StatusForbidden, "invalid API key")
				return
			}
			span.SetAttributes(
				attribute.String("auth.result", "success"),
				attribute.String("auth.access_level", "write"),
			)
			slog.InfoContext(spanCtx, "auth: token accepted",
				"operation", opID, "access_level", "write")
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
					"operation", opID, "result", "invalid_token", "required_level", "read")
				writeError(ctx, http.StatusForbidden, "invalid API key")
				return
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
				"operation", opID, "access_level", level)
		}

		span.SetStatus(codes.Ok, "")
		next(ctx)
	}
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
// Any name outside this set is enforced at the write tier by requiresWriteKey, so a
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

// requiresCredential reports whether the operation demands a credential at all.
//
// An empty Security list is the syntactic form of "no security". A list containing an
// empty requirement object ({}) is the semantic form: OpenAPI reads {} as "security
// optional", i.e. satisfiable with no credential, and huma documents it as such on
// Operation.Security. Testing only the syntactic form would let `Security: [{}]`
// route around the fail-closed default entirely — past the allowlist, into the read
// tier, with no error log and no rejection span — while the equivalent
// `Security: []` is refused. Both are treated as requiring no credential.
func requiresCredential(op *huma.Operation) bool {
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

// requiresWriteKey reports whether the operation must be served the write key.
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
func requiresWriteKey(op *huma.Operation) bool {
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

// writeError writes a JSON error response without needing the huma.API reference.
//
// Headers are set BEFORE the status, and the order is load-bearing: humago's
// SetStatus calls http.ResponseWriter.WriteHeader immediately, which flushes the
// header block, so anything set afterwards never reaches the wire. Setting
// Content-Type second — as this did originally — meant every auth response went out
// content-sniffed as text/plain despite carrying a JSON body.
func writeError(ctx huma.Context, status int, msg string) {
	ctx.SetHeader("Content-Type", "application/json")
	ctx.SetHeader("X-Content-Type-Options", "nosniff")
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
