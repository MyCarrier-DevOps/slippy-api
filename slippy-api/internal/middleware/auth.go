package middleware

import (
	"crypto/subtle"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// authTracerName is the instrumentation scope for authentication operations.
const authTracerName = "slippy-api/auth"

// publicOperationIDs is the explicit allowlist of operations that may be served
// without a credential. Authentication is fail-closed: an operation declaring no
// Security requirement is rejected unless its OperationID appears here, so a
// route registered without a Security declaration fails shut instead of shipping
// world-readable.
//
// The allowlist is compile-time only, deliberately not sourced from config: the
// public surface cannot be widened by an environment variable.
//
// The health route is registered on the ("", "/v1") group, which fans one
// operation out to both prefixes and prefixes the versioned ID — hence two
// entries for a single handler (see huma.PrefixModifier).
//
// The OpenAPI and docs routes need no entry: huma registers those on the adapter
// directly, outside the middleware chain.
var publicOperationIDs = map[string]struct{}{
	"health-check":    {},
	"v1-health-check": {},
}

// NewAPIKeyAuth returns a huma middleware that validates Bearer tokens using a
// two-key scheme. Operations declaring "apiKey" security accept either the read
// key or the write key. Operations declaring "writeApiKey" security accept only
// the write key. Constant-time comparison is used for all token checks.
//
// Auth is fail-closed: an operation that declares no security requirement is
// rejected with 401 unless it is named in publicOperationIDs.
func NewAPIKeyAuth(readKey, writeKey string) func(ctx huma.Context, next func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		op := ctx.Operation()
		opID := op.OperationID

		// No security requirement: serve it only if it is explicitly public.
		if len(op.Security) == 0 {
			if IsPublicOperation(opID) {
				next(ctx)
				return
			}
			rejectUndeclaredOperation(ctx, opID)
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

		if requiresWriteAccess(ctx.Operation()) {
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

// IsPublicOperation reports whether the named operation is on the public
// allowlist and may therefore be served without a credential. Exported so route
// registration can be audited against the shipped policy — see the route audit
// in main_test.go, which fails when a route declares neither Security nor an
// allowlist entry.
func IsPublicOperation(operationID string) bool {
	_, ok := publicOperationIDs[operationID]
	return ok
}

// rejectUndeclaredOperation refuses an operation that declares no security
// requirement and is not allowlisted as public. No credential can satisfy such an
// operation, so reaching here is a registration defect rather than a caller
// error: it is logged at error level and traced so the misconfiguration surfaces
// instead of being served or silently denied.
func rejectUndeclaredOperation(ctx huma.Context, opID string) {
	spanCtx, span := otel.Tracer(authTracerName).Start(ctx.Context(), "auth.rejectUndeclaredOperation",
		trace.WithAttributes(
			attribute.String("auth.operation", opID),
			attribute.String("auth.result", "not_allowlisted"),
		),
	)
	defer span.End()

	span.SetStatus(codes.Error, "operation declares no security requirement and is not allowlisted as public")
	slog.ErrorContext(spanCtx, "auth: rejecting operation that declares no security requirement",
		"operation", opID, "result", "not_allowlisted")
	writeError(ctx, http.StatusUnauthorized, "authentication required")
}

// requiresWriteAccess returns true if the operation declares a "writeApiKey"
// security requirement.
func requiresWriteAccess(op *huma.Operation) bool {
	for _, req := range op.Security {
		if _, ok := req["writeApiKey"]; ok {
			return true
		}
	}
	return false
}

// writeError writes a JSON error response without needing the huma.API reference.
func writeError(ctx huma.Context, status int, msg string) {
	ctx.SetStatus(status)
	ctx.SetHeader("Content-Type", "application/json")
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
