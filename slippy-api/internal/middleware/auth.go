package middleware

import (
	"crypto/subtle"
	"fmt"
	"log"
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

// NewAPIKeyAuth returns a huma middleware that validates Bearer tokens using a
// two-key scheme. Operations declaring "apiKey" security accept either the read
// key or the write key. Operations declaring "writeApiKey" security accept only
// the write key. Constant-time comparison is used for all token checks.
func NewAPIKeyAuth(readKey, writeKey string) func(ctx huma.Context, next func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		// Only enforce auth on operations that declare a security requirement.
		if len(ctx.Operation().Security) == 0 {
			next(ctx)
			return
		}

		// Start a span for the authentication check.
		reqCtx := ctx.Context()
		_, span := otel.Tracer(authTracerName).Start(reqCtx, "auth.validateAPIKey",
			trace.WithAttributes(
				attribute.String("auth.scheme", "bearer"),
				attribute.String("auth.operation", ctx.Operation().OperationID),
			),
		)
		defer span.End()

		token := extractBearerToken(ctx.Header("Authorization"))
		if token == "" {
			span.SetAttributes(attribute.String("auth.result", "missing_token"))
			span.SetStatus(codes.Error, "missing or malformed Authorization header")
			writeError(ctx, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		if requiresWriteAccess(ctx.Operation()) {
			// Write operations: only the write key is accepted.
			if writeKey == "" || subtle.ConstantTimeCompare([]byte(token), []byte(writeKey)) != 1 {
				span.SetAttributes(attribute.String("auth.result", "invalid_token"))
				span.SetStatus(codes.Error, "invalid API key")
				writeError(ctx, http.StatusForbidden, "invalid API key")
				return
			}
			span.SetAttributes(
				attribute.String("auth.result", "success"),
				attribute.String("auth.access_level", "write"),
			)
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
		}

		span.SetStatus(codes.Ok, "")
		next(ctx)
	}
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
