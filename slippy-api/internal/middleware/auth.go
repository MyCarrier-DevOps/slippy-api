package middleware

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"
)

// NewAPIKeyAuth returns a huma middleware that validates Bearer tokens against the
// configured API key using constant-time comparison. Operations that declare a
// "bearer" security requirement are protected; all others pass through.
func NewAPIKeyAuth(apiKey string) func(ctx huma.Context, next func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		// Only enforce auth on operations that declare a security requirement
		if len(ctx.Operation().Security) == 0 {
			next(ctx)
			return
		}

		token := extractBearerToken(ctx.Header("Authorization"))
		if token == "" {
			writeError(ctx, http.StatusUnauthorized, "missing or malformed Authorization header")
			return
		}

		if subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
			writeError(ctx, http.StatusForbidden, "invalid API key")
			return
		}

		next(ctx)
	}
}

// writeError writes a JSON error response without needing the huma.API reference.
func writeError(ctx huma.Context, status int, msg string) {
	ctx.SetStatus(status)
	ctx.SetHeader("Content-Type", "application/json")
	body := fmt.Sprintf(`{"status":%d,"title":"%s"}`, status, msg)
	_, _ = ctx.BodyWriter().Write([]byte(body))
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
