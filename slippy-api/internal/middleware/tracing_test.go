package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/slippy-api/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// --- Auth Middleware Tracing Tests ---

// setupTracingTestAPI creates a minimal huma API with auth middleware and a
// protected endpoint for exercising the auth span logic.
func setupTracingTestAPI(apiKey string) http.Handler {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test", "1.0.0")
	api := humago.New(mux, cfg)
	api.UseMiddleware(NewAPIKeyAuth(apiKey, ""))

	// Register a protected endpoint.
	huma.Register(api, huma.Operation{
		OperationID: "test-op",
		Method:      http.MethodGet,
		Path:        "/protected",
		Security:    []map[string][]string{{"apiKey": {}}},
	}, func(_ context.Context, _ *struct{}) (*struct{ Body string }, error) {
		return &struct{ Body string }{Body: "ok"}, nil
	})

	return mux
}

func TestAuth_Success_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	handler := setupTracingTestAPI("test-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	spans := recorder.Ended()
	// Find the auth span.
	var found bool
	for _, span := range spans {
		if span.Name() == "auth.validateAPIKey" {
			found = true
			assert.Equal(t, codes.Ok, span.Status().Code)
			assertAuthAttr(t, span.Attributes(), "auth.result", "success")
			assertAuthAttr(t, span.Attributes(), "auth.scheme", "bearer")
			assertAuthAttr(t, span.Attributes(), "auth.operation", "test-op")
			break
		}
	}
	require.True(t, found, "expected an auth.validateAPIKey span")
}

func TestAuth_MissingToken_CreatesErrorSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	handler := setupTracingTestAPI("test-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	spans := recorder.Ended()
	var found bool
	for _, span := range spans {
		if span.Name() == "auth.validateAPIKey" {
			found = true
			assert.Equal(t, codes.Error, span.Status().Code)
			assertAuthAttr(t, span.Attributes(), "auth.result", "missing_token")
			break
		}
	}
	require.True(t, found, "expected an auth.validateAPIKey span")
}

func TestAuth_InvalidToken_CreatesErrorSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	handler := setupTracingTestAPI("test-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)

	spans := recorder.Ended()
	var found bool
	for _, span := range spans {
		if span.Name() == "auth.validateAPIKey" {
			found = true
			assert.Equal(t, codes.Error, span.Status().Code)
			assertAuthAttr(t, span.Attributes(), "auth.result", "invalid_token")
			break
		}
	}
	require.True(t, found, "expected an auth.validateAPIKey span")
}

func TestAuth_NoSecurity_NoSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test", "1.0.0")
	api := humago.New(mux, cfg)
	api.UseMiddleware(NewAPIKeyAuth("test-key", ""))

	// Register an unprotected endpoint (no Security requirement).
	huma.Register(api, huma.Operation{
		OperationID: "public-op",
		Method:      http.MethodGet,
		Path:        "/public",
	}, func(_ context.Context, _ *struct{}) (*struct{ Body string }, error) {
		return &struct{ Body string }{Body: "ok"}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/public", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// No auth span should exist — the middleware skipped auth for unprotected routes.
	spans := recorder.Ended()
	for _, span := range spans {
		assert.NotEqual(t, "auth.validateAPIKey", span.Name(),
			"auth span should not be created for unprotected endpoints")
	}
}

// --- Assertion helper ---

func assertAuthAttr(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.Equal(t, want, a.Value.AsString(), "attribute %q", key)
			return
		}
	}
	t.Errorf("attribute %q not found in span", key)
}
