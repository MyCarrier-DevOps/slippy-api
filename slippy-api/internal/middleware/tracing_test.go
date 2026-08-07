package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/slippy-api/internal/telemetry"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

// setupNoSecurityTestAPI registers a single GET operation with no Security
// requirement at the given operation ID and path, so callers can vary the route
// identity the allowlist is keyed on.
func setupNoSecurityTestAPI(operationID, path string) http.Handler {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test", "1.0.0")
	api := humago.New(mux, cfg)
	api.UseMiddleware(NewAPIKeyAuth("test-key", ""))

	huma.Register(api, huma.Operation{
		OperationID: operationID,
		Method:      http.MethodGet,
		Path:        path,
	}, func(_ context.Context, _ *struct{}) (*struct{ Body string }, error) {
		return &struct{ Body string }{Body: "ok"}, nil
	})

	return mux
}

// TestAuth_NoSecurity_NotAllowlisted_CreatesErrorSpan asserts the fail-closed
// rejection is traced. Reaching it means a route shipped without a Security
// declaration, so the span is the signal an operator needs to find it.
func TestAuth_NoSecurity_NotAllowlisted_CreatesErrorSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	handler := setupNoSecurityTestAPI("undeclared-op", "/probe")

	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	var found bool
	for _, span := range recorder.Ended() {
		if span.Name() == "auth.rejectUndeclaredOperation" {
			found = true
			assert.Equal(t, codes.Error, span.Status().Code)
			assertAuthAttr(t, span.Attributes(), "auth.result", "not_allowlisted")
			assertAuthAttr(t, span.Attributes(), "auth.operation", "undeclared-op")
			break
		}
	}
	require.True(t, found, "expected an auth.rejectUndeclaredOperation span")
}

// TestAuth_NoSecurity_Allowlisted_NoSpan keeps the allowlisted path span-free.
// Health is polled continuously by kubelet; a span per probe is pure noise.
func TestAuth_NoSecurity_Allowlisted_NoSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	handler := setupNoSecurityTestAPI("health-check", "/health")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	for _, span := range recorder.Ended() {
		assert.NotContains(t, span.Name(), "auth.",
			"no auth span should be created for an allowlisted public operation")
	}
}

// TestAuth_Span_EndsBeforeHandlerRuns pins the auth span's lifetime to the credential
// check.
//
// The middleware calls next(ctx) to run the rest of the chain, so a `defer span.End()`
// in the closure body would keep the span open across the handler, the cache and the
// database — making auth.validateAPIKey's duration the whole request. It is not a parent
// of that work either (next receives the unmodified huma.Context), so a trace viewer
// shows a full-width auth bar beside the handler bar, which reads as "auth took 200ms".
//
// Two assertions, covering different things. The ordering assertion (auth ends before the
// handler starts) pins the span lifetime deterministically. The duration bound is not
// redundant with it: if a remote key lookup were ever added inside authorize(), the auth
// span could take 500ms and still close before the handler starts — ordering would pass,
// the bound would catch it. Measured over 500 runs including a saturated-core pass with
// no failures, and neither `make test` nor CI runs -race, so the bound is not marginal.
func TestAuth_Span_EndsBeforeHandlerRuns(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test", "1.0.0")
	api := humago.New(mux, cfg)
	api.UseMiddleware(NewAPIKeyAuth("test-key", ""))

	huma.Register(api, huma.Operation{
		OperationID: "slow-op",
		Method:      http.MethodGet,
		Path:        "/slow",
		Security:    []map[string][]string{{"apiKey": {}}},
	}, func(ctx context.Context, _ *struct{}) (*struct{ Body string }, error) {
		_, span := otel.Tracer("test").Start(ctx, "handler.slow")
		time.Sleep(20 * time.Millisecond)
		span.End()
		return &struct{ Body string }{Body: "ok"}, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/slow", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var authSpan, handlerSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		switch span.Name() {
		case "auth.validateAPIKey":
			authSpan = span
		case "handler.slow":
			handlerSpan = span
		}
	}
	require.NotNil(t, authSpan, "expected an auth.validateAPIKey span")
	require.NotNil(t, handlerSpan, "expected a handler.slow span")

	assert.False(t, authSpan.EndTime().After(handlerSpan.StartTime()),
		"auth.validateAPIKey must close before the handler runs; it ended at %s but the handler "+
			"started at %s, so its duration covers downstream work",
		authSpan.EndTime(), handlerSpan.StartTime())
	assert.Less(t, authSpan.EndTime().Sub(authSpan.StartTime()), 10*time.Millisecond,
		"auth span duration should be the credential check, not the 20ms handler")
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
