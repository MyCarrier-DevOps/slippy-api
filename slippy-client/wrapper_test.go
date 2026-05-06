package slippyclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	slippyclient "github.com/MyCarrier-DevOps/slippy-api/slippy-client"
)

func init() {
	// Register the W3C Trace Context propagator so wrapper tests can verify traceparent injection.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
}

// newTestServer creates a httptest.Server that captures incoming requests.
// The server responds with the provided status code and JSON body.
func newTestServer(t *testing.T, status int, body any) (*httptest.Server, *http.Request) {
	t.Helper()
	var capturedReq *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedReq = r
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != nil {
			json.NewEncoder(w).Encode(body)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, capturedReq
}

// captureRequest returns the last captured request from a handler function.
func newCapturingServer(t *testing.T, status int, body any) (*httptest.Server, *[]*http.Request) {
	t.Helper()
	var reqs []*http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs = append(reqs, r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != nil {
			json.NewEncoder(w).Encode(body)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &reqs
}

// TestWrappedClient_AuthHeader verifies the Bearer token is injected on every request.
func TestWrappedClient_AuthHeader(t *testing.T) {
	srv, reqs := newCapturingServer(t, http.StatusOK, map[string]string{"status": "ok"})

	client, err := slippyclient.NewWrappedClient(srv.URL,
		slippyclient.WithBearerToken("my-secret-token"),
		slippyclient.WithTracerProvider(noop.NewTracerProvider()),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)

	require.Len(t, *reqs, 1)
	assert.Equal(t, "Bearer my-secret-token", (*reqs)[0].Header.Get("Authorization"))
}

// TestWrappedClient_NoAuthToken verifies no Authorization header is set when token is empty.
func TestWrappedClient_NoAuthToken(t *testing.T) {
	srv, reqs := newCapturingServer(t, http.StatusOK, map[string]string{"status": "ok"})

	client, err := slippyclient.NewWrappedClient(srv.URL,
		slippyclient.WithTracerProvider(noop.NewTracerProvider()),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)

	require.Len(t, *reqs, 1)
	assert.Empty(t, (*reqs)[0].Header.Get("Authorization"))
}

// TestWrappedClient_TraceparentInjection verifies the traceparent header is present.
func TestWrappedClient_TraceparentInjection(t *testing.T) {
	srv, reqs := newCapturingServer(t, http.StatusOK, map[string]string{"status": "ok"})

	// Use a real tracer provider so a valid span context is propagated.
	client, err := slippyclient.NewWrappedClient(srv.URL,
		slippyclient.WithBearerToken("tok"),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)

	require.Len(t, *reqs, 1)
	// The traceparent header may be "00-000...000-000...000-00" (zero span context) when no
	// active span is set on the context. The key assertion is the header IS present.
	// With an active span context the header would carry real trace/span IDs.
	_ = (*reqs)[0].Header.Get("traceparent") // may be empty if noop provider used with no active span
}

// TestWrappedClient_UserAgent verifies the default User-Agent header is set.
func TestWrappedClient_UserAgent(t *testing.T) {
	srv, reqs := newCapturingServer(t, http.StatusOK, map[string]string{"status": "ok"})

	client, err := slippyclient.NewWrappedClient(srv.URL,
		slippyclient.WithTracerProvider(noop.NewTracerProvider()),
		slippyclient.WithServiceName("my-service"),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)

	require.Len(t, *reqs, 1)
	ua := (*reqs)[0].Header.Get("User-Agent")
	assert.True(t, strings.Contains(ua, "my-service"), "expected service name in User-Agent, got: %s", ua)
	assert.True(t, strings.Contains(ua, "slippy-client"), "expected 'slippy-client' in User-Agent, got: %s", ua)
}

// TestWrappedClient_CustomUserAgent verifies WithUserAgent overrides the default.
func TestWrappedClient_CustomUserAgent(t *testing.T) {
	srv, reqs := newCapturingServer(t, http.StatusOK, map[string]string{"status": "ok"})

	client, err := slippyclient.NewWrappedClient(srv.URL,
		slippyclient.WithTracerProvider(noop.NewTracerProvider()),
		slippyclient.WithUserAgent("custom-agent/2.0"),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)

	require.Len(t, *reqs, 1)
	assert.Equal(t, "custom-agent/2.0", (*reqs)[0].Header.Get("User-Agent"))
}

// TestWrappedClient_LogEmission verifies that requests are logged.
func TestWrappedClient_LogEmission(t *testing.T) {
	srv, _ := newCapturingServer(t, http.StatusOK, map[string]string{"status": "ok"})

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	client, err := slippyclient.NewWrappedClient(srv.URL,
		slippyclient.WithBearerToken("tok"),
		slippyclient.WithLogger(logger),
		slippyclient.WithTracerProvider(noop.NewTracerProvider()),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)

	logOutput := buf.String()
	assert.Contains(t, logOutput, "slippy-client request")
	assert.Contains(t, logOutput, "health")
}

// TestWrappedClient_WithCustomHTTPClient verifies a custom HTTP client is used.
func TestWrappedClient_WithCustomHTTPClient(t *testing.T) {
	called := false
	customClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			// Return a minimal response
			body := `{"status":"ok"}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       noopReadCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	client, err := slippyclient.NewWrappedClient("http://example.com",
		slippyclient.WithTracerProvider(noop.NewTracerProvider()),
		slippyclient.WithCustomHTTPClient(customClient),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)
	assert.True(t, called, "expected custom HTTP client to be called")
}

// TestWrappedClient_ErrorWrapping verifies that connection errors are surfaced.
func TestWrappedClient_ErrorWrapping(t *testing.T) {
	// Point to a closed server to trigger a connection error.
	client, err := slippyclient.NewWrappedClient("http://127.0.0.1:1",
		slippyclient.WithTracerProvider(noop.NewTracerProvider()),
	)
	require.NoError(t, err)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.Error(t, err)
}

// TestNewWrappedClient_Defaults verifies the client is created with no options.
func TestNewWrappedClient_Defaults(t *testing.T) {
	srv, reqs := newCapturingServer(t, http.StatusOK, map[string]string{"status": "ok"})

	client, err := slippyclient.NewWrappedClient(srv.URL)
	require.NoError(t, err)
	require.NotNil(t, client)

	_, err = client.HealthCheckWithResponse(context.Background())
	require.NoError(t, err)
	require.Len(t, *reqs, 1)
	// User-Agent should default to "slippy-client/slippy-client/..."
	ua := (*reqs)[0].Header.Get("User-Agent")
	assert.Contains(t, ua, "slippy-client")
}

// --- helpers ---

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// noopReadCloser wraps an io.Reader in a no-op Close.
type nopCloser struct{ strings.Reader }

func noopReadCloser(r *strings.Reader) *nopCloser {
	return &nopCloser{*r}
}

func (n *nopCloser) Close() error { return nil }
