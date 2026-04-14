package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// shutdownQuickly calls shutdown with a short timeout so tests don't block
// waiting for the periodic metric reader to flush to a non-existent collector.
// Export errors during shutdown are expected in unit tests — the important
// assertion is that Init itself succeeded.
func shutdownQuickly(t *testing.T, shutdown Shutdown) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = shutdown(ctx)
}

func TestInit_SDKDisabled(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, shutdown, "should return a non-nil shutdown function")

	// Shutdown should be a no-op and succeed.
	assert.NoError(t, shutdown(context.Background()))
}

func TestInit_SDKDisabled_CaseInsensitive(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "TRUE")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	assert.NoError(t, shutdown(context.Background()))
}

func TestInit_NoEndpoint(t *testing.T) {
	// Ensure SDK is not disabled but endpoint is empty.
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestInit_UnsupportedProtocol(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "invalid-protocol")

	_, err := Init(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported OTEL_EXPORTER_OTLP_PROTOCOL")
}

func TestInit_GRPCProtocol(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_SERVICE_NAME", "test-service")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	shutdownQuickly(t, shutdown)
}

func TestInit_HTTPProtocol(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_SERVICE_NAME", "test-service-http")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	shutdownQuickly(t, shutdown)
}

func TestInit_DefaultProtocolIsGRPC(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "") // empty → defaults to gRPC

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	shutdownQuickly(t, shutdown)
}

func TestInit_HTTPSEndpoint(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.example.com:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	shutdownQuickly(t, shutdown)
}

func TestInit_K8sResourceAttributes(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_SERVICE_NAME", "slippy-api")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES_NODE_NAME", "node-1")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES_POD_NAME", "slippy-api-abc123")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES_POD_NAMESPACE", "ci")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES_POD_UID", "uid-456")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	shutdownQuickly(t, shutdown)
}

func TestInit_DefaultServiceName(t *testing.T) {
	// Verify init works when OTEL_SERVICE_NAME is not set (uses default).
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_SERVICE_NAME", "")

	shutdown, err := Init(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	shutdownQuickly(t, shutdown)
}

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		endpoint   string
		wantAddr   string
		wantSecure bool
	}{
		{"http scheme", "http://localhost:4317", "localhost:4317", false},
		{"https scheme", "https://collector.example.com:4317", "collector.example.com:4317", true},
		{"no scheme", "localhost:4317", "localhost:4317", false},
		{"http with path", "http://collector:4318/v1/traces", "collector:4318/v1/traces", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, secure := parseEndpoint(tt.endpoint)
			assert.Equal(t, tt.wantAddr, addr)
			assert.Equal(t, tt.wantSecure, secure)
		})
	}
}

func TestNoopShutdown(t *testing.T) {
	err := noopShutdown(context.Background())
	assert.NoError(t, err)
}

// TestNewMetricExporter_UnsupportedProtocol exercises the default branch of
// newMetricExporter directly. Init's metric-error branch is unreachable via
// the public entrypoint because newTraceExporter fails first on the same
// protocol, so test the metric exporter directly to avoid dead coverage.
func TestNewMetricExporter_UnsupportedProtocol(t *testing.T) {
	_, err := newMetricExporter(context.Background(), "invalid", "localhost:4317", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported OTEL_EXPORTER_OTLP_PROTOCOL")
}

// TestNewMetricExporter_GRPCAndHTTP exercises the grpc and http/protobuf
// branches of newMetricExporter directly. The underlying New() calls with
// WithInsecure succeed without a running collector.
func TestNewMetricExporter_GRPCAndHTTP(t *testing.T) {
	ctx := context.Background()
	exp, err := newMetricExporter(ctx, protocolGRPC, "localhost:4317", false)
	require.NoError(t, err)
	require.NotNil(t, exp)
	shutdownExporterQuickly(t, exp.Shutdown)

	exp, err = newMetricExporter(ctx, protocolHTTP, "localhost:4318", true)
	require.NoError(t, err)
	require.NotNil(t, exp)
	shutdownExporterQuickly(t, exp.Shutdown)
}

func shutdownExporterQuickly(t *testing.T, fn func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = fn(ctx)
}

// TestSetupTestTracing covers testutil.go: the helper installs an in-memory
// span recorder, returns it, and the cleanup restores the previous provider.
// Other packages' tests use this helper but their coverage does not count
// toward this package's statistics.
func TestSetupTestTracing(t *testing.T) {
	prev := otel.GetTracerProvider()
	recorder, cleanup := SetupTestTracing()
	require.NotNil(t, recorder)
	require.NotNil(t, cleanup)
	assert.NotEqual(t, prev, otel.GetTracerProvider(), "expected tracer provider to change")

	// Exercise the recorder: start and end a span, verify it is captured.
	_, span := otel.Tracer("testutil-test").Start(context.Background(), "op")
	span.End()
	assert.Len(t, recorder.Ended(), 1)

	cleanup()
	assert.Equal(t, prev, otel.GetTracerProvider(), "expected tracer provider to be restored")
}
