package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
