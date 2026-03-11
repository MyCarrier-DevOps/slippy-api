package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/MyCarrier-DevOps/slippy-api/internal/config"
	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
	"github.com/MyCarrier-DevOps/slippy-api/internal/infrastructure"
)

// --- Stub SlipReader for tests ---

// stubSlipReader implements domain.SlipReader with hardcoded test data.
type stubSlipReader struct {
	slips map[string]*domain.Slip
}

func newStubSlipReader() *stubSlipReader {
	return &stubSlipReader{slips: map[string]*domain.Slip{
		"test-corr-001": {
			CorrelationID: "test-corr-001",
			Repository:    "org/my-service",
			Branch:        "main",
			CommitSHA:     "abc123",
		},
	}}
}

func (s *stubSlipReader) Load(_ context.Context, correlationID string) (*domain.Slip, error) {
	slip, ok := s.slips[correlationID]
	if !ok {
		return nil, errors.New("not found")
	}
	return slip, nil
}

func (s *stubSlipReader) LoadByCommit(_ context.Context, _, _ string) (*domain.Slip, error) {
	return nil, errors.New("not implemented")
}

func (s *stubSlipReader) FindByCommits(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
	return nil, "", errors.New("not implemented")
}

func (s *stubSlipReader) FindAllByCommits(_ context.Context, _ string, _ []string) ([]domain.SlipWithCommit, error) {
	return nil, errors.New("not implemented")
}

// --- buildHandler tests ---

func TestBuildHandler_HealthEndpoint(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil)
	require.NotNil(t, h)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestBuildHandler_AuthRequired(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil)

	// Request without auth header should be rejected
	req := httptest.NewRequest(http.MethodGet, "/slips/test-corr-001", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestBuildHandler_AuthSuccess(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil)

	// Request with valid auth header should succeed
	req := httptest.NewRequest(http.MethodGet, "/slips/test-corr-001", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var slip domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&slip))
	assert.Equal(t, "test-corr-001", slip.CorrelationID)
}

func TestBuildHandler_OpenAPISpec(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))

	info, ok := spec["info"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Slippy API", info["title"])
	assert.Equal(t, "Read-only API for CI/CD routing slips", info["description"])
}

// --- connectCache tests ---

func TestConnectCache_Disabled(t *testing.T) {
	// When DragonflyHost is empty, CacheEnabled() returns false.
	cfg := &config.Config{DragonflyHost: ""}
	reader := newStubSlipReader()

	// dial should never be called when cache is disabled
	dial := func(_ *redis.Options) redis.Cmdable {
		t.Fatal("dial should not be called when cache is disabled")
		return nil
	}

	result := connectCache(cfg, reader, dial)
	// Should return the original reader unchanged
	assert.Equal(t, reader, result)
}

func TestConnectCache_PingFailure(t *testing.T) {
	cfg := &config.Config{
		DragonflyHost:     "localhost",
		DragonflyPort:     16379,
		DragonflyPassword: "",
		CacheTTL:          5 * time.Minute,
	}
	reader := newStubSlipReader()

	// Create a real redis client pointing at a bad address — ping will fail.
	dial := func(opts *redis.Options) redis.Cmdable {
		opts.DialTimeout = 100 * time.Millisecond
		opts.ReadTimeout = 100 * time.Millisecond
		return redis.NewClient(opts)
	}

	result := connectCache(cfg, reader, dial)
	// On ping failure, the original reader is returned (caching disabled gracefully)
	assert.Equal(t, reader, result)
}

func TestConnectCache_PingSuccess(t *testing.T) {
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	if testing.Short() {
		t.Skip("skipping test that requires container runtime in short mode")
	}

	ctx := context.Background()

	// Start a real Redis container via testcontainers.
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err, "failed to start redis container")
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	opts, err := redis.ParseURL(connStr)
	require.NoError(t, err)

	// Extract host and port from the container's connection
	host, port := splitHostPort(opts.Addr)

	cfg := &config.Config{
		DragonflyHost: host,
		DragonflyPort: port,
		CacheTTL:      5 * time.Minute,
	}
	reader := newStubSlipReader()

	dial := func(o *redis.Options) redis.Cmdable {
		return redis.NewClient(o)
	}

	result := connectCache(cfg, reader, dial)
	// Should return a CachedSlipReader, not the original reader
	assert.NotEqual(t, reader, result)
	_, isCached := result.(*infrastructure.CachedSlipReader)
	assert.True(t, isCached, "expected CachedSlipReader when ping succeeds")
}

func TestConnectCache_PassesCorrectOptions(t *testing.T) {
	cfg := &config.Config{
		DragonflyHost:     "my-dragonfly.host",
		DragonflyPort:     6380,
		DragonflyPassword: "secret-pass",
		CacheTTL:          3 * time.Minute,
	}
	reader := newStubSlipReader()

	var capturedOpts *redis.Options
	// Capture the options passed to dial, but return a client that will fail ping
	dial := func(opts *redis.Options) redis.Cmdable {
		capturedOpts = opts
		opts.DialTimeout = 100 * time.Millisecond
		opts.ReadTimeout = 100 * time.Millisecond
		return redis.NewClient(opts)
	}

	_ = connectCache(cfg, reader, dial)

	// Verify the correct address and password were passed
	require.NotNil(t, capturedOpts)
	assert.Equal(t, "my-dragonfly.host:6380", capturedOpts.Addr)
	assert.Equal(t, "secret-pass", capturedOpts.Password)
}

// --- run() error path tests ---

// clearRunEnv unsets all environment variables that run() depends on.
func clearRunEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SLIPPY_API_KEY", "PORT",
		"DRAGONFLY_HOST", "DRAGONFLY_PORT", "DRAGONFLY_PASSWORD",
		"CACHE_TTL",
		"CLICKHOUSE_HOSTNAME", "CLICKHOUSE_PORT", "CLICKHOUSE_USERNAME",
		"CLICKHOUSE_PASSWORD", "CLICKHOUSE_DATABASE", "CLICKHOUSE_SKIP_VERIFY",
		"SLIPPY_PIPELINE_CONFIG",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

func TestRun_MissingAPIKey(t *testing.T) {
	clearRunEnv(t)

	err := run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config:")
}

func TestRun_MissingPipelineConfig(t *testing.T) {
	clearRunEnv(t)
	t.Setenv("SLIPPY_API_KEY", "test-key")

	// config.Load() succeeds, but LoadPipelineConfig() will fail
	// because SLIPPY_PIPELINE_CONFIG is not set
	err := run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pipeline config:")
}

func TestRun_MissingClickhouseConfig(t *testing.T) {
	clearRunEnv(t)
	t.Setenv("SLIPPY_API_KEY", "test-key")
	// Provide a valid inline pipeline config so we get past the pipeline step
	t.Setenv(
		"SLIPPY_PIPELINE_CONFIG",
		`{"version":"1.0","name":"test","steps":[{"name":"build","description":"build"}]}`,
	)

	// config.Load() and pipeline config succeed, but ClickhouseLoadConfig() will fail
	// because CLICKHOUSE_HOSTNAME is required
	err := run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clickhouse")
}

// splitHostPort splits an "host:port" string into its components.
func splitHostPort(addr string) (string, int) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			port, _ := strconv.Atoi(addr[i+1:])
			return addr[:i], port
		}
	}
	return addr, 0
}
