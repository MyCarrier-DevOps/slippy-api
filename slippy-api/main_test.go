package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
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

// --- Stub readers for optional handlers (used in spec generation) ---

type stubImageTagReader struct{}

func (s *stubImageTagReader) ResolveImageTags(_ context.Context, _ string) (*domain.ImageTagResult, error) {
	return &domain.ImageTagResult{}, nil
}

type stubSlipWriter struct{}

func (s *stubSlipWriter) CreateSlipForPush(
	_ context.Context,
	opts domain.PushOptions,
) (*domain.CreateSlipResult, error) {
	return &domain.CreateSlipResult{Slip: &domain.Slip{CorrelationID: opts.CorrelationID}}, nil
}
func (s *stubSlipWriter) StartStep(_ context.Context, _, _, _ string) error    { return nil }
func (s *stubSlipWriter) CompleteStep(_ context.Context, _, _, _ string) error { return nil }
func (s *stubSlipWriter) FailStep(_ context.Context, _, _, _, _ string) error  { return nil }
func (s *stubSlipWriter) SkipStep(_ context.Context, _, _, _, _ string) error  { return nil }
func (s *stubSlipWriter) SetComponentImageTag(_ context.Context, _, _, _ string) error {
	return nil
}

type stubCIJobLogReader struct{}

func (s *stubCIJobLogReader) QueryLogs(_ context.Context, _ *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
	return &domain.CIJobLogResult{}, nil
}

type stubAutomationTestResultsReader struct{}

func (s *stubAutomationTestResultsReader) QueryAutomationTestResults(
	_ context.Context,
	_ *domain.AutomationTestResultsQuery,
) (*domain.AutomationTestResultsResult, error) {
	return &domain.AutomationTestResultsResult{}, nil
}

type stubAutomationTestsReader struct{}

func (s *stubAutomationTestsReader) QueryTestsByCorrelation(
	_ context.Context,
	_ *domain.AutomationTestsByCorrelationQuery,
) (*domain.AutomationTestsResult, error) {
	return &domain.AutomationTestsResult{}, nil
}

func (s *stubAutomationTestsReader) LoadTestByCorrelation(
	_ context.Context,
	_ *domain.LoadTestByCorrelationQuery,
) (*domain.AutomationTestResult, error) {
	return nil, domain.ErrTestNotFound
}

// --- buildHandler tests ---

func TestBuildHandler_HealthEndpoint(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)
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

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)

	// Request without auth header should be rejected
	req := httptest.NewRequest(http.MethodGet, "/slips/test-corr-001", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestBuildHandler_AuthSuccess(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)

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

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))

	info, ok := spec["info"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Slippy API", info["title"])
	assert.Equal(t, "API for CI/CD routing slips", info["description"])
}

// --- v1 versioned endpoint tests ---

func TestBuildHandler_V1HealthEndpoint(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestBuildHandler_V1AuthRequired(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/slips/test-corr-001", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestBuildHandler_V1AuthSuccess(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/slips/test-corr-001", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var slip domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&slip))
	assert.Equal(t, "test-corr-001", slip.CorrelationID)
}

func TestBuildHandler_OpenAPISpecContainsV1Routes(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))

	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)

	// Verify both unversioned (legacy) and v1 paths exist
	assert.Contains(t, paths, "/health")
	assert.Contains(t, paths, "/v1/health")
	assert.Contains(t, paths, "/slips/{correlationID}")
	assert.Contains(t, paths, "/v1/slips/{correlationID}")
}

// --- Optional handler registration tests ---

// TestBuildHandler_WithAllOptionalHandlers exercises the conditional branches
// in buildHandler that register image-tag, ci-job-log, and write routes when
// their respective readers/writer are non-nil.
func TestBuildHandler_WithAllOptionalHandlers(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", WriteAPIKey: "write-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(
		cfg,
		reader,
		&stubSlipWriter{},
		&stubImageTagReader{},
		&stubCIJobLogReader{},
		&stubAutomationTestResultsReader{},
		&stubAutomationTestsReader{},
		nil,
	)
	require.NotNil(t, h)

	// The OpenAPI spec should now contain paths registered via each optional handler.
	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))
	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)

	// Image tag routes registered (expect a /v1/image-tags or similar path).
	// Write routes registered on /v1 only.
	var hasWriteRoute, hasImageTagRoute, hasCIJobLogRoute bool
	for path := range paths {
		if strings.Contains(path, "image-tag") || strings.Contains(path, "imagetag") {
			hasImageTagRoute = true
		}
		if strings.Contains(path, "ci-job-log") || strings.Contains(path, "cijoblog") ||
			strings.Contains(path, "logs") {
			hasCIJobLogRoute = true
		}
		if strings.HasPrefix(path, "/v1/") && (strings.Contains(path, "slip") || strings.Contains(path, "step")) {
			// Write routes register under /v1 for slip/step mutation endpoints.
			// Any /v1/slips* POST/PUT/etc. indicates the writer path was exercised.
			if ops, ok := paths[path].(map[string]any); ok {
				for method := range ops {
					if method == "post" || method == "put" || method == "patch" {
						hasWriteRoute = true
					}
				}
			}
		}
	}
	assert.True(t, hasImageTagRoute, "expected image tag route to be registered")
	assert.True(t, hasCIJobLogRoute, "expected ci job log route to be registered")
	assert.True(t, hasWriteRoute, "expected write route to be registered")
}

// --- Spec generation (gated behind GENERATE_SPEC=1) ---

func TestGenerateOpenAPISpec(t *testing.T) {
	if os.Getenv("GENERATE_SPEC") == "" {
		t.Skip("set GENERATE_SPEC=1 to regenerate OpenAPI spec files")
	}

	cfg := &config.Config{APIKey: "dummy", Port: 8080}
	reader := newStubSlipReader()
	h := buildHandler(
		cfg,
		reader,
		&stubSlipWriter{},
		&stubImageTagReader{},
		&stubCIJobLogReader{},
		&stubAutomationTestResultsReader{},
		&stubAutomationTestsReader{},
		nil,
	)

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Pretty-print the JSON
	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))
	formatted, err := json.MarshalIndent(spec, "", "  ")
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll("api/v1", 0o755))
	require.NoError(t, os.WriteFile("api/v1/openapi.json", formatted, 0o644))
	t.Logf("wrote api/v1/openapi.json (%d bytes)", len(formatted))

	// Also produce a v1-only, OpenAPI 3.0.3 compatible spec for client generation.
	v1Spec := buildV1OnlySpec(t, spec)
	v1Formatted, err := json.MarshalIndent(v1Spec, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile("api/v1/openapi-v1.json", v1Formatted, 0o644))
	t.Logf("wrote api/v1/openapi-v1.json (%d bytes)", len(v1Formatted))
}

// buildV1OnlySpec filters the full spec to v1 paths only, strips the /v1 prefix,
// cleans up v1- operation ID prefixes, and downconverts OpenAPI 3.1 nullable
// syntax to 3.0.3. The "v1" tag is preserved for Swagger UI grouping.
func buildV1OnlySpec(t *testing.T, full map[string]any) map[string]any {
	t.Helper()

	// Deep-copy via JSON round-trip.
	raw, err := json.Marshal(full)
	require.NoError(t, err)
	var spec map[string]any
	require.NoError(t, json.Unmarshal(raw, &spec))

	// Downgrade version.
	spec["openapi"] = "3.0.3"

	// Filter paths: keep only /v1/ prefixed, strip prefix, clean operation IDs.
	oldPaths, _ := spec["paths"].(map[string]any)
	newPaths := make(map[string]any)
	for path, methods := range oldPaths {
		if !strings.HasPrefix(path, "/v1") {
			continue
		}
		stripped := strings.TrimPrefix(path, "/v1")
		if stripped == "" {
			stripped = "/"
		}

		// Clean operation IDs on each method.
		methodMap, ok := methods.(map[string]any)
		if !ok {
			continue
		}
		for _, opAny := range methodMap {
			op, ok := opAny.(map[string]any)
			if !ok {
				continue
			}
			if id, ok := op["operationId"].(string); ok {
				op["operationId"] = strings.TrimPrefix(id, "v1-")
			}
		}
		newPaths[stripped] = methods
	}
	spec["paths"] = newPaths

	// Downconvert 3.1 nullable types to 3.0 format throughout the spec.
	downconvertNullable(spec)

	return spec
}

// downconvertNullable recursively converts {"type": ["array", "null"]} to
// {"type": "array", "nullable": true} for OpenAPI 3.0 compatibility.
func downconvertNullable(v any) {
	switch val := v.(type) {
	case map[string]any:
		if typeVal, ok := val["type"]; ok {
			if arr, ok := typeVal.([]any); ok {
				var nonNull string
				hasNull := false
				for _, item := range arr {
					s, _ := item.(string)
					if s == "null" {
						hasNull = true
					} else {
						nonNull = s
					}
				}
				if hasNull && nonNull != "" {
					val["type"] = nonNull
					val["nullable"] = true
				}
			}
		}
		for _, child := range val {
			downconvertNullable(child)
		}
	case []any:
		for _, item := range val {
			downconvertNullable(item)
		}
	}
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
		"SLIPPY_GITHUB_APP_ID", "SLIPPY_GITHUB_APP_PRIVATE_KEY",
		"SLIPPY_GITHUB_ENTERPRISE_URL", "SLIPPY_ANCESTRY_DEPTH",
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
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	// config.Load() succeeds, but LoadPipelineConfig() will fail
	// because SLIPPY_PIPELINE_CONFIG is not set
	err := run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pipeline config:")
}

func TestRun_MissingClickhouseConfig(t *testing.T) {
	clearRunEnv(t)
	t.Setenv("SLIPPY_API_KEY", "test-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
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
