package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
	"github.com/MyCarrier-DevOps/slippy-api/internal/handler"
	"github.com/MyCarrier-DevOps/slippy-api/internal/infrastructure"
	"github.com/MyCarrier-DevOps/slippy-api/internal/middleware"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// inMemorySlipReader is a simple in-memory implementation of domain.SlipReader
// used as the backing store for end-to-end tests (since we don't spin up ClickHouse).
type inMemorySlipReader struct {
	slips map[string]*domain.Slip // keyed by correlation ID
}

func newInMemorySlipReader() *inMemorySlipReader {
	return &inMemorySlipReader{slips: make(map[string]*domain.Slip)}
}

func (r *inMemorySlipReader) addSlip(s *domain.Slip) {
	r.slips[s.CorrelationID] = s
}

func (r *inMemorySlipReader) Load(_ context.Context, correlationID string) (*domain.Slip, error) {
	s, ok := r.slips[correlationID]
	if !ok {
		return nil, slippy.ErrSlipNotFound
	}
	return s, nil
}

func (r *inMemorySlipReader) LoadByCommit(_ context.Context, repository, commitSHA string) (*domain.Slip, error) {
	for _, s := range r.slips {
		if s.Repository == repository && s.CommitSHA == commitSHA {
			return s, nil
		}
	}
	return nil, slippy.ErrSlipNotFound
}

func (r *inMemorySlipReader) FindByCommits(
	_ context.Context,
	repository string,
	commits []string,
) (*domain.Slip, string, error) {
	for _, c := range commits {
		for _, s := range r.slips {
			if s.Repository == repository && s.CommitSHA == c {
				return s, c, nil
			}
		}
	}
	return nil, "", slippy.ErrSlipNotFound
}

func (r *inMemorySlipReader) FindAllByCommits(
	_ context.Context,
	repository string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	var results []domain.SlipWithCommit
	for _, c := range commits {
		for _, s := range r.slips {
			if s.Repository == repository && s.CommitSHA == c {
				results = append(results, domain.SlipWithCommit{Slip: s, MatchedCommit: c})
			}
		}
	}
	return results, nil
}

// buildTestServer creates a fully-wired HTTP handler with auth, cache, and routes.
func buildTestServer(apiKey string, reader domain.SlipReader, rdb redis.Cmdable, cacheTTL time.Duration) http.Handler {
	mux := http.NewServeMux()
	apiConfig := huma.DefaultConfig("Slippy API E2E Test", "0.0.1")
	apiConfig.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"apiKey": {Type: "http", Scheme: "bearer"},
	}
	api := humago.New(mux, apiConfig)

	// Wire auth middleware
	api.UseMiddleware(middleware.NewAPIKeyAuth(apiKey))

	// Wire cache decorator around the reader
	cachedReader := infrastructure.NewCachedSlipReader(reader, rdb, cacheTTL)

	// Wire routes
	handler.RegisterHealthRoutes(api)
	h := handler.NewSlipHandler(cachedReader)
	handler.RegisterRoutes(api, h)

	return mux
}

// TestE2E_FullStack_WithRedisContainer spins up a real Redis container via testcontainers,
// wires the full API stack (auth + cache + handlers), and exercises the endpoints.
func TestE2E_FullStack_WithRedisContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	// Disable Ryuk reaper — required for Podman compatibility (Ryuk tries to
	// mount the container socket which Podman does not support).
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	ctx := context.Background()

	// --- Start Redis container ---
	redisContainer, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err, "failed to start redis container")
	t.Cleanup(func() {
		require.NoError(t, redisContainer.Terminate(ctx))
	})

	connStr, err := redisContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// Parse the connection string into redis.Options
	opts, err := redis.ParseURL(connStr)
	require.NoError(t, err)
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { rdb.Close() })

	// Verify Redis connectivity
	require.NoError(t, rdb.Ping(ctx).Err(), "redis ping failed")

	// --- Set up in-memory store with test data ---
	store := newInMemorySlipReader()
	store.addSlip(&domain.Slip{
		CorrelationID: "e2e-corr-001",
		Repository:    "org/my-service",
		Branch:        "main",
		CommitSHA:     "aaaa1111",
	})
	store.addSlip(&domain.Slip{
		CorrelationID: "e2e-corr-002",
		Repository:    "org/my-service",
		Branch:        "feature-x",
		CommitSHA:     "bbbb2222",
	})
	store.addSlip(&domain.Slip{
		CorrelationID: "e2e-corr-003",
		Repository:    "org/other-repo",
		Branch:        "main",
		CommitSHA:     "cccc3333",
	})

	apiKey := "e2e-test-secret"
	srv := buildTestServer(apiKey, store, rdb, 5*time.Minute)
	authHeader := "Bearer " + apiKey

	// --- Test: Health endpoint (no auth required) ---
	t.Run("health", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var body map[string]string
		require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
		assert.Equal(t, "ok", body["status"])
	})

	// --- Test: Auth rejection ---
	t.Run("auth_rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/slips/e2e-corr-001", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusForbidden, w.Code)
	})

	// --- Test: Auth missing ---
	t.Run("auth_missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/slips/e2e-corr-001", nil)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	// --- Test: GET /slips/{correlationID} ---
	t.Run("get_slip", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/slips/e2e-corr-001", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var slip domain.Slip
		require.NoError(t, json.NewDecoder(w.Body).Decode(&slip))
		assert.Equal(t, "e2e-corr-001", slip.CorrelationID)
		assert.Equal(t, "org/my-service", slip.Repository)
	})

	// --- Test: GET /slips/{correlationID} — not found ---
	t.Run("get_slip_not_found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/slips/does-not-exist", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	// --- Test: GET /slips/by-commit/{owner}/{repo}/{commitSHA} ---
	t.Run("get_slip_by_commit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/org/my-service/aaaa1111", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var slip domain.Slip
		require.NoError(t, json.NewDecoder(w.Body).Decode(&slip))
		assert.Equal(t, "e2e-corr-001", slip.CorrelationID)
	})

	// --- Test: GET /slips/by-commit — not found ---
	t.Run("get_slip_by_commit_not_found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/org/my-service/0000dead", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	// --- Test: POST /slips/find-by-commits ---
	t.Run("find_by_commits", func(t *testing.T) {
		body := `{"repository":"org/my-service","commits":["bbbb2222","aaaa1111"]}`
		req := httptest.NewRequest(http.MethodPost, "/slips/find-by-commits", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp struct {
			Slip          domain.Slip `json:"slip"`
			MatchedCommit string      `json:"matched_commit"`
		}
		require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
		// Should match the first commit in the list
		assert.Equal(t, "bbbb2222", resp.MatchedCommit)
		assert.Equal(t, "e2e-corr-002", resp.Slip.CorrelationID)
	})

	// --- Test: POST /slips/find-by-commits — not found ---
	t.Run("find_by_commits_not_found", func(t *testing.T) {
		body := `{"repository":"org/my-service","commits":["ffffffff"]}`
		req := httptest.NewRequest(http.MethodPost, "/slips/find-by-commits", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	// --- Test: POST /slips/find-all-by-commits ---
	t.Run("find_all_by_commits", func(t *testing.T) {
		body := `{"repository":"org/my-service","commits":["aaaa1111","bbbb2222"]}`
		req := httptest.NewRequest(http.MethodPost, "/slips/find-all-by-commits", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var items []handler.FindAllByCommitsItem
		require.NoError(t, json.NewDecoder(w.Body).Decode(&items))
		assert.Len(t, items, 2)
	})

	// --- Test: POST /slips/find-all-by-commits — no matches ---
	t.Run("find_all_by_commits_empty", func(t *testing.T) {
		body := `{"repository":"org/my-service","commits":["00000000"]}`
		req := httptest.NewRequest(http.MethodPost, "/slips/find-all-by-commits", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var items []handler.FindAllByCommitsItem
		require.NoError(t, json.NewDecoder(w.Body).Decode(&items))
		assert.Empty(t, items)
	})

	// --- Test: Verify Redis received data (cache decorator write-through) ---
	// Since the current cache decorator is pass-through, we just verify Redis is alive
	t.Run("redis_alive", func(t *testing.T) {
		err := rdb.Set(ctx, "test:e2e:ping", "pong", time.Minute).Err()
		require.NoError(t, err)
		val, err := rdb.Get(ctx, "test:e2e:ping").Result()
		require.NoError(t, err)
		assert.Equal(t, "pong", val)
	})

	// --- Test: OpenAPI spec is served ---
	t.Run("openapi_spec", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify it's valid JSON and contains our API info
		var spec map[string]any
		require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))
		info, ok := spec["info"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Slippy API E2E Test", info["title"])
	})

	// --- Test: Verify repeated requests work (no state corruption) ---
	t.Run("repeated_requests_stable", func(t *testing.T) {
		for i := range 3 {
			req := httptest.NewRequest(http.MethodGet, "/slips/e2e-corr-001", nil)
			req.Header.Set("Authorization", authHeader)
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code, "request %d failed", i)

			var slip domain.Slip
			require.NoError(t, json.NewDecoder(w.Body).Decode(&slip))
			assert.Equal(t, "e2e-corr-001", slip.CorrelationID)
		}
	})

	// --- Test: Cross-repo isolation ---
	t.Run("cross_repo_isolation", func(t *testing.T) {
		// Search for commits from org/my-service in org/other-repo — should not find them
		body := fmt.Sprintf(`{"repository":"org/other-repo","commits":["aaaa1111"]}`)
		req := httptest.NewRequest(http.MethodPost, "/slips/find-by-commits", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}
