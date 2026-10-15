package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/config"
)

const rollbacksE2EPipelineJSON = `{
	"version": "1.0",
	"name": "rollbacks-e2e",
	"steps": [
		{"name": "push_parsed", "description": "push received"},
		{"name": "dev_deploy", "description": "deploy to dev", "prerequisites": ["push_parsed"]}
	]
}`

// newRollbacksE2EStore starts a throwaway Postgres, migrates it to the latest schema through
// the library, and returns the same *slippy.PostgresStore that run() hands to buildHandler.
func newRollbacksE2EStore(t *testing.T) *slippy.PostgresStore {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: requires Docker")
	}
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("ci_test"),
		tcpostgres.WithUsername("slippy_write"),
		tcpostgres.WithPassword("secret"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx,
		fmt.Sprintf("postgres://slippy_write:secret@%s:%s/ci_test?sslmode=disable", host, port.Port()))
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var pingErr error
	for range 10 {
		if pingErr = pool.Ping(ctx); pingErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, pingErr)

	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(rollbacksE2EPipelineJSON))
	require.NoError(t, err)
	_, err = slippy.RunPostgresMigrations(ctx, pool, slippy.PostgresMigrateOptions{PipelineConfig: pipelineCfg})
	require.NoError(t, err)

	store, err := slippy.NewPostgresStore(pool, pipelineCfg, nil)
	require.NoError(t, err)
	return store
}

// TestE2E_RollbackRoutes_Postgres drives the rollback routes through buildHandler (so the tier
// table, gates and auth middleware are the production ones) against a real migrated Postgres.
func TestE2E_RollbackRoutes_Postgres(t *testing.T) {
	store := newRollbacksE2EStore(t)
	ctx := context.Background()

	const corr = "e2e-rollback-corr"
	require.NoError(t, store.Create(ctx, &slippy.Slip{
		CorrelationID: corr,
		Repository:    "Owner/Repo",
		Branch:        "main",
		CommitSHA:     "e2e-sha",
		Status:        slippy.SlipStatusFailed,
	}))

	const readKey, writeKey = "e2e-read-key", "e2e-write-key"
	cfg := &config.Config{APIKey: readKey, WriteAPIKey: writeKey, Port: 8080}
	h, err := buildHandler(handlerDeps{
		cfg:           cfg,
		reader:        newStubSlipReader(),
		writer:        &stubSlipWriter{},
		rollbackStore: store,
	})
	require.NoError(t, err)

	do := func(method, path, key, payload string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}

	want := map[string]any{
		"workflow":      "gitops-rollback-abc12",
		"environment":   "prod",
		"reason":        "canary_abort",
		"source":        "canary-coordinator",
		"rollout_phase": "canary",
		"pod_hash":      "5d8b9c7f6",
		"gitops_repo":   "GitOps-prod",
		"gitops_commit": "a1b2c3d",
		"revert_commit": "e4f5a6b",
		"status":        "success",
		"forced":        true,
		"message":       "canary analysis failed",
		"outcome":       "reverted",
	}
	payload, err := json.Marshal(want)
	require.NoError(t, err)
	path := "/v1/slips/" + corr + "/rollbacks"

	inserted := func(w *httptest.ResponseRecorder) bool {
		var out struct {
			Inserted bool `json:"inserted"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out), w.Body.String())
		return out.Inserted
	}

	// (a) first record is written.
	w := do(http.MethodPost, path, writeKey, string(payload))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.True(t, inserted(w))

	// (b) the same workflow is not recorded twice.
	w = do(http.MethodPost, path, writeKey, string(payload))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.False(t, inserted(w))

	// (c) exactly one record, every field round-tripped.
	w = do(http.MethodGet, path, readKey, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var listed struct {
		Rollbacks []map[string]any `json:"rollbacks"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &listed))
	require.Len(t, listed.Rollbacks, 1)
	got := listed.Rollbacks[0]
	for k, v := range want {
		assert.Equal(t, v, got[k], "field %s", k)
	}
	assert.Equal(t, corr, got["correlation_id"])
	assert.NotZero(t, got["id"])
	assert.NotEmpty(t, got["recorded_at"])

	// (d) a slip that does not exist lists as empty, not as an error.
	w = do(http.MethodGet, "/v1/slips/no-such-slip/rollbacks", readKey, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var empty struct {
		Rollbacks []map[string]any `json:"rollbacks"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &empty))
	assert.NotNil(t, empty.Rollbacks, "an unknown slip lists as [], not null")
	assert.Empty(t, empty.Rollbacks)

	// (e) the read key must not record, and no credential is refused outright.
	w = do(http.MethodPost, path, readKey, string(payload))
	assert.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	w = do(http.MethodPost, path, "", string(payload))
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
