package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- Mock RollbackStore ---

type mockRollbackStore struct {
	recordFn func(ctx context.Context, rec domain.RollbackRecord) (bool, error)
	listFn   func(ctx context.Context, correlationID string) ([]domain.RollbackRecord, error)
}

func (m *mockRollbackStore) RecordRollback(ctx context.Context, rec domain.RollbackRecord) (bool, error) {
	return m.recordFn(ctx, rec)
}

func (m *mockRollbackStore) ListRollbacks(ctx context.Context, correlationID string) ([]domain.RollbackRecord, error) {
	return m.listFn(ctx, correlationID)
}

// neverCalledRollbackStore fails the test if the handler reaches the store.
func neverCalledRollbackStore(t *testing.T) *mockRollbackStore {
	t.Helper()
	return &mockRollbackStore{
		recordFn: func(context.Context, domain.RollbackRecord) (bool, error) {
			t.Fatal("the store must not be called")
			return false, nil
		},
		listFn: func(context.Context, string) ([]domain.RollbackRecord, error) {
			t.Fatal("the store must not be called")
			return nil, nil
		},
	}
}

// setupRollbackTestAPI creates a huma API with the rollback routes (no auth middleware).
func setupRollbackTestAPI(store domain.RollbackStore) http.Handler {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Test Slippy API", "0.0.1"))
	RegisterRollbackRoutes(api, NewRollbackHandler(store))
	return mux
}

// rollbackBody returns a valid record-rollback body with overrides applied; a nil override
// deletes the key.
func rollbackBody(t *testing.T, overrides map[string]any) io.Reader {
	t.Helper()
	body := map[string]any{
		"workflow": "gitops-rollback-abc12", "environment": "prod", "reason": "canary_abort",
		"source": "canary-coordinator", "rollout_phase": "canary", "pod_hash": "5d8b9c7f6",
		"gitops_repo": "GitOps-prod", "gitops_commit": "a1b2c3d", "revert_commit": "e4f5a6b",
		"status": "success", "forced": false, "message": "reverted",
	}
	for k, v := range overrides {
		if v == nil {
			delete(body, k)
			continue
		}
		body[k] = v
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return bytes.NewReader(raw)
}

func postRollback(t *testing.T, h http.Handler, corr string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/slips/"+corr+"/rollbacks", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func decodeInserted(t *testing.T, w *httptest.ResponseRecorder) bool {
	t.Helper()
	var out struct {
		Inserted bool `json:"inserted"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&out))
	return out.Inserted
}

func TestRecordRollback_Inserted(t *testing.T) {
	var got domain.RollbackRecord
	h := setupRollbackTestAPI(&mockRollbackStore{
		recordFn: func(_ context.Context, rec domain.RollbackRecord) (bool, error) {
			got = rec
			return true, nil
		},
	})

	w := postRollback(t, h, "corr-001", rollbackBody(t, map[string]any{"forced": true}))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.True(t, decodeInserted(t, w))
	assert.Equal(t, domain.RollbackRecord{
		CorrelationID: "corr-001", Workflow: "gitops-rollback-abc12", Environment: "prod",
		Reason: "canary_abort", Source: "canary-coordinator", RolloutPhase: "canary", PodHash: "5d8b9c7f6",
		GitOpsRepo: "GitOps-prod", GitOpsCommit: "a1b2c3d", RevertCommit: "e4f5a6b",
		Status: slippy.RollbackStatusSuccess, Forced: true, Message: "reverted",
	}, got, "the correlation id comes from the path and every body field is forwarded")
}

func TestRecordRollback_RepeatWorkflowIsOKButNotInserted(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		recordFn: func(context.Context, domain.RollbackRecord) (bool, error) { return false, nil },
	})
	w := postRollback(t, h, "corr-001", rollbackBody(t, nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.False(t, decodeInserted(t, w))
}

func TestRecordRollback_UnknownSlipIs404(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		recordFn: func(context.Context, domain.RollbackRecord) (bool, error) {
			return false, fmt.Errorf("record rollback for slip x: %w", slippy.ErrSlipNotFound)
		},
	})
	w := postRollback(t, h, "corr-missing", rollbackBody(t, nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRecordRollback_InvalidRecordIs422(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		recordFn: func(context.Context, domain.RollbackRecord) (bool, error) {
			return false, fmt.Errorf("%w: status \"error\" is not one of success, noop, skipped, failed, dry_run",
				slippy.ErrInvalidRollback)
		},
	})
	w := postRollback(t, h, "corr-001", rollbackBody(t, map[string]any{"status": "error"}))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
	assert.Contains(t, w.Body.String(), "status")
}

func TestRecordRollback_MissingRequiredFieldIs422WithoutTouchingTheStore(t *testing.T) {
	h := setupRollbackTestAPI(neverCalledRollbackStore(t))
	w := postRollback(t, h, "corr-001", rollbackBody(t, map[string]any{"status": nil}))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestRecordRollback_EmptyWorkflowIs422(t *testing.T) {
	h := setupRollbackTestAPI(neverCalledRollbackStore(t))
	w := postRollback(t, h, "corr-001", rollbackBody(t, map[string]any{"workflow": ""}))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

// Review Focus 3: the limit is characters, so 1000 two-byte runes pass and 1001 do not.
func TestRecordRollback_MessageLimitCountsCharacters(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		recordFn: func(context.Context, domain.RollbackRecord) (bool, error) { return true, nil },
	})
	w := postRollback(t, h, "corr-001", rollbackBody(t, map[string]any{"message": strings.Repeat("é", 1000)}))
	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

	w = postRollback(t, h, "corr-001", rollbackBody(t, map[string]any{"message": strings.Repeat("é", 1001)}))
	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

// Review Focus 5: an oversized body is refused before it is parsed or stored.
func TestRecordRollback_OversizedBodyIs413(t *testing.T) {
	h := setupRollbackTestAPI(neverCalledRollbackStore(t))
	w := postRollback(t, h, "corr-001", rollbackBody(t, map[string]any{"message": strings.Repeat("x", 20*1024)}))
	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

func TestRecordRollback_MalformedCorrelationIDIs400(t *testing.T) {
	h := setupRollbackTestAPI(neverCalledRollbackStore(t))
	w := postRollback(t, h, "bad$id", rollbackBody(t, nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// Review Focus 1: the path id is forwarded exactly as sent; the store decides it does not match.
func TestRecordRollback_CorrelationIDCaseIsPreserved(t *testing.T) {
	var got string
	h := setupRollbackTestAPI(&mockRollbackStore{
		recordFn: func(_ context.Context, rec domain.RollbackRecord) (bool, error) {
			got = rec.CorrelationID
			return false, slippy.ErrSlipNotFound
		},
	})
	w := postRollback(t, h, "RB-CASE", rollbackBody(t, nil))
	assert.Equal(t, "RB-CASE", got)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRecordRollback_StoreFailureIs500(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		recordFn: func(context.Context, domain.RollbackRecord) (bool, error) {
			return false, errors.New("connection reset")
		},
	})
	w := postRollback(t, h, "corr-001", rollbackBody(t, nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.NotContains(t, w.Body.String(), "connection reset", "driver detail stays in the logs")
}

func getRollbacks(t *testing.T, h http.Handler, corr string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/slips/"+corr+"/rollbacks", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestListRollbacks_OK(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		listFn: func(_ context.Context, id string) ([]domain.RollbackRecord, error) {
			assert.Equal(t, "corr-001", id)
			return []domain.RollbackRecord{
				{ID: 1, CorrelationID: "corr-001", Workflow: "wf-1", Environment: "prod", Status: slippy.RollbackStatusSuccess},
				{ID: 2, CorrelationID: "corr-001", Workflow: "wf-2", Environment: "prod", Status: slippy.RollbackStatusNoop},
			}, nil
		},
	})
	w := getRollbacks(t, h, "corr-001")
	require.Equal(t, http.StatusOK, w.Code)

	var out struct {
		Rollbacks []domain.RollbackRecord `json:"rollbacks"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&out))
	require.Len(t, out.Rollbacks, 2)
	assert.Equal(t, "wf-2", out.Rollbacks[1].Workflow)
	assert.Equal(t, slippy.RollbackStatusNoop, out.Rollbacks[1].Status)
}

func TestListRollbacks_NoneIsAnEmptyArray(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		listFn: func(context.Context, string) ([]domain.RollbackRecord, error) { return nil, nil },
	})
	w := getRollbacks(t, h, "corr-001")
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"rollbacks":[]}`, strings.TrimSpace(stripSchemaField(t, w.Body.Bytes())))
}

func TestListRollbacks_MalformedCorrelationIDIs400(t *testing.T) {
	h := setupRollbackTestAPI(neverCalledRollbackStore(t))
	w := getRollbacks(t, h, "bad$id")
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListRollbacks_StoreFailureIs500(t *testing.T) {
	h := setupRollbackTestAPI(&mockRollbackStore{
		listFn: func(context.Context, string) ([]domain.RollbackRecord, error) {
			return nil, errors.New("timeout")
		},
	})
	w := getRollbacks(t, h, "corr-001")
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// stripSchemaField removes the "$schema" link huma adds to JSON responses, so a test can
// compare the payload alone.
func stripSchemaField(t *testing.T, raw []byte) string {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	delete(m, "$schema")
	out, err := json.Marshal(m)
	require.NoError(t, err)
	return string(out)
}
