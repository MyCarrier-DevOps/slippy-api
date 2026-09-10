package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// --- mockWriter implements domain.SlipWriter ---

type mockWriter struct {
	createSlipForPushFn    func(ctx context.Context, opts domain.PushOptions) (*domain.CreateSlipResult, error)
	startStepFn            func(ctx context.Context, correlationID, stepName, componentName string) error
	completeStepFn         func(ctx context.Context, correlationID, stepName, componentName string) error
	failStepFn             func(ctx context.Context, correlationID, stepName, componentName, reason string) error
	skipStepFn             func(ctx context.Context, correlationID, stepName, componentName, reason string) error
	setComponentImageTagFn func(ctx context.Context, correlationID, componentName, imageTag string) error
	promoteSlipFn          func(ctx context.Context, correlationID, promotedTo string) error
	abandonSlipFn          func(ctx context.Context, correlationID, supersededBy string) error
	claimSlipFn            func(ctx context.Context, correlationID, claimedBy, reason string) error
}

func (m *mockWriter) CreateSlipForPush(ctx context.Context, opts domain.PushOptions) (*domain.CreateSlipResult, error) {
	return m.createSlipForPushFn(ctx, opts)
}
func (m *mockWriter) StartStep(ctx context.Context, cID, step, comp string) error {
	return m.startStepFn(ctx, cID, step, comp)
}
func (m *mockWriter) CompleteStep(ctx context.Context, cID, step, comp string) error {
	return m.completeStepFn(ctx, cID, step, comp)
}
func (m *mockWriter) FailStep(ctx context.Context, cID, step, comp, reason string) error {
	return m.failStepFn(ctx, cID, step, comp, reason)
}
func (m *mockWriter) SkipStep(ctx context.Context, cID, step, comp, reason string) error {
	return m.skipStepFn(ctx, cID, step, comp, reason)
}
func (m *mockWriter) SetComponentImageTag(ctx context.Context, cID, comp, tag string) error {
	return m.setComponentImageTagFn(ctx, cID, comp, tag)
}
func (m *mockWriter) PromoteSlip(ctx context.Context, cID, promotedTo string) error {
	if m.promoteSlipFn != nil {
		return m.promoteSlipFn(ctx, cID, promotedTo)
	}
	return nil
}
func (m *mockWriter) AbandonSlip(ctx context.Context, cID, supersededBy string) error {
	if m.abandonSlipFn != nil {
		return m.abandonSlipFn(ctx, cID, supersededBy)
	}
	return nil
}
func (m *mockWriter) ClaimSlip(ctx context.Context, cID, claimedBy, reason string) error {
	if m.claimSlipFn != nil {
		return m.claimSlipFn(ctx, cID, claimedBy, reason)
	}
	return nil
}

// setupWriteTestAPI creates a huma API with write routes and no auth for testing.
func setupWriteTestAPI(w domain.SlipWriter) http.Handler {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test", "1.0.0")
	api := humago.New(mux, cfg)

	h := NewSlipWriteHandler(w, nil)
	RegisterWriteRoutes(api, h)
	return mux
}

// --- CreateSlip tests ---

func TestCreateSlip_Success(t *testing.T) {
	w := &mockWriter{
		createSlipForPushFn: func(_ context.Context, opts domain.PushOptions) (*domain.CreateSlipResult, error) {
			return &domain.CreateSlipResult{
				Slip:             &domain.Slip{CorrelationID: opts.CorrelationID, Repository: opts.Repository},
				AncestryResolved: true,
			}, nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"correlation_id":"abc-123","repository":"org/repo","branch":"main","commit_sha":"deadbeef"}`
	req := httptest.NewRequest(http.MethodPost, "/slips", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	slip := resp["slip"].(map[string]any)
	assert.Equal(t, "abc-123", slip["correlation_id"])
	assert.Equal(t, true, resp["ancestry_resolved"])
}

func TestCreateSlip_WithComponents(t *testing.T) {
	var gotOpts domain.PushOptions
	w := &mockWriter{
		createSlipForPushFn: func(_ context.Context, opts domain.PushOptions) (*domain.CreateSlipResult, error) {
			gotOpts = opts
			return &domain.CreateSlipResult{
				Slip: &domain.Slip{CorrelationID: opts.CorrelationID},
			}, nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{
		"correlation_id":"abc-123","repository":"org/repo","branch":"main","commit_sha":"dead",
		"components":[{"name":"api","dockerfile_path":"src/Api/Dockerfile"}]
	}`
	req := httptest.NewRequest(http.MethodPost, "/slips", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
	require.Len(t, gotOpts.Components, 1)
	assert.Equal(t, "api", gotOpts.Components[0].Name)
	assert.Equal(t, "src/Api/Dockerfile", gotOpts.Components[0].DockerfilePath)
}

// TestCreateSlip_DispatchIntent covers the DEVOPS-341 boundary: a recognized value is
// forwarded verbatim, omission means unspecified, and anything else is refused with a 422
// BEFORE the writer runs.
//
// The rejection half is the point of validating here at all. The library degrades an
// unrecognized DispatchIntent to its legacy component-count inference, and that degradation is
// not safe — a mis-cased "Nothing" arriving with components present repaves and destroys the
// prior run's history. So the assertion that matters is not just the status code: it is that
// the writer was never called.
func TestCreateSlip_DispatchIntent(t *testing.T) {
	t.Run("forwarded and refused", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			field    string
			wantCode int
			want     domain.DispatchIntent
		}{
			{"something is forwarded", `,"dispatch":"something"`, http.StatusCreated, slippy.DispatchIntentSomething},
			{"nothing is forwarded", `,"dispatch":"nothing"`, http.StatusCreated, slippy.DispatchIntentNothing},
			{"omitted means unspecified", ``, http.StatusCreated, slippy.DispatchIntentUnspecified},
			// Case variants and typos are reachable inputs, not hypotheticals: the value
			// crosses two repo boundaries as a bare string.
			{"mis-cased is refused", `,"dispatch":"Nothing"`, http.StatusUnprocessableEntity, ""},
			{"unknown is refused", `,"dispatch":"maybe"`, http.StatusUnprocessableEntity, ""},
			{"padded is refused", `,"dispatch":"nothing "`, http.StatusUnprocessableEntity, ""},
			// Explicit empty is refused too: omit the field to mean unspecified.
			{"explicit empty is refused", `,"dispatch":""`, http.StatusUnprocessableEntity, ""},
			// ...but an explicit null is NOT refused: huma skips validating a null on a
			// non-required property, so null is a second spelling of "unspecified".
			{"explicit null means unspecified", `,"dispatch":null`, http.StatusCreated, slippy.DispatchIntentUnspecified},
		} {
			t.Run(tc.name, func(t *testing.T) {
				called := false
				var gotOpts domain.PushOptions
				w := &mockWriter{
					createSlipForPushFn: func(_ context.Context, opts domain.PushOptions) (*domain.CreateSlipResult, error) {
						called = true
						gotOpts = opts
						return &domain.CreateSlipResult{Slip: &domain.Slip{CorrelationID: opts.CorrelationID}}, nil
					},
				}
				handler := setupWriteTestAPI(w)

				body := `{"correlation_id":"abc-123","repository":"org/repo","branch":"main","commit_sha":"dead"` +
					tc.field + `}`
				req := httptest.NewRequest(http.MethodPost, "/slips", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				require.Equal(t, tc.wantCode, rec.Code, "body: %s", rec.Body.String())
				if tc.wantCode != http.StatusCreated {
					assert.False(t, called,
						"an out-of-range dispatch must be refused before the writer runs — the "+
							"library's fallback for it is not safe")
					return
				}
				require.True(t, called)
				assert.Equal(t, tc.want, gotOpts.Dispatch)
			})
		}
	})

	// The huma enum literal is a hand copy of the library's constant VALUES, so pin those. This
	// does not (and cannot) pin the SET: if the library ever adds a value, the enum above must
	// be widened by hand — the slippy bump checklist in CLAUDE.md carries that step, because a
	// missed widening fails closed one repo away (422 -> DLQ) while this suite stays green.
	t.Run("the enum's values are the library's two explicit constants", func(t *testing.T) {
		assert.Equal(t, domain.DispatchIntent("something"), slippy.DispatchIntentSomething)
		assert.Equal(t, domain.DispatchIntent("nothing"), slippy.DispatchIntentNothing)
		// "" is recognized by the library but deliberately NOT in the huma enum: omission,
		// not an explicit empty string, is how a caller says "unspecified".
		assert.Equal(t, domain.DispatchIntent(""), slippy.DispatchIntentUnspecified)
	})
}

func TestCreateSlip_WithWarnings(t *testing.T) {
	w := &mockWriter{
		createSlipForPushFn: func(_ context.Context, opts domain.PushOptions) (*domain.CreateSlipResult, error) {
			return &domain.CreateSlipResult{
				Slip:     &domain.Slip{CorrelationID: opts.CorrelationID},
				Warnings: []error{errors.New("ancestry resolution failed")},
			}, nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"correlation_id":"abc","repository":"org/repo","branch":"main","commit_sha":"dead"}`
	req := httptest.NewRequest(http.MethodPost, "/slips", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	warnings := resp["warnings"].([]any)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0].(string), "ancestry resolution failed")
}

func TestCreateSlip_ValidationError(t *testing.T) {
	w := &mockWriter{
		createSlipForPushFn: func(_ context.Context, _ domain.PushOptions) (*domain.CreateSlipResult, error) {
			return nil, errors.New("invalid push options: correlation_id is required")
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"correlation_id":"","repository":"org/repo","branch":"main","commit_sha":"dead"}`
	req := httptest.NewRequest(http.MethodPost, "/slips", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateSlip_InternalError(t *testing.T) {
	w := &mockWriter{
		createSlipForPushFn: func(_ context.Context, _ domain.PushOptions) (*domain.CreateSlipResult, error) {
			return nil, errors.New("clickhouse: connection refused")
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"correlation_id":"abc","repository":"org/repo","branch":"main","commit_sha":"dead"}`
	req := httptest.NewRequest(http.MethodPost, "/slips", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- StartStep tests ---

func TestStartStep_Success(t *testing.T) {
	w := &mockWriter{
		startStepFn: func(_ context.Context, cID, step, comp string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "builds_completed", step)
			assert.Equal(t, "api", comp)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"component_name":"api"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/builds_completed/start", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestStartStep_NoBody(t *testing.T) {
	w := &mockWriter{
		startStepFn: func(_ context.Context, cID, step, comp string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "push_parsed", step)
			assert.Equal(t, "", comp)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/push_parsed/start", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestStartStep_NotFound(t *testing.T) {
	w := &mockWriter{
		startStepFn: func(_ context.Context, _, _, _ string) error {
			return slippy.ErrSlipNotFound
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/builds_completed/start", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestStartStep_StepError(t *testing.T) {
	w := &mockWriter{
		startStepFn: func(_ context.Context, _, _, _ string) error {
			return slippy.NewStepError("update", "abc-123", "build", "api", errors.New("failed"))
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/build/start", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}

// --- CompleteStep tests ---

func TestCompleteStep_Success(t *testing.T) {
	w := &mockWriter{
		completeStepFn: func(_ context.Context, _, _, _ string) error { return nil },
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(
		http.MethodPost,
		"/slips/abc-123/steps/builds_completed/complete",
		strings.NewReader(`{}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestCompleteStep_Error(t *testing.T) {
	w := &mockWriter{
		completeStepFn: func(_ context.Context, _, _, _ string) error {
			return errors.New("database error")
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(
		http.MethodPost,
		"/slips/abc-123/steps/builds_completed/complete",
		strings.NewReader(`{}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- FailStep tests ---

func TestFailStep_Success(t *testing.T) {
	w := &mockWriter{
		failStepFn: func(_ context.Context, cID, step, comp, reason string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "build timeout", reason)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"component_name":"api","reason":"build timeout"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/builds_completed/fail", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestFailStep_NotFound(t *testing.T) {
	w := &mockWriter{
		failStepFn: func(_ context.Context, _, _, _, _ string) error {
			return slippy.ErrSlipNotFound
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"reason":"timeout"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/builds_completed/fail", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- SkipStep tests ---

func TestSkipStep_Success(t *testing.T) {
	w := &mockWriter{
		skipStepFn: func(_ context.Context, cID, step, comp, reason string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "prod_rollback_status", step)
			assert.Equal(t, "", comp)
			assert.Equal(t, "alert-gate passed", reason)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"reason":"alert-gate passed"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/slips/abc-123/steps/prod_rollback_status/skip",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestSkipStep_WithComponent(t *testing.T) {
	w := &mockWriter{
		skipStepFn: func(_ context.Context, cID, step, comp, reason string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "builds_completed", step)
			assert.Equal(t, "api", comp)
			assert.Equal(t, "not needed", reason)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"component_name":"api","reason":"not needed"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/builds_completed/skip", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestSkipStep_NoBody(t *testing.T) {
	w := &mockWriter{
		skipStepFn: func(_ context.Context, cID, step, comp, reason string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "prod_rollback_status", step)
			assert.Equal(t, "", comp)
			assert.Equal(t, "", reason)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(
		http.MethodPost,
		"/slips/abc-123/steps/prod_rollback_status/skip",
		strings.NewReader(`{}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestSkipStep_NilBody(t *testing.T) {
	w := &mockWriter{
		skipStepFn: func(_ context.Context, cID, step, comp, reason string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "prod_rollback_status", step)
			assert.Equal(t, "", comp)
			assert.Equal(t, "", reason)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/prod_rollback_status/skip", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestSkipStep_NotFound(t *testing.T) {
	w := &mockWriter{
		skipStepFn: func(_ context.Context, _, _, _, _ string) error {
			return slippy.ErrSlipNotFound
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"reason":"skip it"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/builds_completed/skip", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSkipStep_InternalError(t *testing.T) {
	w := &mockWriter{
		skipStepFn: func(_ context.Context, _, _, _, _ string) error {
			return errors.New("database error")
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"reason":"skip it"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/builds_completed/skip", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- SetImageTag tests ---

func TestSetImageTag_Success(t *testing.T) {
	w := &mockWriter{
		setComponentImageTagFn: func(_ context.Context, cID, comp, tag string) error {
			assert.Equal(t, "abc-123", cID)
			assert.Equal(t, "api", comp)
			assert.Equal(t, "26.09.abc1234", tag)
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"image_tag":"26.09.abc1234"}`
	req := httptest.NewRequest(http.MethodPut, "/slips/abc-123/components/api/image-tag", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestSetImageTag_NotFound(t *testing.T) {
	w := &mockWriter{
		setComponentImageTagFn: func(_ context.Context, _, _, _ string) error {
			return slippy.ErrSlipNotFound
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"image_tag":"26.09.abc1234"}`
	req := httptest.NewRequest(http.MethodPut, "/slips/abc-123/components/api/image-tag", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSetImageTag_InternalError(t *testing.T) {
	w := &mockWriter{
		setComponentImageTagFn: func(_ context.Context, _, _, _ string) error {
			return errors.New("database error")
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"image_tag":"26.09.abc1234"}`
	req := httptest.NewRequest(http.MethodPut, "/slips/abc-123/components/api/image-tag", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- PromoteSlip tests ---

func TestPromoteSlip_Success(t *testing.T) {
	var gotCID, gotPromotedTo string
	w := &mockWriter{
		promoteSlipFn: func(_ context.Context, cID, promotedTo string) error {
			gotCID, gotPromotedTo = cID, promotedTo
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"promoted_to":"corr-main-merge"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/promote", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "abc-123", gotCID)
	assert.Equal(t, "corr-main-merge", gotPromotedTo)
}

func TestPromoteSlip_NotFound(t *testing.T) {
	w := &mockWriter{
		promoteSlipFn: func(_ context.Context, _, _ string) error {
			return slippy.ErrSlipNotFound
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"promoted_to":"corr-main"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/promote", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPromoteSlip_InternalError(t *testing.T) {
	w := &mockWriter{
		promoteSlipFn: func(_ context.Context, _, _ string) error {
			return errors.New("database error")
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"promoted_to":"corr-main"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/promote", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- AbandonSlip tests ---

func TestAbandonSlip_Success(t *testing.T) {
	var gotCID, gotSupersededBy string
	w := &mockWriter{
		abandonSlipFn: func(_ context.Context, cID, supersededBy string) error {
			gotCID, gotSupersededBy = cID, supersededBy
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"superseded_by":"corr-new-push"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/abandon", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "abc-123", gotCID)
	assert.Equal(t, "corr-new-push", gotSupersededBy)
}

func TestAbandonSlip_NotFound(t *testing.T) {
	w := &mockWriter{
		abandonSlipFn: func(_ context.Context, _, _ string) error {
			return slippy.ErrSlipNotFound
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"superseded_by":"corr-new"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/abandon", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAbandonSlip_InternalError(t *testing.T) {
	w := &mockWriter{
		abandonSlipFn: func(_ context.Context, _, _ string) error {
			return errors.New("database error")
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"superseded_by":"corr-new"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/abandon", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- ClaimSlip tests (DEVOPS-285) ---

func TestClaimSlip_Success(t *testing.T) {
	var gotCID, gotClaimedBy, gotReason string
	w := &mockWriter{
		claimSlipFn: func(_ context.Context, cID, claimedBy, reason string) error {
			gotCID, gotClaimedBy, gotReason = cID, claimedBy, reason
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	body := `{"claimed_by":"rerunner","reason":"retrigger builds and unit tests"}`
	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/claim", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "abc-123", gotCID)
	assert.Equal(t, "rerunner", gotClaimedBy)
	assert.Equal(t, "retrigger builds and unit tests", gotReason)
}

func TestClaimSlip_ReasonIsOptional(t *testing.T) {
	called := false
	var gotReason string
	w := &mockWriter{
		claimSlipFn: func(_ context.Context, _, _, reason string) error {
			called, gotReason = true, reason
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/claim",
		strings.NewReader(`{"claimed_by":"rerunner"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.True(t, called, "claim must reach the writer without a reason")
	assert.Empty(t, gotReason)
}

// A claim that fails must not look like a success to the adopter: the whole point
// of the endpoint is that a caller which cannot claim its slip dispatches nothing.
func TestClaimSlip_WriterFailuresAreNotSwallowed(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"repaved or never created", slippy.ErrSlipNotFound, http.StatusNotFound},
		{"lock contention", domain.ErrWriteContended, http.StatusServiceUnavailable},
		{"anything else", errors.New("connection reset"), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &mockWriter{
				claimSlipFn: func(_ context.Context, _, _, _ string) error { return tc.err },
			}
			handler := setupWriteTestAPI(w)

			req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/claim",
				strings.NewReader(`{"claimed_by":"rerunner"}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

func TestClaimSlip_RejectsBadInput(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		body       string
		wantStatus int
	}{
		// claimed_by carries no omitempty, so huma treats it as required.
		{"claimed_by absent", "/slips/abc-123/claim", `{}`, http.StatusUnprocessableEntity},
		{"claimed_by empty", "/slips/abc-123/claim", `{"claimed_by":""}`, http.StatusUnprocessableEntity},
		{
			"claimed_by malformed",
			"/slips/abc-123/claim",
			`{"claimed_by":"rerun ner!"}`,
			http.StatusUnprocessableEntity,
		},
		// Pinned deliberately: huma rejects unknown body properties with 422, which
		// is why a client that learns about a new field must not be deployed ahead of
		// the API that accepts it. Same constraint the dispatch field hit (DEVOPS-341).
		{
			"unknown body field",
			"/slips/abc-123/claim",
			`{"claimed_by":"rerunner","claimed_at":"now"}`,
			http.StatusUnprocessableEntity,
		},
		// Path validation runs before the writer is touched.
		{
			"correlation ID malformed",
			"/slips/abc$123/claim",
			`{"claimed_by":"rerunner"}`,
			http.StatusBadRequest,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &mockWriter{
				claimSlipFn: func(_ context.Context, _, _, _ string) error {
					t.Fatal("writer must not be called for a rejected request")
					return nil
				},
			}
			handler := setupWriteTestAPI(w)

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

// The straggler half of DEVOPS-285: a late write against a correlation ID whose row
// was repaved away must say something an operator can act on. This asserts the
// message names the likely cause and the recovery on an ORDINARY step write, not
// just on the claim endpoint — the straggler case is the one that survives the
// rerunner fix, and it arrives through the step routes.
func TestWriteNotFound_MessageIsActionable(t *testing.T) {
	// Each route gets its own body: huma rejects a property the operation's schema
	// does not declare, so one shared body would 422 before reaching the writer.
	routes := []struct {
		name string
		path string
		body string
		w    *mockWriter
	}{
		{
			"start step",
			"/slips/abc-123/steps/builds_completed/start",
			`{"component_name":"api"}`,
			&mockWriter{startStepFn: func(_ context.Context, _, _, _ string) error {
				return slippy.ErrSlipNotFound
			}},
		},
		{
			"complete step",
			"/slips/abc-123/steps/builds_completed/complete",
			`{"component_name":"api"}`,
			&mockWriter{completeStepFn: func(_ context.Context, _, _, _ string) error {
				return slippy.ErrSlipNotFound
			}},
		},
		{
			"claim",
			"/slips/abc-123/claim",
			`{"claimed_by":"rerunner"}`,
			&mockWriter{claimSlipFn: func(_ context.Context, _, _, _ string) error {
				return slippy.ErrSlipNotFound
			}},
		},
	}
	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			handler := setupWriteTestAPI(tc.w)

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			require.Equal(t, http.StatusNotFound, rec.Code)
			msg := rec.Body.String()
			assert.Contains(t, msg, "replaced by a newer run for the same commit",
				"the 404 must name the likely cause (a repave), not just report absence")
			assert.Contains(t, msg, "current correlation ID",
				"the 404 must tell the operator how to recover")
		})
	}
}

// --- mapWriteError tests ---

func TestMapWriteError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"slip not found", slippy.ErrSlipNotFound, http.StatusNotFound},
		{"invalid correlation ID", slippy.ErrInvalidCorrelationID, http.StatusBadRequest},
		{"invalid repository", slippy.ErrInvalidRepository, http.StatusBadRequest},
		{"invalid configuration", slippy.ErrInvalidConfiguration, http.StatusBadRequest},
		{"invalid push options", errors.New("invalid push options: missing field"), http.StatusBadRequest},
		{"write contended (domain sentinel)", domain.ErrWriteContended, http.StatusServiceUnavailable},
		{
			"write contended (wrapped)",
			fmt.Errorf("writer.StartStep: %w", domain.ErrWriteContended),
			http.StatusServiceUnavailable,
		},
		{"statement timeout (domain sentinel)", domain.ErrStatementTimeout, http.StatusGatewayTimeout},
		{
			"step error",
			slippy.NewStepError("update", "id", "step", "comp", errors.New("fail")),
			http.StatusUnprocessableEntity,
		},
		{"slip error", slippy.NewSlipError("create", "id", errors.New("fail")), http.StatusUnprocessableEntity},
		{"creation in progress (sentinel)", domain.ErrCreationInProgress, http.StatusConflict},
		{
			"creation in progress (wrapped, as returned by writer)",
			fmt.Errorf("dedup: slip for repo:sha creation in progress, retry: %w", domain.ErrCreationInProgress),
			http.StatusConflict,
		},
		// context.Canceled / context.DeadlineExceeded map to 504 GatewayTimeout.
		// These errors are only produced for insert-failure paths (start/skip and
		// pipeline-level complete/fail where componentName == ""), where a write-op
		// timeout fires before UpdateStepWithHistory lands the row.
		//
		// For component-level complete/fail (componentName != ""), the adapter routes
		// through goLib RunPostExecution: the durable insert runs first, then
		// checkPipelineCompletion is called on the same write-op context. A timeout
		// inside checkPipelineCompletion is wrapped by goLib as ErrSlipNotFound or
		// ErrSlipStatusUpdateFailed (see cases above) — so a raw context error is NOT
		// produced for that sub-path, and the component row is always durable when
		// those 404/500 errors surface. See TestComponentCompletePath_GoLibWraps* below.
		{"context canceled", context.Canceled, http.StatusGatewayTimeout},
		{"deadline exceeded", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{
			"context canceled wrapped in StepError",
			slippy.NewStepError("update", "id", "step", "", context.Canceled),
			http.StatusGatewayTimeout,
		},
		{
			"deadline exceeded wrapped in StepError",
			slippy.NewStepError("update", "id", "step", "", context.DeadlineExceeded),
			http.StatusGatewayTimeout,
		},
		{"generic error", errors.New("something broke"), http.StatusInternalServerError},
		// ErrTerminalAlreadyExists is the I5 v2 freshness-gate sentinel; it must
		// map to 409 Conflict so the CLI does not retry a genuinely terminal write.
		{
			"ErrTerminalAlreadyExists (direct sentinel)",
			slippy.ErrTerminalAlreadyExists,
			http.StatusConflict,
		},
		{
			"ErrTerminalAlreadyExists (wrapped)",
			fmt.Errorf("store.UpdateStepWithHistory: %w", slippy.ErrTerminalAlreadyExists),
			http.StatusConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			humaErr := mapWriteError(tt.err)
			var he huma.StatusError
			require.ErrorAs(t, humaErr, &he)
			assert.Equal(t, tt.wantStatus, he.GetStatus())
		})
	}
}

// TestStartStep_ContextDeadlineExceeded_Returns504 verifies the full HTTP
// round-trip: when the writer returns context.DeadlineExceeded the handler must
// respond 504 GatewayTimeout, not 202. A deadline reaching mapWriteError means
// the authoritative insert (slip_component_states) did NOT land — the write-op
// timeout fired before the row was durable. 504 is retryable; the CLI re-POSTs
// to prevent silent data loss. This is the regression guard against the
// incorrectly-added 202 branch.
func TestStartStep_ContextDeadlineExceeded_Returns504(t *testing.T) {
	w := &mockWriter{
		startStepFn: func(_ context.Context, _, _, _ string) error {
			return context.DeadlineExceeded
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/push_parsed/start", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusGatewayTimeout, rec.Code,
		"writer.DeadlineExceeded must yield 504: authoritative row did not land, CLI must retry")
}

// TestCompleteStep_ContextCanceled_Returns504 verifies the same contract for
// context.Canceled. Must be 504 (retryable), not 202 (silent data loss).
func TestCompleteStep_ContextCanceled_Returns504(t *testing.T) {
	w := &mockWriter{
		completeStepFn: func(_ context.Context, _, _, _ string) error {
			return context.Canceled
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(
		http.MethodPost,
		"/slips/abc-123/steps/push_parsed/complete",
		strings.NewReader(`{}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusGatewayTimeout, rec.Code,
		"writer.Canceled must yield 504, not 202: CLI must retry on lost write")
}

// TestComponentCompletePath_GoLibWrapsCheckPipelineCompletion_404 documents the
// REAL error path for component-level Complete when the post-insert pipeline
// completion check times out or cannot find the slip.
//
// goLib executor.go RunPostExecution calls client.CompleteStep (durable insert)
// first, then checkPipelineCompletion on the same write-op context. A timeout
// inside checkPipelineCompletion wraps the error as ErrSlipNotFound:
//
//	return false, "", fmt.Errorf("%w: failed to load slip for completion check: %s", ErrSlipNotFound, err)
//
// That wraps up through RunPostExecution as
// "post-execution completed but pipeline status update failed: %w". So when
// mapWriteError sees an ErrSlipNotFound it produces 404, not 504. The component
// row is already durable at that point; step/aggregate status self-heals on
// the next Load. The slip-level completion not advancing is a known gap tracked
// as a follow-up (derive slip-completion on read).
func TestComponentCompletePath_GoLibWrapsCheckPipelineCompletion_404(t *testing.T) {
	// Simulate goLib RunPostExecution returning the wrapped ErrSlipNotFound that
	// checkPipelineCompletion produces when store.Load times out after the
	// authoritative component insert has already landed.
	wrappedErr := fmt.Errorf("post-execution completed but pipeline status update failed: %w",
		fmt.Errorf("%w: failed to load slip for completion check: context deadline exceeded", slippy.ErrSlipNotFound),
	)

	humaErr := mapWriteError(wrappedErr)
	var he huma.StatusError
	require.ErrorAs(t, humaErr, &he)
	assert.Equal(t, http.StatusNotFound, he.GetStatus(),
		"goLib wraps checkPipelineCompletion timeout as ErrSlipNotFound → 404; "+
			"component row is durable, this is NOT a retryable insert failure (not 504)")
}

// TestComponentCompletePath_GoLibWrapsCheckPipelineCompletion_500 documents the
// parallel path where the completion-check load succeeds but the subsequent slip
// status update fails. goLib wraps that as ErrSlipStatusUpdateFailed:
//
//	return false, SlipStatusFailed, fmt.Errorf("%w: %s", ErrSlipStatusUpdateFailed, err.Error())
//
// mapWriteError has no explicit case for ErrSlipStatusUpdateFailed, so it falls
// to the default StepError/SlipError branches (422) or the generic 500. The
// component row is already durable; the slip status will converge on the next
// checkPipelineCompletion call.
func TestComponentCompletePath_GoLibWrapsCheckPipelineCompletion_500(t *testing.T) {
	// Simulate goLib RunPostExecution returning the wrapped ErrSlipStatusUpdateFailed
	// that checkPipelineCompletion produces when the slip-status UPDATE itself fails
	// after the authoritative component insert already landed.
	wrappedErr := fmt.Errorf("post-execution completed but pipeline status update failed: %w",
		fmt.Errorf("%w: clickhouse: write timeout", slippy.ErrSlipStatusUpdateFailed),
	)

	humaErr := mapWriteError(wrappedErr)
	var he huma.StatusError
	require.ErrorAs(t, humaErr, &he)
	assert.Equal(t, http.StatusInternalServerError, he.GetStatus(),
		"goLib wraps checkPipelineCompletion slip-status-update failure as ErrSlipStatusUpdateFailed → 500; "+
			"component row is durable, slip status self-heals on next event")
}

// --- validateCorrelationIDFormat tests ---

// TestValidateCorrelationIDFormat_Empty_Returns400 verifies that an empty
// correlation ID is rejected directly by validateCorrelationIDFormat with a
// 400 huma error, rather than relying on downstream library behavior
// (defense-in-depth regression guard — the prior uuid.Parse validator
// rejected empty strings, but len("") <= max and an empty range loop would
// otherwise return nil).
func TestValidateCorrelationIDFormat_Empty_Returns400(t *testing.T) {
	err := validateCorrelationIDFormat("")
	require.Error(t, err)

	var he huma.StatusError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusBadRequest, he.GetStatus(),
		"empty correlation_id must be rejected with 400")
}

// TestValidateCorrelationIDFormat_TooLong verifies that a correlation ID
// exceeding 128 characters is rejected with HTTP 400.
func TestValidateCorrelationIDFormat_TooLong_Returns400(t *testing.T) {
	w := &mockWriter{
		startStepFn: func(_ context.Context, _, _, _ string) error { return nil },
	}
	handler := setupWriteTestAPI(w)

	longID := strings.Repeat("a", 129)
	req := httptest.NewRequest(http.MethodPost,
		"/slips/"+longID+"/steps/push_parsed/start", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"correlation_id >128 chars must be rejected with 400")
}

// TestValidateCorrelationIDFormat_BadChar verifies that a correlation ID
// containing ';' (outside [A-Za-z0-9._:-]) is rejected with HTTP 400.
func TestValidateCorrelationIDFormat_BadChar_Returns400(t *testing.T) {
	w := &mockWriter{
		startStepFn: func(_ context.Context, _, _, _ string) error { return nil },
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost,
		"/slips/bad;id/steps/push_parsed/start", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"correlation_id with ';' must be rejected with 400")
}

// TestValidateCorrelationIDFormat_ValidUUIDish verifies that a UUID-style
// correlation ID (chars in [A-Za-z0-9-]) passes validation and reaches the writer.
func TestValidateCorrelationIDFormat_ValidUUIDish_Passes(t *testing.T) {
	var called bool
	w := &mockWriter{
		startStepFn: func(_ context.Context, _, _, _ string) error {
			called = true
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost,
		"/slips/550e8400-e29b-41d4-a716-446655440000/steps/push_parsed/start", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code,
		"UUID-style correlation_id must pass validation")
	assert.True(t, called, "writer must be invoked for a valid correlation_id")
}

// TestCompleteStep_AllowsRecoveryFromFailed verifies that the handler accepts a
// CompleteStep call after a prior FailStep — the documented `failed → completed`
// recovery flow (STATE_MACHINE_V3.md §Recovery Rules). No step-level state is
// immutable.
func TestCompleteStep_AllowsRecoveryFromFailed(t *testing.T) {
	var called bool
	w := &mockWriter{
		completeStepFn: func(_ context.Context, _, _, _ string) error {
			called = true
			return nil
		},
	}
	handler := setupWriteTestAPI(w)

	req := httptest.NewRequest(http.MethodPost, "/slips/abc-123/steps/prod_deploy/complete", nil)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.True(t, called, "writer must be called for failed → completed recovery")
}
