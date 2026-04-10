package handler

import (
	"context"
	"encoding/json"
	"errors"
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
	setComponentImageTagFn func(ctx context.Context, correlationID, componentName, imageTag string) error
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
func (m *mockWriter) SetComponentImageTag(ctx context.Context, cID, comp, tag string) error {
	return m.setComponentImageTagFn(ctx, cID, comp, tag)
}

// setupWriteTestAPI creates a huma API with write routes and no auth for testing.
func setupWriteTestAPI(w domain.SlipWriter) http.Handler {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test", "1.0.0")
	api := humago.New(mux, cfg)

	h := NewSlipWriteHandler(w)
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
		{
			"step error",
			slippy.NewStepError("update", "id", "step", "comp", errors.New("fail")),
			http.StatusUnprocessableEntity,
		},
		{"slip error", slippy.NewSlipError("create", "id", errors.New("fail")), http.StatusUnprocessableEntity},
		{"generic error", errors.New("something broke"), http.StatusInternalServerError},
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
