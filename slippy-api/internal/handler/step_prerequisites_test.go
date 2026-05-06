package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// setupPrereqTestAPI creates a huma API with only the prerequisites route.
func setupPrereqTestAPI(reader domain.SlipReader, cfg *slippy.PipelineConfig) http.Handler {
	mux := http.NewServeMux()
	apiConfig := huma.DefaultConfig("Test API", "0.0.1")
	api := humago.New(mux, apiConfig)
	h := NewStepPrerequisitesHandler(reader, cfg)
	RegisterStepPrerequisitesRoutes(api, h)
	return mux
}

const prereqPipelineConfigJSON = `{
	"version": "1.0",
	"name": "prereq-test-pipeline",
	"steps": [
		{"name": "build", "description": "component build", "prerequisites": []},
		{"name": "builds", "description": "aggregate builds", "prerequisites": [], "aggregates": "build"},
		{"name": "unit_tests", "description": "unit tests", "prerequisites": []},
		{"name": "dev_deploy", "description": "deploy to dev", "prerequisites": ["builds", "unit_tests"]},
		{"name": "no_prereqs", "description": "step with no prerequisites", "prerequisites": []}
	]
}`

func newPrereqPipelineConfig(t *testing.T) *slippy.PipelineConfig {
	t.Helper()
	cfg, err := slippy.ParsePipelineConfig([]byte(prereqPipelineConfigJSON))
	require.NoError(t, err)
	return cfg
}

func newSlipWithSteps(corrID string, steps map[string]slippy.Step) *domain.Slip {
	return &domain.Slip{
		CorrelationID: corrID,
		Steps:         steps,
	}
}

// TestGetStepPrerequisites_AllSatisfied verifies satisfied=true when all prereqs complete.
func TestGetStepPrerequisites_AllSatisfied(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	slip := newSlipWithSteps("corr-001", map[string]slippy.Step{
		"builds":     {Status: slippy.StepStatusCompleted},
		"unit_tests": {Status: slippy.StepStatusCompleted},
	})

	reader := &mockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			assert.Equal(t, "corr-001", id)
			return slip, nil
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-001/step-prerequisites/dev_deploy", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body StepPrerequisitesResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))

	assert.True(t, body.Satisfied)
	assert.Empty(t, body.Pending)
	assert.Empty(t, body.Failed)
	assert.Equal(t, slippy.StepStatusCompleted, body.PrereqStatuses["builds"])
	assert.Equal(t, slippy.StepStatusCompleted, body.PrereqStatuses["unit_tests"])
}

// TestGetStepPrerequisites_Pending verifies satisfied=false when a prereq is still running.
func TestGetStepPrerequisites_Pending(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	slip := newSlipWithSteps("corr-002", map[string]slippy.Step{
		"builds":     {Status: slippy.StepStatusCompleted},
		"unit_tests": {Status: slippy.StepStatusRunning},
	})

	reader := &mockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-002/step-prerequisites/dev_deploy", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body StepPrerequisitesResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))

	assert.False(t, body.Satisfied)
	assert.Contains(t, body.Pending, "unit_tests")
	assert.Empty(t, body.Failed)
}

// TestGetStepPrerequisites_FailedWithReason verifies failed prereqs include reason strings.
func TestGetStepPrerequisites_FailedWithReason(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	errorMsg := "build container OOM"
	slip := newSlipWithSteps("corr-003", map[string]slippy.Step{
		"builds":     {Status: slippy.StepStatusFailed, Error: errorMsg},
		"unit_tests": {Status: slippy.StepStatusCompleted},
	})

	reader := &mockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-003/step-prerequisites/dev_deploy", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body StepPrerequisitesResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))

	assert.False(t, body.Satisfied)
	assert.Empty(t, body.Pending)
	require.Len(t, body.Failed, 1)
	assert.Equal(t, "builds", body.Failed[0].StepName)
	assert.Equal(t, errorMsg, body.Failed[0].Reason)
}

// TestGetStepPrerequisites_MissingSlip verifies 404 when the slip doesn't exist.
func TestGetStepPrerequisites_MissingSlip(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	reader := &mockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/missing-corr/step-prerequisites/dev_deploy", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestGetStepPrerequisites_UnknownStep verifies 400 when step is not in pipeline config.
func TestGetStepPrerequisites_UnknownStep(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	reader := &mockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return newSlipWithSteps("corr-004", nil), nil
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-004/step-prerequisites/nonexistent_step", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestGetStepPrerequisites_AggregateExpansion verifies prereqs referencing aggregate steps
// use the aggregate step entry from slip.Steps.
func TestGetStepPrerequisites_AggregateExpansion(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	slip := newSlipWithSteps("corr-005", map[string]slippy.Step{
		"builds":     {Status: slippy.StepStatusCompleted},
		"unit_tests": {Status: slippy.StepStatusCompleted},
	})

	reader := &mockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-005/step-prerequisites/dev_deploy", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body StepPrerequisitesResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))

	assert.True(t, body.Satisfied)
	assert.Contains(t, body.PrereqStatuses, "builds")
	assert.Contains(t, body.PrereqStatuses, "unit_tests")
}

// TestGetStepPrerequisites_NoPrerequisites verifies satisfied=true for step with no prereqs.
func TestGetStepPrerequisites_NoPrerequisites(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	slip := newSlipWithSteps("corr-006", map[string]slippy.Step{})

	reader := &mockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-006/step-prerequisites/no_prereqs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body StepPrerequisitesResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))

	assert.True(t, body.Satisfied)
	assert.Empty(t, body.Pending)
	assert.Empty(t, body.Failed)
}

// TestGetStepPrerequisites_MissingStepInSlip verifies pending when a prereq step
// is not yet in slip.Steps.
func TestGetStepPrerequisites_MissingStepInSlip(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)

	slip := newSlipWithSteps("corr-007", map[string]slippy.Step{
		"builds": {Status: slippy.StepStatusCompleted},
	})

	reader := &mockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	h := setupPrereqTestAPI(reader, cfg)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-007/step-prerequisites/dev_deploy", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body StepPrerequisitesResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))

	assert.False(t, body.Satisfied)
	assert.Contains(t, body.Pending, "unit_tests")
	assert.Equal(t, slippy.StepStatusPending, body.PrereqStatuses["unit_tests"])
}

func TestNewStepPrerequisitesHandler(t *testing.T) {
	cfg := newPrereqPipelineConfig(t)
	reader := &mockReader{}
	h := NewStepPrerequisitesHandler(reader, cfg)
	assert.NotNil(t, h)
}
