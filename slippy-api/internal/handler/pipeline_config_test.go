package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// setupPipelineConfigTestAPI creates a huma API with only the pipeline config route.
func setupPipelineConfigTestAPI(cfg *slippy.PipelineConfig) http.Handler {
	mux := http.NewServeMux()
	config := huma.DefaultConfig("Test API", "0.0.1")
	api := humago.New(mux, config)
	h := NewPipelineConfigHandler(cfg)
	RegisterPipelineConfigRoutes(api, h)
	return mux
}

func newTestPipelineConfig(t *testing.T) *slippy.PipelineConfig {
	t.Helper()
	raw := []byte(`{
		"version": "1.0",
		"name": "test-pipeline",
		"steps": [
			{"name": "build", "description": "build step", "prerequisites": []},
			{"name": "builds", "description": "aggregate builds", "prerequisites": [], "aggregates": "build"},
			{"name": "dev_deploy", "description": "deploy to dev", "prerequisites": ["builds"]},
			{"name": "prod_gate", "description": "prod gate", "is_gate": true, "prerequisites": ["dev_deploy"]}
		]
	}`)
	cfg, err := slippy.ParsePipelineConfig(raw)
	require.NoError(t, err)
	return cfg
}

func TestGetPipelineConfig_Success(t *testing.T) {
	cfg := newTestPipelineConfig(t)
	handler := setupPipelineConfigTestAPI(cfg)

	req := httptest.NewRequest(http.MethodGet, "/pipeline-config", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body PipelineConfigResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "1.0", body.Version)
	assert.Equal(t, "test-pipeline", body.Name)
	assert.Len(t, body.Steps, 4)

	// Verify the aggregate step has IsAggregate=true and AggregateOf set.
	var buildsStep *PipelineConfigStep
	for i := range body.Steps {
		if body.Steps[i].Name == "builds" {
			buildsStep = &body.Steps[i]
		}
	}
	require.NotNil(t, buildsStep, "expected 'builds' step in response")
	assert.True(t, buildsStep.IsAggregate)
	assert.Equal(t, "build", buildsStep.AggregateOf)

	// Verify gate step.
	var gateStep *PipelineConfigStep
	for i := range body.Steps {
		if body.Steps[i].Name == "prod_gate" {
			gateStep = &body.Steps[i]
		}
	}
	require.NotNil(t, gateStep, "expected 'prod_gate' step in response")
	assert.True(t, gateStep.IsGate)
}

func TestGetPipelineConfig_StepPrerequisites(t *testing.T) {
	cfg := newTestPipelineConfig(t)
	handler := setupPipelineConfigTestAPI(cfg)

	req := httptest.NewRequest(http.MethodGet, "/pipeline-config", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body PipelineConfigResponseBody
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))

	// dev_deploy should list "builds" as a prerequisite.
	var devDeploy *PipelineConfigStep
	for i := range body.Steps {
		if body.Steps[i].Name == "dev_deploy" {
			devDeploy = &body.Steps[i]
		}
	}
	require.NotNil(t, devDeploy)
	assert.Equal(t, []string{"builds"}, devDeploy.Prerequisites)
}

func TestGetPipelineConfig_NilConfig(t *testing.T) {
	handler := setupPipelineConfigTestAPI(nil)

	req := httptest.NewRequest(http.MethodGet, "/pipeline-config", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestNewPipelineConfigHandler(t *testing.T) {
	cfg := newTestPipelineConfig(t)
	h := NewPipelineConfigHandler(cfg)
	assert.NotNil(t, h)
	assert.Equal(t, cfg, h.cfg)
}
