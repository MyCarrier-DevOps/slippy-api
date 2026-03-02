package handler

import (
	"context"
	"encoding/json"
	"errors"
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

// --- Mock ImageTagReader ---

type mockImageTagReader struct {
	resolveImageTagsFn func(ctx context.Context, correlationID string) (*domain.ImageTagResult, error)
}

func (m *mockImageTagReader) ResolveImageTags(ctx context.Context, correlationID string) (*domain.ImageTagResult, error) {
	return m.resolveImageTagsFn(ctx, correlationID)
}

// setupImageTagTestAPI creates a huma API with image-tag routes for testing (no auth middleware).
func setupImageTagTestAPI(imageTagReader domain.ImageTagReader) http.Handler {
	mux := http.NewServeMux()
	config := huma.DefaultConfig("Test Slippy API", "0.0.1")
	api := humago.New(mux, config)

	h := NewImageTagHandler(imageTagReader)
	RegisterImageTagRoutes(api, h)

	return mux
}

func TestGetImageTags_BuildScopeAll(t *testing.T) {
	expected := &domain.ImageTagResult{
		Tags: map[string]string{
			"api":    "26.09.aef1234",
			"worker": "26.09.aef1234",
		},
		BuildScope: domain.BuildScopeAll,
		SlipTag:    "26.09.aef1234",
	}

	mock := &mockImageTagReader{
		resolveImageTagsFn: func(_ context.Context, id string) (*domain.ImageTagResult, error) {
			assert.Equal(t, "corr-001", id)
			return expected, nil
		},
	}

	handler := setupImageTagTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-001/image-tags", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body domain.ImageTagResult
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, domain.BuildScopeAll, body.BuildScope)
	assert.Equal(t, "26.09.aef1234", body.SlipTag)
	assert.Equal(t, "26.09.aef1234", body.Tags["api"])
	assert.Equal(t, "26.09.aef1234", body.Tags["worker"])
}

func TestGetImageTags_BuildScopeModified(t *testing.T) {
	expected := &domain.ImageTagResult{
		Tags: map[string]string{
			"my_component":       "26.09.aef1234",
			"my_other_component": "26.03.a4241ce",
		},
		BuildScope: domain.BuildScopeModified,
		SlipTag:    "26.09.aef1234",
	}

	mock := &mockImageTagReader{
		resolveImageTagsFn: func(_ context.Context, id string) (*domain.ImageTagResult, error) {
			return expected, nil
		},
	}

	handler := setupImageTagTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/corr-002/image-tags", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body domain.ImageTagResult
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, domain.BuildScopeModified, body.BuildScope)
	assert.Equal(t, "26.09.aef1234", body.Tags["my_component"])
	assert.Equal(t, "26.03.a4241ce", body.Tags["my_other_component"])
}

func TestGetImageTags_SlipNotFound(t *testing.T) {
	mock := &mockImageTagReader{
		resolveImageTagsFn: func(_ context.Context, _ string) (*domain.ImageTagResult, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	handler := setupImageTagTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/not-found/image-tags", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetImageTags_InvalidCorrelationID(t *testing.T) {
	mock := &mockImageTagReader{
		resolveImageTagsFn: func(_ context.Context, _ string) (*domain.ImageTagResult, error) {
			return nil, slippy.ErrInvalidCorrelationID
		},
	}

	handler := setupImageTagTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/bad-id/image-tags", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetImageTags_NoBuildInfo(t *testing.T) {
	mock := &mockImageTagReader{
		resolveImageTagsFn: func(_ context.Context, _ string) (*domain.ImageTagResult, error) {
			return nil, errors.New("no ci.buildinfo rows found for correlation ID")
		},
	}

	handler := setupImageTagTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/empty-build/image-tags", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetImageTags_InternalError(t *testing.T) {
	mock := &mockImageTagReader{
		resolveImageTagsFn: func(_ context.Context, _ string) (*domain.ImageTagResult, error) {
			return nil, errors.New("clickhouse connection lost")
		},
	}

	handler := setupImageTagTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/some-id/image-tags", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
