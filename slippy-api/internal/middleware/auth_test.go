package middleware

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
)

// greetingOutput is a simple response type for test endpoints.
type greetingOutput struct {
	Body struct {
		Message string `json:"message"`
	}
}

// setupAuthTestAPI creates a minimal huma API with auth middleware for testing.
// It registers one protected and one public endpoint.
func setupAuthTestAPI(apiKey string) http.Handler {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test API", "1.0.0")
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"bearer": {
			Type:   "http",
			Scheme: "bearer",
		},
	}
	api := humago.New(mux, cfg)

	api.UseMiddleware(NewAPIKeyAuth(apiKey))

	// Protected endpoint
	huma.Register(api, huma.Operation{
		OperationID: "get-protected",
		Method:      http.MethodGet,
		Path:        "/protected",
		Security: []map[string][]string{
			{"bearer": {}},
		},
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "ok"
		return resp, nil
	})

	// Public endpoint (no security requirements)
	huma.Register(api, huma.Operation{
		OperationID: "get-public",
		Method:      http.MethodGet,
		Path:        "/public",
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "public"
		return resp, nil
	})

	return mux
}

func TestAuthMiddleware_ValidKey(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer test-secret-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "ok", body["message"])
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_InvalidKey(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestAuthMiddleware_MalformedHeader(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Basic abc123")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_PublicEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	// No Authorization header — should still succeed on a public endpoint
	req := httptest.NewRequest(http.MethodGet, "/public", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "public", body["message"])
}
