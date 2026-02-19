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

func TestAuthMiddleware_EmptyBearerToken(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer ")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Empty token after "Bearer " should be rejected
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_CaseSensitiveBearer(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	// "bearer" lowercase — should fail as extractBearerToken checks "Bearer " prefix
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "bearer test-secret-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_ResponseBody(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)

	// Verify error response body is valid JSON with expected fields
	var errBody map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&errBody))
	assert.Equal(t, float64(401), errBody["status"])
	assert.Contains(t, errBody["title"], "missing")
}

func TestAuthMiddleware_ForbiddenResponseBody(t *testing.T) {
	handler := setupAuthTestAPI("test-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)

	var errBody map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&errBody))
	assert.Equal(t, float64(403), errBody["status"])
	assert.Contains(t, errBody["title"], "invalid")
}

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		expected string
	}{
		{"valid", "Bearer my-token", "my-token"},
		{"empty header", "", ""},
		{"missing prefix", "my-token", ""},
		{"basic auth", "Basic abc123", ""},
		{"bearer lowercase", "bearer my-token", ""},
		{"only prefix", "Bearer ", ""},
		{"extra whitespace", "Bearer   my-token  ", "my-token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractBearerToken(tt.header))
		})
	}
}
