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

// setupAuthTestAPI creates a minimal huma API with two-key auth middleware.
// It registers a protected read endpoint, a protected write endpoint, and a
// public endpoint.
func setupAuthTestAPI(readKey, writeKey string) http.Handler {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test API", "1.0.0")
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"apiKey":      {Type: "http", Scheme: "bearer"},
		"writeApiKey": {Type: "http", Scheme: "bearer"},
	}
	api := humago.New(mux, cfg)

	api.UseMiddleware(NewAPIKeyAuth(readKey, writeKey))

	// Protected read endpoint
	huma.Register(api, huma.Operation{
		OperationID: "get-protected",
		Method:      http.MethodGet,
		Path:        "/protected",
		Security:    []map[string][]string{{"apiKey": {}}},
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "ok"
		return resp, nil
	})

	// Protected write endpoint
	huma.Register(api, huma.Operation{
		OperationID: "post-write",
		Method:      http.MethodPost,
		Path:        "/write",
		Security:    []map[string][]string{{"writeApiKey": {}}},
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "written"
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

// --- Read endpoint tests ---

func TestAuthMiddleware_ReadKey_ReadEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer read-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "ok", body["message"])
}

func TestAuthMiddleware_WriteKey_ReadEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer write-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_WrongKey_ReadEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

// --- Write endpoint tests ---

func TestAuthMiddleware_WriteKey_WriteEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodPost, "/write", nil)
	req.Header.Set("Authorization", "Bearer write-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "written", body["message"])
}

func TestAuthMiddleware_ReadKey_WriteEndpoint_Forbidden(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodPost, "/write", nil)
	req.Header.Set("Authorization", "Bearer read-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestAuthMiddleware_WrongKey_WriteEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodPost, "/write", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

// --- Write key empty (read-only mode) ---

func TestAuthMiddleware_EmptyWriteKey_WriteEndpoint_Forbidden(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "")

	req := httptest.NewRequest(http.MethodPost, "/write", nil)
	req.Header.Set("Authorization", "Bearer read-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestAuthMiddleware_EmptyWriteKey_ReadEndpoint_OK(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer read-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// --- General auth tests ---

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_MalformedHeader(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Basic abc123")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_PublicEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/public", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "public", body["message"])
}

func TestAuthMiddleware_EmptyBearerToken(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer ")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_CaseSensitiveBearer(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "bearer read-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_ResponseBody_Unauthorized(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	var errBody map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&errBody))
	assert.Equal(t, float64(401), errBody["status"])
	assert.Contains(t, errBody["title"], "missing")
}

func TestAuthMiddleware_ResponseBody_Forbidden(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

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

func TestAuthMiddleware_MissingHeader_WriteEndpoint(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodPost, "/write", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// --- requiresWriteAccess ---

func TestRequiresWriteAccess(t *testing.T) {
	tests := []struct {
		name     string
		security []map[string][]string
		expected bool
	}{
		{"no security", nil, false},
		{"apiKey only", []map[string][]string{{"apiKey": {}}}, false},
		{"writeApiKey only", []map[string][]string{{"writeApiKey": {}}}, true},
		{"both schemes", []map[string][]string{{"apiKey": {}}, {"writeApiKey": {}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &huma.Operation{Security: tt.security}
			assert.Equal(t, tt.expected, requiresWriteAccess(op))
		})
	}
}

// --- extractBearerToken ---

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
