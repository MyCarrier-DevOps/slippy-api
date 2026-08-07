package middleware

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

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

	// No security requirement and not on the public allowlist. Stands in for a
	// route whose author forgot to declare Security.
	huma.Register(api, huma.Operation{
		OperationID: "get-unsecured",
		Method:      http.MethodGet,
		Path:        "/unsecured",
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "unsecured"
		return resp, nil
	})

	// No security requirement, but explicitly allowlisted as public. Uses the
	// real allowlisted operation ID so the test exercises the shipped policy.
	huma.Register(api, huma.Operation{
		OperationID: "health-check",
		Method:      http.MethodGet,
		Path:        "/health",
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "healthy"
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

// TestAuthMiddleware_NoSecurity_NotAllowlisted_Rejected pins the fail-closed
// default: an operation that declares no security requirement and is not named
// on the public allowlist is rejected, not served. A new route that forgets its
// Security declaration must break here rather than ship world-readable.
func TestAuthMiddleware_NoSecurity_NotAllowlisted_Rejected(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/unsecured", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	var errBody map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&errBody))
	assert.Equal(t, float64(401), errBody["status"])
}

// TestAuthMiddleware_NoSecurity_Allowlisted_OK covers the other side of the
// allowlist: a named public operation is still served without a credential.
func TestAuthMiddleware_NoSecurity_Allowlisted_OK(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "healthy", body["message"])
}

// TestIsPublicRoute pins the shipped allowlist. Widening it is a deliberate security
// decision, so it must show up as a change to this table.
func TestIsPublicRoute(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		path     string
		expected bool
	}{
		{"health", http.MethodGet, "/health", true},
		{"v1 health", http.MethodGet, "/v1/health", true},
		{"lowercase method still matches", "get", "/health", true},
		{"empty method and path", "", "", false},
		{"health on an unregistered prefix", http.MethodGet, "/v2/health", false},
		{"health under a different method", http.MethodPost, "/health", false},
		{"path prefix is not enough", http.MethodGet, "/health/deep", false},
		{"read route", http.MethodGet, "/v1/slips/abc", false},
		{"write route", http.MethodPost, "/v1/slips", false},
		{"diagnostic", http.MethodGet, "/v1/diagnostics/clickhouse-schema-version", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsPublicRoute(tt.method, tt.path))
		})
	}
}

// TestPublicRoutes pins the exported allowlist contents, sorted. The route audit
// checks these back against the registered routes, so the shape matters.
func TestPublicRoutes(t *testing.T) {
	assert.Equal(t, []string{"GET /health", "GET /v1/health"}, PublicRoutes())
}

// TestKnownSecuritySchemes pins the scheme names the middleware tiers explicitly.
// Anything outside this set is enforced at the write tier by requiresWriteKey.
func TestKnownSecuritySchemes(t *testing.T) {
	assert.Equal(t, []string{"apiKey", "writeApiKey"}, KnownSecuritySchemes())
}

// TestServedAtWriteTier_ExportedSurface pins the tiering decision as the startup route
// check sees it, which is the same function the middleware uses — there is no second
// implementation to drift from.
func TestServedAtWriteTier_ExportedSurface(t *testing.T) {
	tests := []struct {
		name     string
		security []map[string][]string
		expected bool
	}{
		{"read scheme", []map[string][]string{{"apiKey": {}}}, false},
		{"write scheme", []map[string][]string{{"writeApiKey": {}}}, true},
		{"capitalisation typo", []map[string][]string{{"writeAPIKey": {}}}, true},
		{"no security", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &huma.Operation{Security: tt.security}
			assert.Equal(t, tt.expected, RequiresWriteKey(op))
		})
	}
}

// TestAuthMiddleware_AllowlistIsKeyedOnRouteNotOperationID covers the escape branch
// that an operation-ID-keyed allowlist could not close: an operation reusing an
// allowlisted ID at a different path. With a method+path key the ID is irrelevant, so
// the impostor is rejected even though it carries the allowlisted name.
//
// Registering it requires Hidden, because a duplicate operation ID otherwise panics
// in huma.OpenAPI.AddOperation — and Hidden is precisely the case the route audit
// cannot see, since hidden operations never enter the OpenAPI document.
func TestAuthMiddleware_AllowlistIsKeyedOnRouteNotOperationID(t *testing.T) {
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("Test API", "1.0.0"))
	api.UseMiddleware(NewAPIKeyAuth("read-key", "write-key"))

	// The genuine allowlisted route.
	huma.Register(api, huma.Operation{
		OperationID: "health-check",
		Method:      http.MethodGet,
		Path:        "/health",
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "healthy"
		return resp, nil
	})

	// An impostor borrowing the allowlisted operation ID at a different path.
	huma.Register(api, huma.Operation{
		OperationID: "health-check",
		Method:      http.MethodGet,
		Path:        "/deep-health",
		Hidden:      true,
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "leaked"
		return resp, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/deep-health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code,
		"an operation reusing an allowlisted ID at a non-allowlisted path must be rejected")

	// The genuine route is unaffected.
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

// TestAuthMiddleware_StaleAllowlistEntryGrantsNothing covers the other escape branch:
// an allowlist entry whose route no longer exists. Under an ID-keyed allowlist a
// stranded entry stayed permanently true and would have made any later operation
// adopting that ID public. Keyed on method+path, an operation with the allowlisted ID
// registered at a path nobody allowlisted gets nothing.
func TestAuthMiddleware_StaleAllowlistEntryGrantsNothing(t *testing.T) {
	// "v1-health-check" is allowlisted as GET /v1/health. Registering it at a
	// different path — what the documented v2 migration in docs/versioning-api.md
	// produces — must not inherit the entry.
	handler := setupNoSecurityTestAPI("v1-health-check", "/probe")

	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestAuthMiddleware_OptionalSecurityIsRejected pins the semantic-absence case.
// OpenAPI reads an empty requirement object as "security optional", i.e. satisfiable
// with no credential, so it must be treated exactly like Security: [] rather than
// being waved past the allowlist into the read tier.
func TestAuthMiddleware_OptionalSecurityIsRejected(t *testing.T) {
	tests := []struct {
		name     string
		security []map[string][]string
	}{
		{"optional only", []map[string][]string{{}}},
		{"read scheme plus optional", []map[string][]string{{"apiKey": {}}, {}}},
		{"write scheme plus optional", []map[string][]string{{"writeApiKey": {}}, {}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			cfg := huma.DefaultConfig("Test API", "1.0.0")
			cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
				"apiKey":      {Type: "http", Scheme: "bearer"},
				"writeApiKey": {Type: "http", Scheme: "bearer"},
			}
			api := humago.New(mux, cfg)
			api.UseMiddleware(NewAPIKeyAuth("read-key", "write-key"))

			huma.Register(api, huma.Operation{
				OperationID: "post-optional-security",
				Method:      http.MethodPost,
				Path:        "/danger",
				Security:    tt.security,
			}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
				resp := &greetingOutput{}
				resp.Body.Message = "mutated"
				return resp, nil
			})

			// No credential, the read key, and the write key must all be refused: the
			// route is not allowlisted, so "security optional" means "not serveable".
			for _, token := range []string{"", "read-key", "write-key"} {
				req := httptest.NewRequest(http.MethodPost, "/danger", nil)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)
				assert.Equal(t, http.StatusUnauthorized, w.Code, "token %q", token)
			}
		})
	}
}

// TestAuthMiddleware_UnknownSchemeRequiresWriteKey pins the inverted tiering default.
// A scheme name the middleware does not know — the "writeAPIKey" capitalisation typo
// is the realistic one — must escalate to the write tier rather than falling through
// to the read check and letting the read key mutate.
func TestAuthMiddleware_UnknownSchemeRequiresWriteKey(t *testing.T) {
	mux := http.NewServeMux()
	cfg := huma.DefaultConfig("Test API", "1.0.0")
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"apiKey":      {Type: "http", Scheme: "bearer"},
		"writeApiKey": {Type: "http", Scheme: "bearer"},
	}
	api := humago.New(mux, cfg)
	api.UseMiddleware(NewAPIKeyAuth("read-key", "write-key"))

	huma.Register(api, huma.Operation{
		OperationID: "post-typo-scheme",
		Method:      http.MethodPost,
		Path:        "/danger",
		Security:    []map[string][]string{{"writeAPIKey": {}}}, // capital API — a typo
	}, func(ctx context.Context, input *struct{}) (*greetingOutput, error) {
		resp := &greetingOutput{}
		resp.Body.Message = "mutated"
		return resp, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/danger", nil)
	req.Header.Set("Authorization", "Bearer read-key")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code, "read key must not satisfy an unknown scheme")

	req = httptest.NewRequest(http.MethodPost, "/danger", nil)
	req.Header.Set("Authorization", "Bearer write-key")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code, "write key satisfies the write tier")
}

// TestAuthMiddleware_NoSecurity_NotAllowlisted_RejectedWithValidKey shows the
// rejection is a property of the operation, not of the caller: even the write
// key does not open a route that was never declared public.
func TestAuthMiddleware_NoSecurity_NotAllowlisted_RejectedWithValidKey(t *testing.T) {
	handler := setupAuthTestAPI("read-key", "write-key")

	req := httptest.NewRequest(http.MethodGet, "/unsecured", nil)
	req.Header.Set("Authorization", "Bearer write-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
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

// --- requiresCredential / requiresWriteKey ---

func TestRequiresCredential(t *testing.T) {
	tests := []struct {
		name     string
		security []map[string][]string
		expected bool
	}{
		{"nil security", nil, false},
		{"empty security list", []map[string][]string{}, false},
		{"optional security", []map[string][]string{{}}, false},
		{"read scheme plus optional", []map[string][]string{{"apiKey": {}}, {}}, false},
		{"write scheme plus optional", []map[string][]string{{"writeApiKey": {}}, {}}, false},
		{"apiKey only", []map[string][]string{{"apiKey": {}}}, true},
		{"writeApiKey only", []map[string][]string{{"writeApiKey": {}}}, true},
		{"both schemes", []map[string][]string{{"apiKey": {}}, {"writeApiKey": {}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &huma.Operation{Security: tt.security}
			assert.Equal(t, tt.expected, RequiresCredential(op))
		})
	}
}

func TestRequiresWriteKey(t *testing.T) {
	tests := []struct {
		name     string
		security []map[string][]string
		expected bool
	}{
		{"no security", nil, false},
		{"apiKey only", []map[string][]string{{"apiKey": {}}}, false},
		{"apiKey with scopes", []map[string][]string{{"apiKey": {"read"}}}, false},
		{"writeApiKey only", []map[string][]string{{"writeApiKey": {}}}, true},
		{"both schemes", []map[string][]string{{"apiKey": {}}, {"writeApiKey": {}}}, true},
		// The inverted default: anything that is not exactly "apiKey" escalates
		// rather than falling through to the read check.
		{"capitalisation typo", []map[string][]string{{"writeAPIKey": {}}}, true},
		{"unknown scheme", []map[string][]string{{"oauth2": {}}}, true},
		{"apiKey ANDed with another scheme", []map[string][]string{{"apiKey": {}, "oauth2Read": {}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &huma.Operation{Security: tt.security}
			assert.Equal(t, tt.expected, RequiresWriteKey(op))
		})
	}
}

// --- Error response headers ---

// TestAuthMiddleware_ErrorResponseContentType pins the Content-Type of auth failures
// against a real server rather than a recorder.
//
// httptest.NewRecorder cannot catch this class of bug: it records header mutations
// made after WriteHeader, so a recorder reports the intended media type even when
// the wire carries a sniffed text/plain. humago's SetStatus calls WriteHeader
// immediately, so header order in writeError is load-bearing and only an actual HTTP
// round trip proves it.
//
// The value must stay application/problem+json: that is what the generated OpenAPI
// document declares for the default response of every operation, and what huma emits
// for the errors it handles itself. Missing and wrong credentials are the two most
// common errors this API returns, so they are the worst ones to have contradict the
// published contract.
func TestAuthMiddleware_ErrorResponseContentType(t *testing.T) {
	srv := httptest.NewServer(setupAuthTestAPI("read-key", "write-key"))
	defer srv.Close()

	tests := []struct {
		name       string
		path       string
		token      string
		wantStatus int
	}{
		{"missing token", "/protected", "", http.StatusUnauthorized},
		{"invalid token", "/protected", "wrong-key", http.StatusForbidden},
		{"not allowlisted", "/unsecured", "", http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+tt.path, nil)
			require.NoError(t, err)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}

			resp, err := srv.Client().Do(req)
			require.NoError(t, err)
			defer func() { require.NoError(t, resp.Body.Close()) }()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Equal(t, "application/problem+json", resp.Header.Get("Content-Type"))
			assert.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var errBody map[string]any
			require.NoError(t, json.Unmarshal(body, &errBody))
			assert.Equal(t, float64(tt.wantStatus), errBody["status"])
		})
	}
}

// --- writeError body-writer failure path ---

// errWriter always fails on Write. Used to exercise writeError's log path
// when the response body cannot be flushed.
type errWriter struct{}

func (errWriter) Write(_ []byte) (int, error) { return 0, errors.New("write failed") }

// stubHumaContext is a minimal huma.Context that records SetStatus/SetHeader
// calls and returns a failing BodyWriter. Every other method is a zero-value
// stub that exists only to satisfy the interface.
type stubHumaContext struct {
	statusSet int
	headers   map[string]string
}

func (s *stubHumaContext) Operation() *huma.Operation        { return &huma.Operation{} }
func (s *stubHumaContext) Context() context.Context          { return context.Background() }
func (s *stubHumaContext) TLS() *tls.ConnectionState         { return nil }
func (s *stubHumaContext) Version() huma.ProtoVersion        { return huma.ProtoVersion{} }
func (s *stubHumaContext) Method() string                    { return "" }
func (s *stubHumaContext) Host() string                      { return "" }
func (s *stubHumaContext) RemoteAddr() string                { return "" }
func (s *stubHumaContext) URL() url.URL                      { return url.URL{} }
func (s *stubHumaContext) Param(_ string) string             { return "" }
func (s *stubHumaContext) Query(_ string) string             { return "" }
func (s *stubHumaContext) Header(_ string) string            { return "" }
func (s *stubHumaContext) EachHeader(_ func(string, string)) {}
func (s *stubHumaContext) BodyReader() io.Reader             { return nil }
func (s *stubHumaContext) GetMultipartForm() (*multipart.Form, error) {
	return nil, errors.New("not supported")
}
func (s *stubHumaContext) SetReadDeadline(_ time.Time) error { return nil }
func (s *stubHumaContext) SetStatus(code int)                { s.statusSet = code }
func (s *stubHumaContext) Status() int                       { return s.statusSet }
func (s *stubHumaContext) SetHeader(name, value string) {
	if s.headers == nil {
		s.headers = map[string]string{}
	}
	s.headers[name] = value
}
func (s *stubHumaContext) AppendHeader(name, value string) { s.SetHeader(name, value) }
func (s *stubHumaContext) BodyWriter() io.Writer           { return errWriter{} }

// TestWriteError_BodyWriterFailure ensures the defensive log branch fires when
// BodyWriter().Write returns an error. Status and Content-Type must still be
// set, and the function must not panic.
func TestWriteError_BodyWriterFailure(t *testing.T) {
	ctx := &stubHumaContext{}
	writeError(ctx, http.StatusForbidden, "forbidden")
	assert.Equal(t, http.StatusForbidden, ctx.statusSet)
	assert.Equal(t, "application/problem+json", ctx.headers["Content-Type"])
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
