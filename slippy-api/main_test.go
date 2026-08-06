package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse/clickhousetest"

	"github.com/MyCarrier-DevOps/slippy-api/internal/config"
	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
	"github.com/MyCarrier-DevOps/slippy-api/internal/handler"
	"github.com/MyCarrier-DevOps/slippy-api/internal/infrastructure"
	"github.com/MyCarrier-DevOps/slippy-api/internal/middleware"
)

// --- Stub SlipReader for tests ---

// stubSlipReader implements domain.SlipReader with hardcoded test data.
type stubSlipReader struct {
	slips map[string]*domain.Slip
}

func newStubSlipReader() *stubSlipReader {
	return &stubSlipReader{slips: map[string]*domain.Slip{
		"test-corr-001": {
			CorrelationID: "test-corr-001",
			Repository:    "org/my-service",
			Branch:        "main",
			CommitSHA:     "abc123",
		},
	}}
}

func (s *stubSlipReader) Load(_ context.Context, correlationID string) (*domain.Slip, error) {
	slip, ok := s.slips[correlationID]
	if !ok {
		return nil, errors.New("not found")
	}
	return slip, nil
}

func (s *stubSlipReader) LoadByCommit(_ context.Context, _, _ string) (*domain.Slip, error) {
	return nil, errors.New("not implemented")
}

func (s *stubSlipReader) LoadByCommitExact(_ context.Context, _, _ string) (*domain.Slip, error) {
	return nil, errors.New("not implemented")
}

func (s *stubSlipReader) FindByCommits(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
	return nil, "", errors.New("not implemented")
}

func (s *stubSlipReader) FindAllByCommits(_ context.Context, _ string, _ []string) ([]domain.SlipWithCommit, error) {
	return nil, errors.New("not implemented")
}

// --- Stub readers for optional handlers (used in spec generation) ---

type stubImageTagReader struct{}

func (s *stubImageTagReader) ResolveImageTags(_ context.Context, _ string) (*domain.ImageTagResult, error) {
	return &domain.ImageTagResult{}, nil
}

type stubSlipWriter struct{}

func (s *stubSlipWriter) CreateSlipForPush(
	_ context.Context,
	opts domain.PushOptions,
) (*domain.CreateSlipResult, error) {
	return &domain.CreateSlipResult{Slip: &domain.Slip{CorrelationID: opts.CorrelationID}}, nil
}
func (s *stubSlipWriter) StartStep(_ context.Context, _, _, _ string) error    { return nil }
func (s *stubSlipWriter) CompleteStep(_ context.Context, _, _, _ string) error { return nil }
func (s *stubSlipWriter) FailStep(_ context.Context, _, _, _, _ string) error  { return nil }
func (s *stubSlipWriter) SkipStep(_ context.Context, _, _, _, _ string) error  { return nil }
func (s *stubSlipWriter) SetComponentImageTag(_ context.Context, _, _, _ string) error {
	return nil
}
func (s *stubSlipWriter) PromoteSlip(_ context.Context, _, _ string) error { return nil }
func (s *stubSlipWriter) AbandonSlip(_ context.Context, _, _ string) error { return nil }

type stubCIJobLogReader struct{}

func (s *stubCIJobLogReader) QueryLogs(_ context.Context, _ *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
	return &domain.CIJobLogResult{}, nil
}

type stubAutomationTestResultsReader struct{}

func (s *stubAutomationTestResultsReader) QueryAutomationTestResults(
	_ context.Context,
	_ *domain.AutomationTestResultsQuery,
) (*domain.AutomationTestResultsResult, error) {
	return &domain.AutomationTestResultsResult{}, nil
}

type stubAutomationTestsReader struct{}

func (s *stubAutomationTestsReader) QueryTestsByCorrelation(
	_ context.Context,
	_ *domain.AutomationTestsByCorrelationQuery,
) (*domain.AutomationTestsResult, error) {
	return &domain.AutomationTestsResult{}, nil
}

func (s *stubAutomationTestsReader) LoadTestByCorrelation(
	_ context.Context,
	_ *domain.LoadTestByCorrelationQuery,
) (*domain.AutomationTestResult, error) {
	return nil, domain.ErrTestNotFound
}

// --- buildHandler tests ---

func TestBuildHandler_HealthEndpoint(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)
	require.NotNil(t, h)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestBuildHandler_AuthRequired(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)

	// Request without auth header should be rejected
	req := httptest.NewRequest(http.MethodGet, "/slips/test-corr-001", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestBuildHandler_AuthSuccess(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)

	// Request with valid auth header should succeed
	req := httptest.NewRequest(http.MethodGet, "/slips/test-corr-001", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var slip domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&slip))
	assert.Equal(t, "test-corr-001", slip.CorrelationID)
}

func TestBuildHandler_OpenAPISpec(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))

	info, ok := spec["info"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Slippy API", info["title"])
	assert.Equal(t, "API for CI/CD routing slips", info["description"])
}

// --- v1 versioned endpoint tests ---

func TestBuildHandler_V1HealthEndpoint(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestBuildHandler_V1AuthRequired(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/slips/test-corr-001", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestBuildHandler_V1AuthSuccess(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/slips/test-corr-001", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var slip domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&slip))
	assert.Equal(t, "test-corr-001", slip.CorrelationID)
}

func TestBuildHandler_OpenAPISpecContainsV1Routes(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(cfg, reader, nil, nil, nil, nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))

	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)

	// Verify both unversioned (legacy) and v1 paths exist
	assert.Contains(t, paths, "/health")
	assert.Contains(t, paths, "/v1/health")
	assert.Contains(t, paths, "/slips/{correlationID}")
	assert.Contains(t, paths, "/v1/slips/{correlationID}")
}

// --- Route security audit ---

// fetchOpenAPISpec serves GET /openapi.json against h and decodes the document.
func fetchOpenAPISpec(t *testing.T, h http.Handler) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))
	return spec
}

// mockClickHouseSession returns a ClickHouse session whose only answer is
// "the schema_version table does not exist".
//
// That is enough for the diagnostic handler to complete: clickhousemigrator's
// GetSchemaVersion scans a count() first and short-circuits to version 0 when it is
// zero, so one row satisfies the whole call and no second query is issued. A live
// session is not needed to prove the route's auth behavior, only a non-nil one — the
// handler dereferences session.Conn() as soon as a request clears auth.
func mockClickHouseSession() ch.ClickhouseSessionInterface {
	return &clickhousetest.MockSession{
		ConnConn: &clickhousetest.MockConn{
			QueryRowRow: &clickhousetest.MockRow{ScanData: []any{uint64(0)}},
		},
	}
}

// buildFullyWiredHandler builds the handler with every route-registering dependency
// supplied, so the OpenAPI document covers every operation buildHandler can register.
//
// pipelineCfg is nil, which is not an omission: main.go registers the pipeline-config
// and step-prerequisites routes unconditionally, so their operations appear in the
// document either way. Only the handlers guarded by a nil check need supplying.
//
// The diagnostics handler gets a mock ClickHouse session rather than nil so requests
// that clear auth can reach it — see TestBuildHandler_DiagnosticRouteRequiresKey.
func buildFullyWiredHandler(t *testing.T) http.Handler {
	t.Helper()

	cfg := &config.Config{APIKey: "test-key", WriteAPIKey: "write-key", Port: 8080}
	h := buildHandler(
		cfg,
		newStubSlipReader(),
		&stubSlipWriter{},
		&stubImageTagReader{},
		&stubCIJobLogReader{},
		&stubAutomationTestResultsReader{},
		&stubAutomationTestsReader{},
		nil,
		handler.NewDiagnosticsHandler(mockClickHouseSession(), "slippy"),
	)
	require.NotNil(t, h)
	return h
}

// operationsExcludedFromPublishedSpec names operations that buildFullyWiredHandler
// registers but TestGenerateOpenAPISpec deliberately leaves out of api/v1/*.json.
//
// The two constructions differ, so the audited surface is a strict superset of the
// published contract. That divergence is intentional — publishing the diagnostic
// would add a path and a schema to the committed spec and a method to slippy-client,
// which is outside DEVOPS-217 — but it must not be able to grow silently, so the
// exclusion is named here and asserted in both directions.
var operationsExcludedFromPublishedSpec = []string{"get-clickhouse-schema-version"}

// TestBuildHandler_EveryOperationIsSecuredOrAllowlisted is the registration-time
// half of fail-closed auth (DEVOPS-217). The middleware rejects any operation that
// requires no credential and is not allowlisted; this walks the OpenAPI document and
// fails the build for such a route rather than letting it ship — open under the old
// opt-in model, uniformly 401 under the new one.
//
// It also checks that declarations are *enforceable*, not merely present. The
// middleware tiers on literal scheme names, so an unrecognized name is enforced at
// the write tier and an empty requirement object means "no credential required" —
// both are declaration mistakes the middleware handles safely but silently, and this
// names them at build time instead.
//
// Scope, precisely: this walks the operations that huma.Register puts in the document
// built by buildFullyWiredHandler. Three kinds of route are outside it by
// construction — operations marked Hidden (huma omits them from the document),
// routes huma registers on the adapter rather than through huma.Register (pinned
// separately by TestBuildHandler_CredentialFreeAdapterRoutes), and any route behind a
// config branch this fixture does not enable. Hidden and config-gated routes are
// still subject to the middleware at runtime, so omission from this audit is
// fail-closed for them, not fail-open; only the adapter routes genuinely bypass auth.
func TestBuildHandler_EveryOperationIsSecuredOrAllowlisted(t *testing.T) {
	spec := fetchOpenAPISpec(t, buildFullyWiredHandler(t))

	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)
	require.NotEmpty(t, paths)

	// Pin the document's scheme set against what the middleware actually tiers.
	// Adding a third scheme must force a look at internal/middleware/auth.go, because
	// requiresWriteKey enforces every name it does not know at the write tier.
	components, ok := spec["components"].(map[string]any)
	require.True(t, ok)
	declaredSchemes, ok := components["securitySchemes"].(map[string]any)
	require.True(t, ok)
	for name := range declaredSchemes {
		assert.Contains(t, middleware.KnownSecuritySchemes(), name,
			"components.securitySchemes declares %q, which middleware.requiresWriteKey does not "+
				"tier explicitly — it would be enforced at the write tier, so read callers get 403. "+
				"Add it to the read condition in internal/middleware/auth.go or drop it.", name)
	}

	seenOps := map[string]struct{}{}
	seenRoutes := map[string]struct{}{}
	for path, methodsAny := range paths {
		methods, ok := methodsAny.(map[string]any)
		require.True(t, ok, "path %s", path)

		for method, opAny := range methods {
			op, ok := opAny.(map[string]any)
			require.True(t, ok, "%s %s", method, path)

			opID, _ := op["operationId"].(string)
			seenOps[opID] = struct{}{}
			seenRoutes[strings.ToUpper(method)+" "+path] = struct{}{}

			security, _ := op["security"].([]any)
			if len(security) == 0 {
				assert.True(t, middleware.IsPublicRoute(method, path),
					"%s %s (operationId %q) declares no security requirement and is not on the public "+
						"allowlist in internal/middleware/auth.go, so it would be rejected with 401. "+
						"Declare Security on the operation, or add %q to publicRoutes if it is "+
						"deliberately public.",
					strings.ToUpper(method), path, opID, strings.ToUpper(method)+" "+path)
				continue
			}

			for _, requirementAny := range security {
				requirement, ok := requirementAny.(map[string]any)
				require.True(t, ok, "%s %s security requirement", method, path)

				assert.NotEmpty(t, requirement,
					"%s %s (operationId %q) declares an empty security requirement ({}), which OpenAPI "+
						"reads as optional auth. The middleware treats that as requiring no credential "+
						"and rejects it with 401. Name a scheme, or leave Security unset and allowlist "+
						"the route.",
					strings.ToUpper(method), path, opID)

				for name := range requirement {
					assert.Contains(t, declaredSchemes, name,
						"%s %s (operationId %q) declares security scheme %q, which is absent from "+
							"components.securitySchemes — most likely a typo. The middleware would "+
							"enforce it at the write tier, so read callers get 403.",
						strings.ToUpper(method), path, opID, name)
				}
			}
		}
	}

	// The reverse direction, which a forward-only walk cannot check: every allowlist
	// entry must still match a registered route. An entry whose route was renamed or
	// regrouped is dead config that grants nothing under a method+path key — but it is
	// also the exact residue the old operation-ID key turned into a free credential,
	// so it fails the build rather than lingering.
	for _, route := range middleware.PublicRoutes() {
		assert.Contains(t, seenRoutes, route,
			"publicRoutes in internal/middleware/auth.go allowlists %q, but no registered operation "+
				"serves it. Remove the stale entry or restore the route.", route)
	}

	// Guard against a vacuous pass: the audit must have covered a public route, a
	// read-key route on both prefixes, a write-key route, and the diagnostic.
	for _, opID := range []string{
		"health-check",
		"v1-health-check",
		"get-slip",
		"v1-get-slip",
		"create-slip",
		"get-clickhouse-schema-version",
	} {
		assert.Contains(t, seenOps, opID, "expected the audit to cover operation %q", opID)
	}
}

// TestBuildHandler_CredentialFreeAdapterRoutes pins the routes that genuinely bypass
// the auth middleware.
//
// huma.Register is the only path that wraps a handler in api.Middlewares(); the spec
// and docs handlers are registered straight onto the adapter by huma.NewAPI, so they
// are served with no credential and never appear in spec["paths"] — which puts them
// permanently outside the route audit above. DefaultConfig enables all six, and
// main.go uses it unmodified.
//
// This test is the gate that turns that prose into an assertion: a huma upgrade adding
// a seventh, or a config change moving one, shows up here instead of silently widening
// the credential-free surface.
func TestBuildHandler_CredentialFreeAdapterRoutes(t *testing.T) {
	h := buildFullyWiredHandler(t)

	adapterRoutes := []string{
		"/openapi.json",
		"/openapi-3.0.json",
		"/openapi.yaml",
		"/openapi-3.0.yaml",
		"/docs",
		// A real component schema. huma's schema handler answers 200 with a JSON
		// "null" for an unknown name, so an invented one would pass vacuously.
		"/schemas/ErrorModel.json",
	}
	for _, path := range adapterRoutes {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code,
				"%s is expected to be served without a credential", path)
			assert.NotEmpty(t, w.Body.Bytes())
			assert.NotEqual(t, "null", strings.TrimSpace(w.Body.String()),
				"%s answered 200 with a null body — the probe is not hitting real content", path)
		})
	}

	// None of them is an operation in the document, so the route audit cannot see
	// them. That is why they need this test rather than an allowlist entry.
	spec := fetchOpenAPISpec(t, h)
	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)
	for _, path := range adapterRoutes {
		assert.NotContains(t, paths, path)
	}
}

// TestGenerateOpenAPISpec_PublishedSurfaceMatchesAudited pins the deliberate gap
// between the audited route surface and the published contract.
//
// buildFullyWiredHandler supplies the diagnostics handler; TestGenerateOpenAPISpec
// does not, so api/v1/openapi.json and the generated slippy-client omit the
// diagnostic. That is intentional and out of scope for DEVOPS-217, but two divergent
// "wire everything" constructions in one file will drift, and CI auto-commits the
// regenerated spec. This asserts the difference is exactly the named exclusion — in
// both directions — so a second omission cannot slip in unnoticed.
func TestGenerateOpenAPISpec_PublishedSurfaceMatchesAudited(t *testing.T) {
	audited := operationIDs(t, fetchOpenAPISpec(t, buildFullyWiredHandler(t)))
	published := operationIDs(t, fetchOpenAPISpec(t, buildSpecGenerationHandler()))

	for _, opID := range operationsExcludedFromPublishedSpec {
		assert.Contains(t, audited, opID, "excluded operation %q should still be audited", opID)
		assert.NotContains(t, published, opID,
			"operation %q is named in operationsExcludedFromPublishedSpec but appears in the "+
				"published spec — remove it from the exclusion list", opID)
	}

	for opID := range audited {
		if slices.Contains(operationsExcludedFromPublishedSpec, opID) {
			continue
		}
		assert.Contains(t, published, opID,
			"operation %q is audited but missing from the published spec. Either register it in "+
				"TestGenerateOpenAPISpec's handler, or add it to "+
				"operationsExcludedFromPublishedSpec with a reason.", opID)
	}
}

// operationIDs collects every operationId in an OpenAPI document.
func operationIDs(t *testing.T, spec map[string]any) map[string]struct{} {
	t.Helper()

	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)

	ids := map[string]struct{}{}
	for _, methodsAny := range paths {
		methods, ok := methodsAny.(map[string]any)
		require.True(t, ok)
		for _, opAny := range methods {
			op, ok := opAny.(map[string]any)
			require.True(t, ok)
			if id, ok := op["operationId"].(string); ok {
				ids[id] = struct{}{}
			}
		}
	}
	return ids
}

// TestBuildHandler_DiagnosticRouteIsRenamedAndSecured pins the other half of
// DEVOPS-217: the ClickHouse schema-version probe no longer sits under /v1/admin/
// — a namespace that reads as privileged and invited genuinely administrative
// (and unauthenticated) additions — and it now requires the read key.
func TestBuildHandler_DiagnosticRouteIsRenamedAndSecured(t *testing.T) {
	spec := fetchOpenAPISpec(t, buildFullyWiredHandler(t))

	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)

	assert.NotContains(t, paths, "/admin/schema-version")
	assert.NotContains(t, paths, "/v1/admin/schema-version")
	require.Contains(t, paths, "/v1/diagnostics/clickhouse-schema-version")

	methods, ok := paths["/v1/diagnostics/clickhouse-schema-version"].(map[string]any)
	require.True(t, ok)
	op, ok := methods["get"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "get-clickhouse-schema-version", op["operationId"])

	security, ok := op["security"].([]any)
	require.True(t, ok, "diagnostic must declare a security requirement")
	require.Len(t, security, 1)
	scheme, ok := security[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, scheme, "apiKey", "diagnostic should accept the read key")
}

// TestBuildHandler_DiagnosticRouteRequiresKey exercises the renamed route end to end
// through the middleware, in both directions.
//
// The positive path is the point: DEVOPS-217's headline change to this route is
// unauthenticated -> read-key-required, and a 401-only test cannot tell "auth
// enforced" from "route broken" — flipping apiKeySecurity to writeApiKeySecurity, or
// regressing the read branch, would pass. Asserting that the read key gets a 200
// requires a handler that survives being reached, which is why
// buildFullyWiredHandler supplies a mock ClickHouse session rather than nil.
func TestBuildHandler_DiagnosticRouteRequiresKey(t *testing.T) {
	const diagnosticPath = "/v1/diagnostics/clickhouse-schema-version"

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{"no credential", "", http.StatusUnauthorized},
		{"read key accepted", "test-key", http.StatusOK},
		{"write key accepted", "write-key", http.StatusOK},
		{"wrong key rejected", "nope", http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := buildFullyWiredHandler(t)

			req := httptest.NewRequest(http.MethodGet, diagnosticPath, nil)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			assert.Equal(t, tt.wantStatus, w.Code)
			if tt.wantStatus == http.StatusOK {
				var body map[string]any
				require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
				assert.Contains(t, body, "current",
					"a request that clears auth must reach the handler and get a version back")
			}
		})
	}

	t.Run("retired admin path is gone", func(t *testing.T) {
		h := buildFullyWiredHandler(t)

		req := httptest.NewRequest(http.MethodGet, "/v1/admin/schema-version", nil)
		req.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

// --- Optional handler registration tests ---

// TestBuildHandler_WithAllOptionalHandlers exercises the conditional branches
// in buildHandler that register image-tag, ci-job-log, and write routes when
// their respective readers/writer are non-nil.
func TestBuildHandler_WithAllOptionalHandlers(t *testing.T) {
	cfg := &config.Config{APIKey: "test-key", WriteAPIKey: "write-key", Port: 8080}
	reader := newStubSlipReader()

	h := buildHandler(
		cfg,
		reader,
		&stubSlipWriter{},
		&stubImageTagReader{},
		&stubCIJobLogReader{},
		&stubAutomationTestResultsReader{},
		&stubAutomationTestsReader{},
		nil,
		nil,
	)
	require.NotNil(t, h)

	// The OpenAPI spec should now contain paths registered via each optional handler.
	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))
	paths, ok := spec["paths"].(map[string]any)
	require.True(t, ok)

	// Image tag routes registered (expect a /v1/image-tags or similar path).
	// Write routes registered on /v1 only.
	var hasWriteRoute, hasImageTagRoute, hasCIJobLogRoute bool
	for path := range paths {
		if strings.Contains(path, "image-tag") || strings.Contains(path, "imagetag") {
			hasImageTagRoute = true
		}
		if strings.Contains(path, "ci-job-log") || strings.Contains(path, "cijoblog") ||
			strings.Contains(path, "logs") {
			hasCIJobLogRoute = true
		}
		if strings.HasPrefix(path, "/v1/") && (strings.Contains(path, "slip") || strings.Contains(path, "step")) {
			// Write routes register under /v1 for slip/step mutation endpoints.
			// Any /v1/slips* POST/PUT/etc. indicates the writer path was exercised.
			if ops, ok := paths[path].(map[string]any); ok {
				for method := range ops {
					if method == "post" || method == "put" || method == "patch" {
						hasWriteRoute = true
					}
				}
			}
		}
	}
	assert.True(t, hasImageTagRoute, "expected image tag route to be registered")
	assert.True(t, hasCIJobLogRoute, "expected ci job log route to be registered")
	assert.True(t, hasWriteRoute, "expected write route to be registered")
}

// --- Spec generation (gated behind GENERATE_SPEC=1) ---

// buildSpecGenerationHandler builds the handler whose OpenAPI document is published
// to api/v1/*.json and, via make generate-client, to slippy-client.
//
// It deliberately omits the diagnostics handler, so the published contract is the
// audited surface minus operationsExcludedFromPublishedSpec. Keep the two in step
// through that list — TestGenerateOpenAPISpec_PublishedSurfaceMatchesAudited asserts
// the difference is exactly what is named there.
func buildSpecGenerationHandler() http.Handler {
	cfg := &config.Config{APIKey: "dummy", Port: 8080}
	return buildHandler(
		cfg,
		newStubSlipReader(),
		&stubSlipWriter{},
		&stubImageTagReader{},
		&stubCIJobLogReader{},
		&stubAutomationTestResultsReader{},
		&stubAutomationTestsReader{},
		nil,
		nil,
	)
}

func TestGenerateOpenAPISpec(t *testing.T) {
	if os.Getenv("GENERATE_SPEC") == "" {
		t.Skip("set GENERATE_SPEC=1 to regenerate OpenAPI spec files")
	}

	h := buildSpecGenerationHandler()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Pretty-print the JSON
	var spec map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&spec))
	formatted, err := json.MarshalIndent(spec, "", "  ")
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll("api/v1", 0o755))
	require.NoError(t, os.WriteFile("api/v1/openapi.json", formatted, 0o644))
	t.Logf("wrote api/v1/openapi.json (%d bytes)", len(formatted))

	// Also produce a v1-only, OpenAPI 3.0.3 compatible spec for client generation.
	v1Spec := buildV1OnlySpec(t, spec)
	v1Formatted, err := json.MarshalIndent(v1Spec, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile("api/v1/openapi-v1.json", v1Formatted, 0o644))
	t.Logf("wrote api/v1/openapi-v1.json (%d bytes)", len(v1Formatted))
}

// buildV1OnlySpec filters the full spec to v1 paths only, strips the /v1 prefix,
// cleans up v1- operation ID prefixes, and downconverts OpenAPI 3.1 nullable
// syntax to 3.0.3. The "v1" tag is preserved for Swagger UI grouping.
func buildV1OnlySpec(t *testing.T, full map[string]any) map[string]any {
	t.Helper()

	// Deep-copy via JSON round-trip.
	raw, err := json.Marshal(full)
	require.NoError(t, err)
	var spec map[string]any
	require.NoError(t, json.Unmarshal(raw, &spec))

	// Downgrade version.
	spec["openapi"] = "3.0.3"

	// Filter paths: keep only /v1/ prefixed, strip prefix, clean operation IDs.
	oldPaths, _ := spec["paths"].(map[string]any)
	newPaths := make(map[string]any)
	for path, methods := range oldPaths {
		if !strings.HasPrefix(path, "/v1") {
			continue
		}
		stripped := strings.TrimPrefix(path, "/v1")
		if stripped == "" {
			stripped = "/"
		}

		// Clean operation IDs on each method.
		methodMap, ok := methods.(map[string]any)
		if !ok {
			continue
		}
		for _, opAny := range methodMap {
			op, ok := opAny.(map[string]any)
			if !ok {
				continue
			}
			if id, ok := op["operationId"].(string); ok {
				op["operationId"] = strings.TrimPrefix(id, "v1-")
			}
		}
		newPaths[stripped] = methods
	}
	spec["paths"] = newPaths

	// Downconvert 3.1 nullable types to 3.0 format throughout the spec.
	downconvertNullable(spec)

	return spec
}

// downconvertNullable recursively converts {"type": ["array", "null"]} to
// {"type": "array", "nullable": true} for OpenAPI 3.0 compatibility.
func downconvertNullable(v any) {
	switch val := v.(type) {
	case map[string]any:
		if typeVal, ok := val["type"]; ok {
			if arr, ok := typeVal.([]any); ok {
				var nonNull string
				hasNull := false
				for _, item := range arr {
					s, _ := item.(string)
					if s == "null" {
						hasNull = true
					} else {
						nonNull = s
					}
				}
				if hasNull && nonNull != "" {
					val["type"] = nonNull
					val["nullable"] = true
				}
			}
		}
		for _, child := range val {
			downconvertNullable(child)
		}
	case []any:
		for _, item := range val {
			downconvertNullable(item)
		}
	}
}

// --- connectCache tests ---

func TestConnectCache_Disabled(t *testing.T) {
	// When DragonflyHost is empty, CacheEnabled() returns false.
	cfg := &config.Config{DragonflyHost: ""}
	reader := newStubSlipReader()

	// dial should never be called when cache is disabled
	dial := func(_ *redis.Options) redis.Cmdable {
		t.Fatal("dial should not be called when cache is disabled")
		return nil
	}

	result, rdb := connectCache(cfg, reader, dial)
	// Should return the original reader unchanged and a nil client.
	assert.Equal(t, reader, result)
	assert.Nil(t, rdb)
}

func TestConnectCache_PingFailure(t *testing.T) {
	cfg := &config.Config{
		DragonflyHost:     "localhost",
		DragonflyPort:     16379,
		DragonflyPassword: "",
		CacheTTL:          5 * time.Minute,
	}
	reader := newStubSlipReader()

	// Create a real redis client pointing at a bad address — ping will fail.
	dial := func(opts *redis.Options) redis.Cmdable {
		opts.DialTimeout = 100 * time.Millisecond
		opts.ReadTimeout = 100 * time.Millisecond
		return redis.NewClient(opts)
	}

	result, rdb := connectCache(cfg, reader, dial)
	// On ping failure, the original reader is returned (caching disabled gracefully)
	// and no client is surfaced (dedup lock disabled / fail-open).
	assert.Equal(t, reader, result)
	assert.Nil(t, rdb)
}

func TestConnectCache_PingSuccess(t *testing.T) {
	// Use miniredis (in-memory pure-Go Redis) instead of testcontainers to
	// eliminate Docker daemon dependency and AzDO .UnitTestV2 flake.
	mr := miniredis.RunT(t)

	port, err := strconv.Atoi(mr.Port())
	require.NoError(t, err)

	cfg := &config.Config{
		DragonflyHost: mr.Host(),
		DragonflyPort: port,
		CacheTTL:      5 * time.Minute,
	}
	reader := newStubSlipReader()

	dial := func(o *redis.Options) redis.Cmdable {
		return redis.NewClient(o)
	}

	result, rdb := connectCache(cfg, reader, dial)
	// Should return a CachedSlipReader, not the original reader
	assert.NotEqual(t, reader, result)
	_, isCached := result.(*infrastructure.CachedSlipReader)
	assert.True(t, isCached, "expected CachedSlipReader when ping succeeds")
	// On success the live client is surfaced so the dedup Locker can reuse it.
	assert.NotNil(t, rdb)
}

func TestConnectCache_PassesCorrectOptions(t *testing.T) {
	cfg := &config.Config{
		DragonflyHost:     "my-dragonfly.host",
		DragonflyPort:     6380,
		DragonflyPassword: "secret-pass",
		CacheTTL:          3 * time.Minute,
	}
	reader := newStubSlipReader()

	var capturedOpts *redis.Options
	// Capture the options passed to dial, but return a client that will fail ping
	dial := func(opts *redis.Options) redis.Cmdable {
		capturedOpts = opts
		opts.DialTimeout = 100 * time.Millisecond
		opts.ReadTimeout = 100 * time.Millisecond
		return redis.NewClient(opts)
	}

	_, _ = connectCache(cfg, reader, dial)

	// Verify the correct address and password were passed
	require.NotNil(t, capturedOpts)
	assert.Equal(t, "my-dragonfly.host:6380", capturedOpts.Addr)
	assert.Equal(t, "secret-pass", capturedOpts.Password)
}

// --- run() error path tests ---

// clearRunEnv unsets all environment variables that run() depends on.
func clearRunEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SLIPPY_API_KEY", "SLIPPY_WRITE_API_KEY", "PORT",
		"DRAGONFLY_HOST", "DRAGONFLY_PORT", "DRAGONFLY_PASSWORD",
		"CACHE_TTL",
		"SLIPPY_GITHUB_APP_ID", "SLIPPY_GITHUB_APP_PRIVATE_KEY",
		"SLIPPY_GITHUB_ENTERPRISE_URL", "SLIPPY_ANCESTRY_DEPTH",
		"CLICKHOUSE_HOSTNAME", "CLICKHOUSE_PORT", "CLICKHOUSE_USERNAME",
		"CLICKHOUSE_PASSWORD", "CLICKHOUSE_DATABASE", "CLICKHOUSE_SKIP_VERIFY",
		"POSTGRES_HOSTNAME", "POSTGRES_USERNAME", "POSTGRES_PASSWORD",
		"POSTGRES_DATABASE", "POSTGRES_PORT", "POSTGRES_SSLMODE",
		"POSTGRES_MAX_CONNS", "POSTGRES_MIN_CONNS", "POSTGRES_CONN_MAX_LIFETIME",
		"SLIPPY_PIPELINE_CONFIG",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

func TestRun_MissingAPIKey(t *testing.T) {
	clearRunEnv(t)

	err := run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config:")
}

func TestRun_MissingPipelineConfig(t *testing.T) {
	clearRunEnv(t)
	t.Setenv("SLIPPY_API_KEY", "test-key")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	// config.Load() succeeds, but LoadPipelineConfig() will fail
	// because SLIPPY_PIPELINE_CONFIG is not set
	err := run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pipeline config:")
}

func TestRun_MissingPostgresConfig(t *testing.T) {
	clearRunEnv(t)
	t.Setenv("SLIPPY_API_KEY", "test-key")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	// Provide a valid inline pipeline config so we get past the pipeline step.
	t.Setenv(
		"SLIPPY_PIPELINE_CONFIG",
		`{"version":"1.0","name":"test","steps":[{"name":"build","description":"build"}]}`,
	)

	// config.Load() and pipeline config succeed, but PostgresLoadConfig() fails because
	// POSTGRES_HOSTNAME is required. Postgres (the slip store) is now validated before the
	// ClickHouse session used by the non-slip readers.
	err := run()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres config")
}

func TestIsEncryptingSSLMode(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want bool
	}{
		{"verify-full", true},
		{"verify-ca", true},
		{"require", true},
		{"prefer", false},
		{"allow", false},
		{"disable", false},
		{"", false},
	} {
		if got := isEncryptingSSLMode(tc.mode); got != tc.want {
			t.Errorf("isEncryptingSSLMode(%q) = %v, want %v", tc.mode, got, tc.want)
		}
	}
}
