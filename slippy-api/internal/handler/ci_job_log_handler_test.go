package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- Mock CIJobLogReader ---

type mockCIJobLogReader struct {
	queryLogsFn func(ctx context.Context, query *domain.CIJobLogQuery) (*domain.CIJobLogResult, error)
}

func (m *mockCIJobLogReader) QueryLogs(
	ctx context.Context,
	query *domain.CIJobLogQuery,
) (*domain.CIJobLogResult, error) {
	return m.queryLogsFn(ctx, query)
}

// setupCIJobLogTestAPI creates a huma API with CI job log routes for testing (no auth middleware).
func setupCIJobLogTestAPI(reader domain.CIJobLogReader) http.Handler {
	mux := http.NewServeMux()
	config := huma.DefaultConfig("Test Slippy API", "0.0.1")
	api := humago.New(mux, config)

	h := NewCIJobLogHandler(reader)
	RegisterCIJobLogRoutes(api, h)

	return mux
}

// logsResponse mirrors the JSON shape of GetLogsOutput.Body for decoding.
type logsResponse struct {
	Logs     []domain.CIJobLog `json:"logs"`
	NextPage string            `json:"next_page"`
	Count    int               `json:"count"`
}

func TestGetLogs_Success(t *testing.T) {
	ts := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	mock := &mockCIJobLogReader{
		queryLogsFn: func(_ context.Context, q *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
			assert.Equal(t, "corr-001", q.CorrelationID)
			assert.Equal(t, 100, q.Limit)
			assert.Equal(t, domain.SortDesc, q.Sort)
			return &domain.CIJobLogResult{
				Logs: []domain.CIJobLog{
					{Timestamp: ts, Level: "ERROR", Service: "svc1", Component: "comp1",
						Cluster: "cl", Cloud: "az", Environment: "prod", Namespace: "ns",
						Message: "something failed", CIJobInstance: "inst1", CIJobType: "deploy",
						BuildRepository: "repo1", BuildImage: "img1", BuildBranch: "main"},
				},
				Count: 1,
			}, nil
		},
	}

	handler := setupCIJobLogTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/logs/corr-001", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body logsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, 1, body.Count)
	assert.Empty(t, body.NextPage)
	require.Len(t, body.Logs, 1)
	assert.Equal(t, "ERROR", body.Logs[0].Level)
	assert.Equal(t, "svc1", body.Logs[0].Service)
}

func TestGetLogs_WithNextPage(t *testing.T) {
	ts := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	cursor := ts.Format(time.RFC3339Nano)

	mock := &mockCIJobLogReader{
		queryLogsFn: func(_ context.Context, _ *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
			return &domain.CIJobLogResult{
				Logs: []domain.CIJobLog{
					{Timestamp: ts, Level: "INFO", Service: "svc"},
				},
				NextCursor: cursor,
				Count:      1,
			}, nil
		},
	}

	handler := setupCIJobLogTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/logs/corr-002?limit=1&sort=desc", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body logsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.NotEmpty(t, body.NextPage)
	assert.Contains(t, body.NextPage, "/logs/corr-002")
	assert.Contains(t, body.NextPage, "cursor=")
	assert.Contains(t, body.NextPage, "limit=1")
	assert.Contains(t, body.NextPage, "sort=desc")
}

func TestGetLogs_EmptyResults(t *testing.T) {
	mock := &mockCIJobLogReader{
		queryLogsFn: func(_ context.Context, _ *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
			return &domain.CIJobLogResult{
				Logs:  nil,
				Count: 0,
			}, nil
		},
	}

	handler := setupCIJobLogTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/logs/corr-empty", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body logsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, 0, body.Count)
	assert.Empty(t, body.NextPage)
	// Logs should be an empty array, not null
	assert.NotNil(t, body.Logs)
	assert.Empty(t, body.Logs)
}

func TestGetLogs_InternalError(t *testing.T) {
	mock := &mockCIJobLogReader{
		queryLogsFn: func(_ context.Context, _ *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
			return nil, errors.New("clickhouse connection lost")
		},
	}

	handler := setupCIJobLogTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/logs/corr-err", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetLogs_InvalidCursorError(t *testing.T) {
	mock := &mockCIJobLogReader{
		queryLogsFn: func(_ context.Context, _ *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
			return nil, domain.ErrInvalidCursor
		},
	}

	handler := setupCIJobLogTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/logs/corr-bad?cursor=not-valid", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetLogs_FiltersPassedToReader(t *testing.T) {
	mock := &mockCIJobLogReader{
		queryLogsFn: func(_ context.Context, q *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
			assert.Equal(t, "ERROR", q.Level)
			assert.Equal(t, "my-service", q.Service)
			assert.Equal(t, "prod", q.Environment)
			assert.Equal(t, domain.SortAsc, q.Sort)
			return &domain.CIJobLogResult{Logs: []domain.CIJobLog{}, Count: 0}, nil
		},
	}

	handler := setupCIJobLogTestAPI(mock)
	req := httptest.NewRequest(
		http.MethodGet,
		"/logs/corr-filter?level=ERROR&service=my-service&environment=prod&sort=asc",
		nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGetLogs_DefaultLimitAndSort(t *testing.T) {
	mock := &mockCIJobLogReader{
		queryLogsFn: func(_ context.Context, q *domain.CIJobLogQuery) (*domain.CIJobLogResult, error) {
			assert.Equal(t, 100, q.Limit)
			assert.Equal(t, domain.SortDesc, q.Sort)
			return &domain.CIJobLogResult{Logs: []domain.CIJobLog{}, Count: 0}, nil
		},
	}

	handler := setupCIJobLogTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/logs/corr-defaults", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// --- buildNextPageURL tests ---

func TestBuildNextPageURL_Basic(t *testing.T) {
	input := &GetLogsInput{
		CorrelationID: "corr-001",
		Limit:         50,
		Sort:          "desc",
	}
	cursor := "2026-03-10T12:00:00Z"

	result := buildNextPageURL(input, cursor)
	assert.Contains(t, result, "/logs/corr-001?")
	assert.Contains(t, result, "limit=50")
	assert.Contains(t, result, "sort=desc")
	assert.Contains(t, result, "cursor=")
}

func TestBuildNextPageURL_WithFilters(t *testing.T) {
	input := &GetLogsInput{
		CorrelationID: "corr-002",
		Limit:         100,
		Sort:          "asc",
		Level:         "ERROR",
		Service:       "my-service",
		BuildBranch:   "main",
	}
	cursor := "2026-03-10T12:00:00Z"

	result := buildNextPageURL(input, cursor)
	assert.Contains(t, result, "/logs/corr-002?")
	assert.Contains(t, result, "level=ERROR")
	assert.Contains(t, result, "service=my-service")
	assert.Contains(t, result, "build_branch=main")
	assert.Contains(t, result, "sort=asc")
	// Non-set filters should not appear
	assert.NotContains(t, result, "cloud=")
	assert.NotContains(t, result, "cluster=")
}

func TestBuildNextPageURL_EncodesSpecialCharacters(t *testing.T) {
	input := &GetLogsInput{
		CorrelationID: "corr/special chars",
		Limit:         10,
		Sort:          "desc",
		Message:       "error: something went wrong",
	}
	cursor := "2026-03-10T12:00:00Z"

	result := buildNextPageURL(input, cursor)
	// Correlation ID should be path-escaped
	assert.Contains(t, result, "/logs/corr%2Fspecial%20chars?")
	// Message should be query-escaped
	assert.Contains(t, result, "message=error")
}
