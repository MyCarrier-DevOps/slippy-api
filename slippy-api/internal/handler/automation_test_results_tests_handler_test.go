package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- mock tests reader ----------------------------------------------------

type mockAutomationTestsReader struct {
	queryByCorr func(ctx context.Context, q *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error)
	loadByCorr  func(ctx context.Context, q *domain.LoadTestByCorrelationQuery) (*domain.AutomationTestResult, error)
}

func (m *mockAutomationTestsReader) QueryTestsByCorrelation(
	ctx context.Context,
	q *domain.AutomationTestsByCorrelationQuery,
) (*domain.AutomationTestsResult, error) {
	if m.queryByCorr == nil {
		return &domain.AutomationTestsResult{}, nil
	}
	return m.queryByCorr(ctx, q)
}

func (m *mockAutomationTestsReader) LoadTestByCorrelation(
	ctx context.Context,
	q *domain.LoadTestByCorrelationQuery,
) (*domain.AutomationTestResult, error) {
	if m.loadByCorr == nil {
		return nil, domain.ErrTestNotFound
	}
	return m.loadByCorr(ctx, q)
}

type testsResponse struct {
	Tests    []domain.AutomationTestResult `json:"tests"`
	NextPage string                        `json:"next_page"`
	Count    int                           `json:"count"`
}

// --- /tests list ----------------------------------------------------------

func TestGetTestsByCorrelationID_Success(t *testing.T) {
	corrID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, q *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			assert.Equal(t, corrID, q.CorrelationID)
			assert.Equal(t, "Failed", q.Status, "default status should be Failed")
			assert.Equal(t, 100, q.Limit)
			return &domain.AutomationTestsResult{
				Tests: []domain.AutomationTestResult{
					{TestName: "TestThing", ResultStatus: "Failed", StartTime: tStart, TestID: "abc"},
				},
				Count: 1,
			}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body testsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, 1, body.Count)
	require.Len(t, body.Tests, 1)
	assert.Equal(t, "TestThing", body.Tests[0].TestName)
}

func TestGetTestsByCorrelationID_InvalidUUID(t *testing.T) {
	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, _ *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			t.Fatal("reader should not be called for invalid UUID")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-correlation/not-a-uuid/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestsByCorrelationID_ReaderError(t *testing.T) {
	corrID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, _ *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			return nil, errors.New("clickhouse exploded")
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetTestsByCorrelationID_InvalidCursor(t *testing.T) {
	corrID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, _ *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			return nil, domain.ErrInvalidTestsCursor
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests?cursor=bad", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestsByCorrelationID_EmptyResults(t *testing.T) {
	corrID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, _ *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			return &domain.AutomationTestsResult{Tests: nil, Count: 0}, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body testsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, 0, body.Count)
	assert.NotNil(t, body.Tests)
	assert.Empty(t, body.Tests)
}

func TestGetTestsByCorrelationID_FiltersAndStatusThreaded(t *testing.T) {
	corrID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, q *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			assert.Equal(t, "prod", q.Environment)
			assert.Equal(t, "stk1", q.Stack)
			assert.Equal(t, "FeatureCoreApi", q.Stage)
			assert.Equal(t, uint8(2), q.Attempt)
			assert.Equal(t, "Passed", q.Status)
			assert.Equal(t, 50, q.Limit)
			return &domain.AutomationTestsResult{Tests: []domain.AutomationTestResult{}, Count: 0}, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(
		http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+
			"/tests?environment=prod&stack=stk1&stage=FeatureCoreApi&attempt=2&status=Passed&limit=50",
		nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGetTestsByCorrelationID_StatusAllSentinel(t *testing.T) {
	corrID := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, q *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			assert.Equal(t, "", q.Status, "status=* should disable the filter")
			return &domain.AutomationTestsResult{Tests: []domain.AutomationTestResult{}, Count: 0}, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests?status=*", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGetTestsByCorrelationID_NextPageURL(t *testing.T) {
	corrID := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	tests := &mockAutomationTestsReader{
		queryByCorr: func(_ context.Context, _ *domain.AutomationTestsByCorrelationQuery) (*domain.AutomationTestsResult, error) {
			return &domain.AutomationTestsResult{
				Tests:      []domain.AutomationTestResult{{TestName: "T", ResultStatus: "Failed"}},
				Count:      1,
				NextCursor: "2026-04-01T12:00:00Z|abcd",
			}, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests?environment=prod&limit=25", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body testsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Contains(t, body.NextPage, "/v1/automation-test-results/by-correlation/")
	assert.Contains(t, body.NextPage, "/tests?")
	assert.Contains(t, body.NextPage, "cursor=")
	assert.Contains(t, body.NextPage, "limit=25")
	assert.Contains(t, body.NextPage, "environment=prod")
	// status was not set, should not appear; default Failed re-applies on next call.
	assert.NotContains(t, body.NextPage, "status=")
}

// --- single test (stack-trace) drilldown ---------------------------------

func TestGetTestByCorrelationID_Success(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-1111-1111-1111-111111111111")
	testID := uuid.MustParse("bbbbbbbb-1111-1111-1111-111111111111")

	tests := &mockAutomationTestsReader{
		loadByCorr: func(_ context.Context, q *domain.LoadTestByCorrelationQuery) (*domain.AutomationTestResult, error) {
			assert.Equal(t, corrID, q.CorrelationID)
			assert.Equal(t, testID, q.TestID)
			return &domain.AutomationTestResult{
				TestID:       testID.String(),
				TestName:     "TestThing",
				Feature:      "FeatureCoreApi",
				ResultStatus: "Failed",
				StackTrace:   "panic: oh no\n  at line 1",
			}, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body domain.AutomationTestResult
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, testID.String(), body.TestID)
	assert.Equal(t, "panic: oh no\n  at line 1", body.StackTrace)
}

func TestGetTestByCorrelationID_InvalidCorrelationID(t *testing.T) {
	tests := &mockAutomationTestsReader{
		loadByCorr: func(_ context.Context, _ *domain.LoadTestByCorrelationQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("reader should not be called for invalid correlationID")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	testID := uuid.MustParse("bbbbbbbb-1111-1111-1111-111111111111")
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/not-a-uuid/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestByCorrelationID_InvalidTestID(t *testing.T) {
	tests := &mockAutomationTestsReader{
		loadByCorr: func(_ context.Context, _ *domain.LoadTestByCorrelationQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("reader should not be called for invalid testId")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	corrID := uuid.MustParse("aaaaaaaa-1111-1111-1111-111111111111")
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/not-a-uuid", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestByCorrelationID_NotFound(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-2222-2222-2222-222222222222")
	testID := uuid.MustParse("bbbbbbbb-2222-2222-2222-222222222222")
	tests := &mockAutomationTestsReader{
		loadByCorr: func(_ context.Context, _ *domain.LoadTestByCorrelationQuery) (*domain.AutomationTestResult, error) {
			return nil, domain.ErrTestNotFound
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetTestByCorrelationID_LoadError500(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-3333-3333-3333-333333333333")
	testID := uuid.MustParse("bbbbbbbb-3333-3333-3333-333333333333")
	tests := &mockAutomationTestsReader{
		loadByCorr: func(_ context.Context, _ *domain.LoadTestByCorrelationQuery) (*domain.AutomationTestResult, error) {
			return nil, errors.New("clickhouse exploded")
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --- registration gating --------------------------------------------------

func TestTestsRoutes_NotRegisteredWhenReaderNil(t *testing.T) {
	handler := setupAutomationTestResultsTestAPIWithTests(&mockAutomationTestResultsReader{}, nil)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/11111111-1111-1111-1111-111111111111/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	// With no testsReader, the /tests route is not registered → 404
	assert.Equal(t, http.StatusNotFound, w.Code)
}
