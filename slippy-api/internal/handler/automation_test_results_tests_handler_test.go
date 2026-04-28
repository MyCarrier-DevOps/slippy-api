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

type mockAutomationTestsReader struct {
	queryFn  func(ctx context.Context, q *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error)
	loadByID func(ctx context.Context, q *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error)
}

func (m *mockAutomationTestsReader) QueryTests(
	ctx context.Context,
	q *domain.AutomationTestsQuery,
) (*domain.AutomationTestsResult, error) {
	if m.queryFn == nil {
		return &domain.AutomationTestsResult{}, nil
	}
	return m.queryFn(ctx, q)
}

func (m *mockAutomationTestsReader) LoadTestByID(
	ctx context.Context,
	q *domain.LoadTestByIDQuery,
) (*domain.AutomationTestResult, error) {
	if m.loadByID == nil {
		return nil, domain.ErrTestNotFound
	}
	return m.loadByID(ctx, q)
}

type testsResponse struct {
	Tests    []domain.AutomationTestResult `json:"tests"`
	NextPage string                        `json:"next_page"`
	Count    int                           `json:"count"`
}

// resolvedRunsFor returns a single matching RunResults row that the parent
// reader hands back to the handler. The fields here are what runsToKeysAndWindow
// then turns into a ResolvedRunKey + a [StartTime, FinishTime+buffer] window.
func resolvedRunsFor(start, finish time.Time) *domain.AutomationTestResultsResult {
	return &domain.AutomationTestResultsResult{
		Runs: []domain.AutomationTestRunResult{
			{
				ReleaseID:       "26.04.abc1234",
				Attempt:         1,
				Stage:           "FeatureCoreApi",
				EnvironmentName: "prod",
				StackName:       "stk1",
				StartTime:       start,
				FinishTime:      finish,
			},
		},
		Count: 1,
	}
}

// --- by correlationId -----------------------------------------------------

func TestGetTestsByCorrelationID_Success(t *testing.T) {
	corrID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	tFinish := tStart.Add(2 * time.Minute)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			assert.Equal(t, corrID, q.CorrelationID)
			return resolvedRunsFor(tStart, tFinish), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			require.Len(t, q.Runs, 1)
			assert.Equal(t, "26.04.abc1234", q.Runs[0].ReleaseID)
			assert.Equal(t, uint8(1), q.Runs[0].Attempt)
			assert.Equal(t, tStart, q.MinStart)
			// MaxFinish should include the 5-minute buffer
			assert.Equal(t, tFinish.Add(5*time.Minute), q.MaxFinish)
			assert.Equal(t, "Failed", q.Status, "default status should be Failed")
			return &domain.AutomationTestsResult{
				Tests: []domain.AutomationTestResult{
					{TestName: "TestThing", ResultStatus: "Failed", StartTime: tStart, TestID: "abc"},
				},
				Count: 1,
			}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-correlation/"+corrID.String()+"/tests", nil)
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
	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			t.Fatal("parent reader should not be called for invalid UUID")
			return nil, nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			t.Fatal("tests reader should not be called for invalid UUID")
			return nil, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-correlation/not-a-uuid/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestsByCorrelationID_RunResolutionError(t *testing.T) {
	corrID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return nil, errors.New("clickhouse connection lost")
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			t.Fatal("tests reader should not be called when run resolution fails")
			return nil, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-correlation/"+corrID.String()+"/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetTestsByCorrelationID_TestQueryError(t *testing.T) {
	corrID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			return nil, errors.New("test query failed")
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-correlation/"+corrID.String()+"/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetTestsByCorrelationID_InvalidCursor(t *testing.T) {
	corrID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			return nil, domain.ErrInvalidTestsCursor
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(
		http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests?cursor=bad",
		nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestsByCorrelationID_EmptyResolvedRuns(t *testing.T) {
	corrID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return &domain.AutomationTestResultsResult{Runs: nil, Count: 0}, nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			t.Fatal("tests reader should not be called when no runs match")
			return nil, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-correlation/"+corrID.String()+"/tests", nil)
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
	corrID := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			assert.Equal(t, "prod", q.Environment)
			assert.Equal(t, "stk1", q.Stack)
			assert.Equal(t, "FeatureCoreApi", q.Stage)
			assert.Equal(t, uint32(2), q.Attempt)
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			assert.Equal(t, "Passed", q.Status)
			assert.Equal(t, 50, q.Limit)
			return &domain.AutomationTestsResult{Tests: []domain.AutomationTestResult{}, Count: 0}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
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

func TestGetTestsByCorrelationID_NextPageURL(t *testing.T) {
	corrID := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			return &domain.AutomationTestsResult{
				Tests:      []domain.AutomationTestResult{{TestName: "T", ResultStatus: "Failed"}},
				Count:      1,
				NextCursor: "2026-04-01T12:00:00Z|abcd",
			}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(
		http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests?environment=prod&limit=25",
		nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body testsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Contains(t, body.NextPage, "/v1/automation-test-results/")
	assert.Contains(t, body.NextPage, "/tests?")
	assert.Contains(t, body.NextPage, "cursor=")
	assert.Contains(t, body.NextPage, "limit=25")
	assert.Contains(t, body.NextPage, "environment=prod")
	// status was not set on the request, so it should not appear in next_page;
	// the next request will still default to Failed via resolveStatus.
	assert.NotContains(t, body.NextPage, "status=")
}

// --- by releaseId ---------------------------------------------------------

func TestGetTestsByRelease_Success(t *testing.T) {
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryByReleaseFn: func(_ context.Context, q *domain.AutomationTestResultsByReleaseQuery) (*domain.AutomationTestResultsResult, error) {
			assert.Equal(t, "abc1234", q.ReleaseIDSubstring)
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			assert.Equal(t, "Failed", q.Status)
			require.Len(t, q.Runs, 1)
			return &domain.AutomationTestsResult{
				Tests: []domain.AutomationTestResult{{TestName: "X", ResultStatus: "Failed"}},
				Count: 1,
			}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-release/abc1234/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body testsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, 1, body.Count)
}

func TestGetTestsByRelease_TooShort(t *testing.T) {
	parent := &mockAutomationTestResultsReader{
		queryByReleaseFn: func(_ context.Context, _ *domain.AutomationTestResultsByReleaseQuery) (*domain.AutomationTestResultsResult, error) {
			t.Fatal("parent reader should not be called for too-short releaseId")
			return nil, nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			t.Fatal("tests reader should not be called for too-short releaseId")
			return nil, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-release/abc123/tests", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestsByRelease_StatusOverrideAllSentinel(t *testing.T) {
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryByReleaseFn: func(_ context.Context, _ *domain.AutomationTestResultsByReleaseQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			assert.Equal(t, "", q.Status, "status=* should disable the filter")
			return &domain.AutomationTestsResult{Tests: []domain.AutomationTestResult{}, Count: 0}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-release/abc1234/tests?status=*", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGetTestsByRelease_NextPageURL(t *testing.T) {
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryByReleaseFn: func(_ context.Context, _ *domain.AutomationTestResultsByReleaseQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestsQuery) (*domain.AutomationTestsResult, error) {
			return &domain.AutomationTestsResult{
				Tests:      []domain.AutomationTestResult{{TestName: "T", ResultStatus: "Failed"}},
				Count:      1,
				NextCursor: "2026-04-01T12:00:00Z|cafe",
			}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/by-release/abc1234/tests?attempt=3", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body testsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Contains(t, body.NextPage, "/v1/automation-test-results/by-release/abc1234/tests?")
	assert.Contains(t, body.NextPage, "cursor=")
	assert.Contains(t, body.NextPage, "attempt=3")
}

// --- single-test (stack-trace) drilldown ---------------------------------

func TestGetTestByCorrelationID_Success(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-1111-1111-1111-111111111111")
	testID := uuid.MustParse("bbbbbbbb-1111-1111-1111-111111111111")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	tFinish := tStart.Add(time.Minute)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tFinish), nil
		},
	}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, q *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			assert.Equal(t, testID, q.TestID)
			require.Len(t, q.Runs, 1)
			assert.Equal(t, "26.04.abc1234", q.Runs[0].ReleaseID)
			assert.Equal(t, tStart, q.MinStart)
			assert.Equal(t, tFinish.Add(5*time.Minute), q.MaxFinish)
			return &domain.AutomationTestResult{
				TestID:       testID.String(),
				TestName:     "TestThing",
				Feature:      "FeatureCoreApi",
				ResultStatus: "Failed",
				StackTrace:   "panic: oh no\n  at line 1",
			}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
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
	parent := &mockAutomationTestResultsReader{}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("loadByID should not be called for invalid correlationID")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	testID := uuid.MustParse("bbbbbbbb-1111-1111-1111-111111111111")
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/not-a-uuid/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestByCorrelationID_InvalidTestID(t *testing.T) {
	parent := &mockAutomationTestResultsReader{}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("loadByID should not be called for invalid testId")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	corrID := uuid.MustParse("aaaaaaaa-1111-1111-1111-111111111111")
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/not-a-uuid", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestByCorrelationID_RunResolutionError(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-2222-2222-2222-222222222222")
	testID := uuid.MustParse("bbbbbbbb-2222-2222-2222-222222222222")
	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return nil, errors.New("clickhouse down")
		},
	}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("loadByID should not be called when run resolution fails")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetTestByCorrelationID_EmptyResolvedRuns404(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-3333-3333-3333-333333333333")
	testID := uuid.MustParse("bbbbbbbb-3333-3333-3333-333333333333")
	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return &domain.AutomationTestResultsResult{Runs: nil}, nil
		},
	}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("loadByID should not be called when no runs match")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetTestByCorrelationID_TestNotFound404(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-4444-4444-4444-444444444444")
	testID := uuid.MustParse("bbbbbbbb-4444-4444-4444-444444444444")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			return nil, domain.ErrTestNotFound
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetTestByCorrelationID_LoadError500(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-5555-5555-5555-555555555555")
	testID := uuid.MustParse("bbbbbbbb-5555-5555-5555-555555555555")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			return nil, errors.New("clickhouse exploded")
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-correlation/"+corrID.String()+"/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetTestByRelease_Success(t *testing.T) {
	testID := uuid.MustParse("cccccccc-1111-1111-1111-111111111111")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	parent := &mockAutomationTestResultsReader{
		queryByReleaseFn: func(_ context.Context, q *domain.AutomationTestResultsByReleaseQuery) (*domain.AutomationTestResultsResult, error) {
			assert.Equal(t, "abc1234", q.ReleaseIDSubstring)
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, q *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			assert.Equal(t, testID, q.TestID)
			return &domain.AutomationTestResult{
				TestID:       testID.String(),
				TestName:     "TestX",
				ResultStatus: "Failed",
				StackTrace:   "boom",
			}, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-release/abc1234/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var body domain.AutomationTestResult
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "boom", body.StackTrace)
}

func TestGetTestByRelease_TooShort(t *testing.T) {
	testID := uuid.MustParse("cccccccc-2222-2222-2222-222222222222")
	parent := &mockAutomationTestResultsReader{}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("loadByID should not be called for too-short releaseId")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-release/abc123/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestByRelease_InvalidTestID(t *testing.T) {
	parent := &mockAutomationTestResultsReader{}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			t.Fatal("loadByID should not be called for invalid testId")
			return nil, nil
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-release/abc1234/tests/not-a-uuid", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTestByRelease_TestNotFound(t *testing.T) {
	testID := uuid.MustParse("cccccccc-3333-3333-3333-333333333333")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	parent := &mockAutomationTestResultsReader{
		queryByReleaseFn: func(_ context.Context, _ *domain.AutomationTestResultsByReleaseQuery) (*domain.AutomationTestResultsResult, error) {
			return resolvedRunsFor(tStart, tStart.Add(time.Minute)), nil
		},
	}
	tests := &mockAutomationTestsReader{
		loadByID: func(_ context.Context, _ *domain.LoadTestByIDQuery) (*domain.AutomationTestResult, error) {
			return nil, domain.ErrTestNotFound
		},
	}
	handler := setupAutomationTestResultsTestAPIWithTests(parent, tests)
	req := httptest.NewRequest(http.MethodGet,
		"/automation-test-results/by-release/abc1234/tests/"+testID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --- registration gating --------------------------------------------------

func TestTestsRoutes_NotRegisteredWhenReaderNil(t *testing.T) {
	parent := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return &domain.AutomationTestResultsResult{}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPIWithTests(parent, nil)
	req := httptest.NewRequest(
		http.MethodGet,
		"/automation-test-results/by-correlation/11111111-1111-1111-1111-111111111111/tests",
		nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// With no testsReader, the /tests route is not registered → ServeMux returns 404
	assert.Equal(t, http.StatusNotFound, w.Code)
}
