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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

type mockAutomationTestResultsReader struct {
	queryFn func(ctx context.Context, q *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error)
}

func (m *mockAutomationTestResultsReader) QueryAutomationTestResults(
	ctx context.Context,
	q *domain.AutomationTestResultsQuery,
) (*domain.AutomationTestResultsResult, error) {
	return m.queryFn(ctx, q)
}

func setupAutomationTestResultsTestAPI(reader domain.AutomationTestResultsReader) http.Handler {
	mux := http.NewServeMux()
	config := huma.DefaultConfig("Test Slippy API", "0.0.1")
	api := humago.New(mux, config)

	h := NewAutomationTestResultsHandler(reader)
	RegisterAutomationTestResultsRoutes(api, h)

	return mux
}

type automationTestResultsResponse struct {
	Runs  []domain.AutomationTestRunResult `json:"runs"`
	Count int                              `json:"count"`
}

func TestGetAutomationTestResults_Success(t *testing.T) {
	corrID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	tFinish := tStart.Add(time.Minute)

	mock := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			assert.Equal(t, corrID, q.CorrelationID)
			assert.True(t, q.LatestOnly)
			return &domain.AutomationTestResultsResult{
				Runs: []domain.AutomationTestRunResult{
					{
						Outcome:         "Passed",
						Passed:          10,
						Failed:          0,
						StartTime:       tStart,
						FinishTime:      tFinish,
						ReleaseID:       "26.04.abc1234",
						Attempt:         1,
						Stage:           "FeatureCoreApi",
						EnvironmentName: "prod",
						StackName:       "stk1",
					},
				},
				Count: 1,
			}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/"+corrID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body automationTestResultsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, 1, body.Count)
	require.Len(t, body.Runs, 1)
	assert.Equal(t, "Passed", body.Runs[0].Outcome)
	assert.Equal(t, "FeatureCoreApi", body.Runs[0].Stage)
}

func TestGetAutomationTestResults_InvalidUUID(t *testing.T) {
	mock := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			t.Fatal("reader should not be called for invalid UUID")
			return nil, nil
		},
	}

	handler := setupAutomationTestResultsTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/not-a-uuid", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetAutomationTestResults_ReaderError(t *testing.T) {
	corrID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	mock := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return nil, errors.New("clickhouse connection lost")
		},
	}

	handler := setupAutomationTestResultsTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/"+corrID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetAutomationTestResults_EmptyResults(t *testing.T) {
	corrID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	mock := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, _ *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			return &domain.AutomationTestResultsResult{Runs: nil, Count: 0}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/"+corrID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body automationTestResultsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, 0, body.Count)
	assert.NotNil(t, body.Runs)
	assert.Empty(t, body.Runs)
}

func TestGetAutomationTestResults_FiltersPassedToReader(t *testing.T) {
	corrID := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	mock := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			assert.Equal(t, corrID, q.CorrelationID)
			assert.Equal(t, "prod", q.Environment)
			assert.Equal(t, "stk1", q.Stack)
			assert.Equal(t, "FeatureCoreApi", q.Stage)
			assert.Equal(t, uint32(3), q.Attempt)
			assert.False(t, q.LatestOnly)
			return &domain.AutomationTestResultsResult{Runs: []domain.AutomationTestRunResult{}, Count: 0}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPI(mock)
	req := httptest.NewRequest(
		http.MethodGet,
		"/automation-test-results/"+corrID.String()+
			"?environment=prod&stack=stk1&stage=FeatureCoreApi&attempt=3&latest_only=false",
		nil,
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestGetAutomationTestResults_DefaultLatestOnlyTrue(t *testing.T) {
	corrID := uuid.MustParse("55555555-5555-5555-5555-555555555555")

	mock := &mockAutomationTestResultsReader{
		queryFn: func(_ context.Context, q *domain.AutomationTestResultsQuery) (*domain.AutomationTestResultsResult, error) {
			assert.True(t, q.LatestOnly, "latest_only should default to true when omitted")
			assert.Equal(t, uint32(0), q.Attempt)
			return &domain.AutomationTestResultsResult{Runs: []domain.AutomationTestRunResult{}, Count: 0}, nil
		},
	}

	handler := setupAutomationTestResultsTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/automation-test-results/"+corrID.String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
