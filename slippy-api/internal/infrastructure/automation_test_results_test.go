package infrastructure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse/clickhousetest"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// runResultsSeed is one row of mock data shaped to match the column order in
// AutomationTestResultsStore.QueryAutomationTestResults.
func runResultsSeed(
	startTime, finishTime time.Time,
	testRunID, corrID, batchID *uuid.UUID,
) []any {
	return []any{
		"Passed",         // Outcome
		uint32(10),       // Passed
		uint32(0),        // Failed
		startTime,        // StartTime
		finishTime,       // FinishTime
		"26.04.abc1234",  // ReleaseId
		uint32(1),        // Attempt
		"FeatureCoreApi", // Stage
		"prod",           // EnvironmentName
		"stk1",           // StackName
		"",               // ErrorMessage
		"main",           // BranchName
		"attempt-id-1",   // AttemptId
		testRunID,        // TestRunId
		corrID,           // CorrelationId
		"job-1",          // JobNumber
		batchID,          // BatchId
		uint32(1),        // TotalTestJobCount
	}
}

func TestQueryAutomationTestResults_BasicSuccess(t *testing.T) {
	corrID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	tFinish := tStart.Add(2 * time.Minute)
	testRunID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	batchID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "autotest_results.RunResults")
			assert.Contains(t, query, "CorrelationId = toUUID({correlationId:String})")
			assert.Contains(t, query, "LIMIT 1 BY (EnvironmentName, StackName, Stage)")
			// Confirm the named arg passed through with the parsed UUID value
			foundCorr := false
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok && np.Name == "correlationId" {
					assert.Equal(t, corrID.String(), np.Value)
					foundCorr = true
				}
			}
			assert.True(t, foundCorr, "expected correlationId named arg")
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					runResultsSeed(tStart, tFinish, &testRunID, &corrID, &batchID),
				}),
			}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	result, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Count)
	require.Len(t, result.Runs, 1)
	row := result.Runs[0]
	assert.Equal(t, "Passed", row.Outcome)
	assert.Equal(t, uint32(10), row.Passed)
	assert.Equal(t, "FeatureCoreApi", row.Stage)
	assert.Equal(t, "prod", row.EnvironmentName)
	require.NotNil(t, row.TestRunID)
	assert.Equal(t, testRunID.String(), *row.TestRunID)
	require.NotNil(t, row.CorrelationID)
	assert.Equal(t, corrID.String(), *row.CorrelationID)
	require.NotNil(t, row.BatchID)
	assert.Equal(t, batchID.String(), *row.BatchID)
}

func TestQueryAutomationTestResults_FiltersApplied(t *testing.T) {
	corrID := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "EnvironmentName ILIKE {environment:String}")
			assert.Contains(t, query, "StackName ILIKE {stack:String}")
			assert.Contains(t, query, "Stage ILIKE {stage:String}")
			// Confirm named args carry the filter values
			values := map[string]any{}
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					values[np.Name] = np.Value
				}
			}
			assert.Equal(t, "prod", values["environment"])
			assert.Equal(t, "stk1", values["stack"])
			assert.Equal(t, "FeatureCoreApi", values["stage"])
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	_, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
		Environment:   "prod",
		Stack:         "stk1",
		Stage:         "FeatureCoreApi",
	})
	require.NoError(t, err)
}

func TestQueryAutomationTestResults_ExactAttemptNoLimitBy(t *testing.T) {
	corrID := uuid.MustParse("55555555-5555-5555-5555-555555555555")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "Attempt = {attempt:UInt32}")
			assert.NotContains(t, query, "LIMIT 1 BY")
			foundAttempt := false
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok && np.Name == "attempt" {
					assert.Equal(t, uint32(3), np.Value)
					foundAttempt = true
				}
			}
			assert.True(t, foundAttempt)
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	_, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
		Attempt:       3,
	})
	require.NoError(t, err)
}

func TestQueryAutomationTestResults_NullableUUIDs(t *testing.T) {
	corrID := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	tFinish := tStart.Add(time.Minute)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					runResultsSeed(tStart, tFinish, nil, nil, nil),
				}),
			}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	result, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
	})
	require.NoError(t, err)
	require.Len(t, result.Runs, 1)
	assert.Nil(t, result.Runs[0].TestRunID)
	assert.Nil(t, result.Runs[0].CorrelationID)
	assert.Nil(t, result.Runs[0].BatchID)
}

func TestQueryAutomationTestResults_QueryError(t *testing.T) {
	corrID := uuid.MustParse("88888888-8888-8888-8888-888888888888")
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return nil, errors.New("connection refused")
		},
	}

	store := NewAutomationTestResultsStore(session)
	result, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
	})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to query automation test results")
}

func TestQueryAutomationTestResults_ScanError(t *testing.T) {
	corrID := uuid.MustParse("99999999-9999-9999-9999-999999999999")
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanErr:  errors.New("scan failure"),
			}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	result, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
	})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to scan automation test results row")
}

func TestQueryAutomationTestResults_RowsErr(t *testing.T) {
	corrID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{},
				ErrErr:   errors.New("rows iteration failure"),
			}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	result, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
	})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "error iterating automation test results rows")
}

func TestQueryAutomationTestResults_CloseError(t *testing.T) {
	corrID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	tStart := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	tFinish := tStart.Add(time.Minute)
	tr := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					runResultsSeed(tStart, tFinish, &tr, &corrID, nil),
				}),
				CloseErr: errors.New("close failure"),
			}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	_, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to close rows")
}

func TestQueryAutomationTestResults_NilQuery(t *testing.T) {
	store := NewAutomationTestResultsStore(&clickhousetest.MockSession{})
	result, err := store.QueryAutomationTestResults(context.Background(), nil)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "query must not be nil")
}

func TestQueryAutomationTestResults_NilCorrelationID(t *testing.T) {
	store := NewAutomationTestResultsStore(&clickhousetest.MockSession{})
	result, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "correlation ID is required")
}

func TestQueryAutomationTestResults_OrderByClause(t *testing.T) {
	corrID := uuid.MustParse("dddddddd-dddd-dddd-dddd-dddddddddddd")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, _ ...any) (ch.Rows, error) {
			assert.Contains(t, query, "ORDER BY EnvironmentName, StackName, Stage, Attempt DESC, StartTime DESC")
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}

	store := NewAutomationTestResultsStore(session)
	_, err := store.QueryAutomationTestResults(context.Background(), &domain.AutomationTestResultsQuery{
		CorrelationID: corrID,
	})
	require.NoError(t, err)
}
