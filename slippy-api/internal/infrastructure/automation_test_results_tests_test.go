package infrastructure

import (
	"context"
	"errors"
	"strings"
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

// testResultSeed builds one mock TestResultsCor row matching scanTestResultRow's
// column order.
func testResultSeed(
	startTime time.Time,
	testID uuid.UUID,
	corrID *uuid.UUID,
	status string,
) []any {
	return []any{
		"FeatureCoreApi", // Feature
		"TestThing",      // TestName
		"some message",   // ResultMessage
		status,           // ResultStatus
		float64(1.5),     // Duration
		"desc",           // Description
		"scenario title", // ScenarioInfoTitle
		"scenario desc",  // ScenarioInfoDescription
		[]string{"tag1"}, // ScenarioInfoTags
		"Passed",         // ScenarioExecutionStatus
		"trace",          // StackTrace
		"26.04.abc1234",  // ReleaseId
		"stk1",           // StackName
		"FeatureCoreApi", // Stage
		"prod",           // EnvironmentName
		uint8(1),         // Attempt
		startTime,        // StartTime
		"main",           // BranchName
		testID,           // TestId
		corrID,           // CorrelationId (Nullable UUID)
	}
}

// --- QueryTestsByCorrelation tests ----------------------------------------

func TestQueryTestsByCorrelation_BasicSuccess(t *testing.T) {
	corrID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	testID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "autotest_results.TestResultsCor")
			assert.Contains(t, query, "StartTime >= now() - INTERVAL 14 DAY")
			assert.Contains(t, query, "CorrelationId = toUUID({correlationId:String})")
			assert.Contains(t, query, "ResultStatus ILIKE {status:String}")
			assert.Contains(t, query, "ORDER BY StartTime ASC, TestId ASC")
			assert.Contains(t, query, "LIMIT {fetchLimit:UInt32}")
			// Confirm correlationId param + fetchLimit = limit + 1
			values := map[string]any{}
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					values[np.Name] = np.Value
				}
			}
			assert.Equal(t, corrID.String(), values["correlationId"])
			assert.Equal(t, uint32(11), values["fetchLimit"])
			assert.Equal(t, "Failed", values["status"])
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					testResultSeed(ts, testID, &corrID, "Failed"),
				}),
			}, nil
		},
	}

	store := NewAutomationTestsStore(session)
	result, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: corrID,
		Status:        "Failed",
		Limit:         10,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Count)
	require.Len(t, result.Tests, 1)
	assert.Equal(t, "TestThing", result.Tests[0].TestName)
	assert.Equal(t, testID.String(), result.Tests[0].TestID)
	require.NotNil(t, result.Tests[0].CorrelationID)
	assert.Equal(t, corrID.String(), *result.Tests[0].CorrelationID)
}

func TestQueryTestsByCorrelation_FiltersApplied(t *testing.T) {
	corrID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "EnvironmentName ILIKE {environment:String}")
			assert.Contains(t, query, "StackName ILIKE {stack:String}")
			assert.Contains(t, query, "Stage ILIKE {stage:String}")
			assert.Contains(t, query, "Attempt = {attempt:UInt8}")
			values := map[string]any{}
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					values[np.Name] = np.Value
				}
			}
			assert.Equal(t, "prod", values["environment"])
			assert.Equal(t, "stk1", values["stack"])
			assert.Equal(t, "FeatureCoreApi", values["stage"])
			assert.Equal(t, uint8(2), values["attempt"])
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: corrID,
		Environment:   "prod",
		Stack:         "stk1",
		Stage:         "FeatureCoreApi",
		Attempt:       2,
		Limit:         10,
	})
	require.NoError(t, err)
}

func TestQueryTestsByCorrelation_EmptyStatusOmitsClause(t *testing.T) {
	corrID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, _ ...any) (ch.Rows, error) {
			// ResultStatus appears in SELECT; the filter clause should be absent.
			assert.NotContains(t, query, "ResultStatus ILIKE")
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: corrID,
		Status:        "",
		Limit:         10,
	})
	require.NoError(t, err)
}

func TestQueryTestsByCorrelation_NextPageDetected(t *testing.T) {
	corrID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	ts1 := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	ts2 := ts1.Add(time.Minute)
	ts3 := ts2.Add(time.Minute)
	id1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	id2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	id3 := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, args ...any) (ch.Rows, error) {
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok && np.Name == "fetchLimit" {
					assert.Equal(t, uint32(3), np.Value)
				}
			}
			return &clickhousetest.MockRows{
				NextData: []bool{true, true, true},
				ScanFunc: scannerFor([][]any{
					testResultSeed(ts1, id1, &corrID, "Failed"),
					testResultSeed(ts2, id2, &corrID, "Failed"),
					testResultSeed(ts3, id3, &corrID, "Failed"),
				}),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	result, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: corrID,
		Limit:         2,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, result.Count)
	require.Len(t, result.Tests, 2)
	assert.Equal(t, encodeTestsCursor(ts2, id2.String()), result.NextCursor)
}

func TestQueryTestsByCorrelation_CursorAppliedToWhere(t *testing.T) {
	corrID := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	id := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	cursor := encodeTestsCursor(ts, id.String())

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "StartTime > toDateTime({cursorTime:String})")
			assert.Contains(t, query, "TestId > toUUID({cursorId:String})")
			values := map[string]any{}
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					values[np.Name] = np.Value
				}
			}
			assert.Equal(t, ts.UTC().Format("2006-01-02 15:04:05"), values["cursorTime"])
			assert.Equal(t, id.String(), values["cursorId"])
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: corrID,
		Limit:         10,
		Cursor:        cursor,
	})
	require.NoError(t, err)
}

func TestQueryTestsByCorrelation_InvalidCursor(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: uuid.MustParse("77777777-7777-7777-7777-777777777777"),
		Limit:         10,
		Cursor:        "not-a-cursor",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidTestsCursor))
}

func TestQueryTestsByCorrelation_QueryError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return nil, errors.New("connection refused")
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: uuid.MustParse("88888888-8888-8888-8888-888888888888"),
		Limit:         10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to query automation tests")
}

func TestQueryTestsByCorrelation_ScanError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanErr:  errors.New("scan failure"),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: uuid.MustParse("99999999-9999-9999-9999-999999999999"),
		Limit:         10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to scan automation tests row")
}

func TestQueryTestsByCorrelation_RowsErr(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{},
				ErrErr:   errors.New("rows iteration failure"),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
		Limit:         10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error iterating automation tests rows")
}

func TestQueryTestsByCorrelation_CloseError(t *testing.T) {
	corrID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	tID := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{testResultSeed(ts, tID, &corrID, "Failed")}),
				CloseErr: errors.New("close failure"),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: corrID,
		Limit:         10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to close rows")
}

func TestQueryTestsByCorrelation_NilQuery(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.QueryTestsByCorrelation(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query must not be nil")
}

func TestQueryTestsByCorrelation_NilCorrelationID(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "correlation ID is required")
}

func TestQueryTestsByCorrelation_ZeroLimit(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.QueryTestsByCorrelation(context.Background(), &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: uuid.MustParse("dddddddd-dddd-dddd-dddd-dddddddddddd"),
		Limit:         0,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be at least 1")
}

// --- LoadTestByCorrelation tests ------------------------------------------

func TestLoadTestByCorrelation_Success(t *testing.T) {
	corrID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	testID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "autotest_results.TestResultsCor")
			assert.Contains(t, query, "StartTime >= now() - INTERVAL 14 DAY")
			assert.Contains(t, query, "CorrelationId = toUUID({correlationId:String})")
			assert.Contains(t, query, "TestId = toUUID({testId:String})")
			assert.Contains(t, query, "LIMIT 1")
			assert.NotContains(t, query, "fetchLimit")
			assert.NotContains(t, query, "ResultStatus ILIKE")
			values := map[string]any{}
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					values[np.Name] = np.Value
				}
			}
			assert.Equal(t, corrID.String(), values["correlationId"])
			assert.Equal(t, testID.String(), values["testId"])
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					testResultSeed(ts, testID, &corrID, "Failed"),
				}),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	res, err := store.LoadTestByCorrelation(context.Background(), &domain.LoadTestByCorrelationQuery{
		CorrelationID: corrID,
		TestID:        testID,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, testID.String(), res.TestID)
	assert.Equal(t, "Failed", res.ResultStatus)
	assert.Equal(t, "trace", res.StackTrace)
}

func TestLoadTestByCorrelation_NotFound(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	res, err := store.LoadTestByCorrelation(context.Background(), &domain.LoadTestByCorrelationQuery{
		CorrelationID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		TestID:        uuid.MustParse("33333333-3333-3333-3333-333333333333"),
	})
	require.Error(t, err)
	assert.Nil(t, res)
	assert.True(t, errors.Is(err, domain.ErrTestNotFound))
}

func TestLoadTestByCorrelation_NilQuery(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.LoadTestByCorrelation(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query must not be nil")
}

func TestLoadTestByCorrelation_NilCorrelationID(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.LoadTestByCorrelation(context.Background(), &domain.LoadTestByCorrelationQuery{
		TestID: uuid.MustParse("33333333-3333-3333-3333-333333333333"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "correlation ID is required")
}

func TestLoadTestByCorrelation_NilTestID(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.LoadTestByCorrelation(context.Background(), &domain.LoadTestByCorrelationQuery{
		CorrelationID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "test ID is required")
}

func TestLoadTestByCorrelation_QueryError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return nil, errors.New("connection refused")
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.LoadTestByCorrelation(context.Background(), &domain.LoadTestByCorrelationQuery{
		CorrelationID: uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		TestID:        uuid.MustParse("55555555-5555-5555-5555-555555555555"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load automation test")
}

func TestLoadTestByCorrelation_ScanError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanErr:  errors.New("scan failure"),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.LoadTestByCorrelation(context.Background(), &domain.LoadTestByCorrelationQuery{
		CorrelationID: uuid.MustParse("66666666-6666-6666-6666-666666666666"),
		TestID:        uuid.MustParse("77777777-7777-7777-7777-777777777777"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to scan automation test row")
}

// --- cursor helpers --------------------------------------------------------

func TestParseTestsCursor_Invalid(t *testing.T) {
	cases := []struct {
		name   string
		cursor string
	}{
		{"no separator", "2026-04-01T12:00:00Z"},
		{"bad timestamp", "not-a-time|11111111-1111-1111-1111-111111111111"},
		{"bad uuid", "2026-04-01T12:00:00Z|not-a-uuid"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := parseTestsCursor(c.cursor)
			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "cursor") ||
				strings.Contains(err.Error(), "uuid") ||
				strings.Contains(err.Error(), "timestamp"))
		})
	}
}

func TestEncodeParseTestsCursor_RoundTrip(t *testing.T) {
	ts := time.Date(2026, 4, 1, 12, 30, 45, 123456789, time.UTC)
	id := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	cursor := encodeTestsCursor(ts, id.String())

	gotTS, gotID, err := parseTestsCursor(cursor)
	require.NoError(t, err)
	assert.Equal(t, ts, gotTS)
	assert.Equal(t, id, gotID)
}
