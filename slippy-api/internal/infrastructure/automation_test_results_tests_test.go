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

// testResultSeed is one row of mock data shaped to match the column order in
// AutomationTestsStore.QueryTests.
func testResultSeed(startTime time.Time, testID uuid.UUID, status string) []any {
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
	}
}

func defaultRunKey() domain.ResolvedRunKey {
	return domain.ResolvedRunKey{
		ReleaseID:       "26.04.abc1234",
		Attempt:         1,
		Stage:           "FeatureCoreApi",
		EnvironmentName: "prod",
		StackName:       "stk1",
	}
}

func TestQueryTests_BasicSuccess(t *testing.T) {
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	testID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "autotest_results.TestResults")
			assert.Contains(t, query, "StartTime >= {minStart:DateTime}")
			assert.Contains(t, query, "StartTime <= {maxFinish:DateTime}")
			assert.Contains(t, query, "ReleaseId = {r0:String}")
			assert.Contains(t, query, "Attempt = {a0:UInt8}")
			assert.Contains(t, query, "Stage ILIKE {s0:String}")
			assert.Contains(t, query, "EnvironmentName ILIKE {e0:String}")
			assert.Contains(t, query, "StackName ILIKE {k0:String}")
			assert.Contains(t, query, "ResultStatus ILIKE {status:String}")
			assert.Contains(t, query, "ORDER BY StartTime ASC, TestId ASC")
			assert.Contains(t, query, "LIMIT {fetchLimit:UInt32}")
			// fetchLimit should be limit + 1
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok && np.Name == "fetchLimit" {
					assert.Equal(t, uint32(11), np.Value)
				}
			}
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					testResultSeed(ts, testID, "Failed"),
				}),
			}, nil
		},
	}

	store := NewAutomationTestsStore(session)
	result, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:      []domain.ResolvedRunKey{defaultRunKey()},
		MinStart:  ts.Add(-time.Hour),
		MaxFinish: ts.Add(time.Hour),
		Status:    "Failed",
		Limit:     10,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Count)
	assert.Empty(t, result.NextCursor)
	require.Len(t, result.Tests, 1)
	assert.Equal(t, "TestThing", result.Tests[0].TestName)
	assert.Equal(t, "Failed", result.Tests[0].ResultStatus)
	assert.Equal(t, testID.String(), result.Tests[0].TestID)
}

func TestQueryTests_MultipleRunsOrTuples(t *testing.T) {
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	testID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			// Both run keys should appear with indexed param names
			assert.Contains(t, query, "{r0:String}")
			assert.Contains(t, query, "{r1:String}")
			assert.Contains(t, query, "{a0:UInt8}")
			assert.Contains(t, query, "{a1:UInt8}")
			// They should be OR'd together inside the parenthesised group
			assert.Contains(t, query, " OR ")
			values := map[string]any{}
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					values[np.Name] = np.Value
				}
			}
			assert.Equal(t, "26.04.abc1234", values["r0"])
			assert.Equal(t, "26.04.def5678", values["r1"])
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					testResultSeed(ts, testID, "Passed"),
				}),
			}, nil
		},
	}

	store := NewAutomationTestsStore(session)
	_, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs: []domain.ResolvedRunKey{
			defaultRunKey(),
			{
				ReleaseID:       "26.04.def5678",
				Attempt:         2,
				Stage:           "FeatureUI",
				EnvironmentName: "prod",
				StackName:       "stk1",
			},
		},
		MinStart:  ts.Add(-time.Hour),
		MaxFinish: ts.Add(time.Hour),
		Limit:     10,
	})
	require.NoError(t, err)
}

func TestQueryTests_EmptyStatusOmitsClause(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, _ ...any) (ch.Rows, error) {
			// ResultStatus appears in SELECT; the filter clause should be absent.
			assert.NotContains(t, query, "ResultStatus ILIKE")
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}

	store := NewAutomationTestsStore(session)
	_, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:   []domain.ResolvedRunKey{defaultRunKey()},
		Status: "",
		Limit:  10,
	})
	require.NoError(t, err)
}

func TestQueryTests_EmptyRunsShortCircuits(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			t.Fatal("session should not be called when Runs is empty")
			return nil, nil
		},
	}

	store := NewAutomationTestsStore(session)
	result, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:  nil,
		Limit: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result.Count)
	assert.Empty(t, result.Tests)
}

func TestQueryTests_NextPageDetected(t *testing.T) {
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
					testResultSeed(ts1, id1, "Failed"),
					testResultSeed(ts2, id2, "Failed"),
					testResultSeed(ts3, id3, "Failed"),
				}),
			}, nil
		},
	}

	store := NewAutomationTestsStore(session)
	result, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:  []domain.ResolvedRunKey{defaultRunKey()},
		Limit: 2,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Count)
	require.Len(t, result.Tests, 2)
	// next_cursor should be the (StartTime, TestId) of the last returned row,
	// not the trimmed third row.
	assert.Equal(t, encodeTestsCursor(ts2, id2.String()), result.NextCursor)
}

func TestQueryTests_CursorAppliedToWhere(t *testing.T) {
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	id := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	cursor := encodeTestsCursor(ts, id.String())

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "StartTime > {cursorTime:DateTime}")
			assert.Contains(t, query, "TestId > {cursorId:UUID}")
			values := map[string]any{}
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					values[np.Name] = np.Value
				}
			}
			assert.Equal(t, ts, values["cursorTime"])
			assert.Equal(t, id, values["cursorId"])
			return &clickhousetest.MockRows{NextData: []bool{}}, nil
		},
	}

	store := NewAutomationTestsStore(session)
	_, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:   []domain.ResolvedRunKey{defaultRunKey()},
		Limit:  10,
		Cursor: cursor,
	})
	require.NoError(t, err)
}

func TestQueryTests_InvalidCursor(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	result, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:   []domain.ResolvedRunKey{defaultRunKey()},
		Limit:  10,
		Cursor: "not-a-cursor",
	})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, errors.Is(err, domain.ErrInvalidTestsCursor))
}

func TestQueryTests_QueryError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return nil, errors.New("connection refused")
		},
	}
	store := NewAutomationTestsStore(session)
	result, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:  []domain.ResolvedRunKey{defaultRunKey()},
		Limit: 10,
	})
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to query automation tests")
}

func TestQueryTests_ScanError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanErr:  errors.New("scan failure"),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:  []domain.ResolvedRunKey{defaultRunKey()},
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to scan automation tests row")
}

func TestQueryTests_RowsErr(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{},
				ErrErr:   errors.New("rows iteration failure"),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:  []domain.ResolvedRunKey{defaultRunKey()},
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error iterating automation tests rows")
}

func TestQueryTests_CloseError(t *testing.T) {
	ts := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	id := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{testResultSeed(ts, id, "Failed")}),
				CloseErr: errors.New("close failure"),
			}, nil
		},
	}
	store := NewAutomationTestsStore(session)
	_, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:  []domain.ResolvedRunKey{defaultRunKey()},
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to close rows")
}

func TestQueryTests_NilQuery(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.QueryTests(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query must not be nil")
}

func TestQueryTests_ZeroLimit(t *testing.T) {
	store := NewAutomationTestsStore(&clickhousetest.MockSession{})
	_, err := store.QueryTests(context.Background(), &domain.AutomationTestsQuery{
		Runs:  []domain.ResolvedRunKey{defaultRunKey()},
		Limit: 0,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be at least 1")
}

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
			// Make sure error message references what failed for debugability
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
