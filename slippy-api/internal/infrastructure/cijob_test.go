package infrastructure

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse/clickhousetest"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// scannerFor creates a ScanFunc that copies values from data rows to destinations
// using reflection, which handles time.Time and other types the default mock
// copyValue doesn't support.
func scannerFor(data [][]any) func(dest ...any) error {
	idx := 0
	return func(dest ...any) error {
		if idx >= len(data) {
			return errors.New("scan called beyond available rows")
		}
		row := data[idx]
		idx++
		for i := range dest {
			if i >= len(row) {
				break
			}
			reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(row[i]))
		}
		return nil
	}
}

func TestQueryLogs_BasicSuccess(t *testing.T) {
	ts1 := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 10, 11, 59, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "observability.ciJob")
			assert.Contains(t, query, "CorrelationId = {correlationId:String}")
			assert.Contains(t, query, "ORDER BY Timestamp DESC")
			return &clickhousetest.MockRows{
				NextData: []bool{true, true},
				ScanFunc: scannerFor([][]any{
					{
						ts1,
						"ERROR",
						"svc1",
						"comp1",
						"cluster1",
						"aws",
						"prod",
						"ns1",
						"msg1",
						"inst1",
						"deploy",
						"repo1",
						"img1",
						"main",
						uint64(111),
					},
					{
						ts2,
						"INFO",
						"svc2",
						"comp2",
						"cluster2",
						"gcp",
						"dev",
						"ns2",
						"msg2",
						"inst2",
						"build",
						"repo2",
						"img2",
						"feature",
						uint64(222),
					},
				}),
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-001",
		Limit:         10,
		Sort:          domain.SortDesc,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Count)
	assert.Empty(t, result.NextCursor)
	require.Len(t, result.Logs, 2)
	assert.Equal(t, "ERROR", result.Logs[0].Level)
	assert.Equal(t, "svc1", result.Logs[0].Service)
	assert.Equal(t, ts1, result.Logs[0].Timestamp)
	assert.Equal(t, "INFO", result.Logs[1].Level)
}

func TestQueryLogs_WithCursorDesc(t *testing.T) {
	cursorStr := "2026-03-10T12:00:00Z|999"
	ts := time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, _ ...any) (ch.Rows, error) {
			assert.Contains(t, query, "Timestamp < {cursor:DateTime64(9, 'UTC')}")
			assert.Contains(t, query, "{cursorHash:UInt64}")
			assert.Contains(t, query, "ORDER BY Timestamp DESC")
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					{ts, "WARN", "svc", "comp", "cl", "az", "stg", "ns", "msg", "inst", "job", "repo", "img", "dev", uint64(100)},
				}),
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-002",
		Limit:         10,
		Cursor:        cursorStr,
		Sort:          domain.SortDesc,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Count)
	assert.Empty(t, result.NextCursor)
}

func TestQueryLogs_WithCursorAsc(t *testing.T) {
	cursorStr := "2026-03-10T10:00:00Z|500"
	ts := time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, _ ...any) (ch.Rows, error) {
			assert.Contains(t, query, "Timestamp > {cursor:DateTime64(9, 'UTC')}")
			assert.Contains(t, query, "{cursorHash:UInt64}")
			assert.Contains(t, query, "ORDER BY Timestamp ASC")
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					{ts, "INFO", "svc", "comp", "cl", "az", "stg", "ns", "msg", "inst", "job", "repo", "img", "dev", uint64(100)},
				}),
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-003",
		Limit:         10,
		Cursor:        cursorStr,
		Sort:          domain.SortAsc,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Count)
}

func TestQueryLogs_WithFilters(t *testing.T) {
	ts := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			assert.Contains(t, query, "Level = {fLevel:String}")
			assert.Contains(t, query, "Service = {fService:String}")
			// Verify named params include the filter values
			foundLevel := false
			foundService := false
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok {
					switch np.Name {
					case "fLevel":
						assert.Equal(t, "ERROR", np.Value)
						foundLevel = true
					case "fService":
						assert.Equal(t, "my-service", np.Value)
						foundService = true
					}
				}
			}
			assert.True(t, foundLevel, "expected fLevel named param")
			assert.True(t, foundService, "expected fService named param")
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					{
						ts,
						"ERROR",
						"my-service",
						"comp",
						"cl",
						"az",
						"prod",
						"ns",
						"error occurred",
						"inst",
						"deploy",
						"repo",
						"img",
						"main",
						uint64(300),
					},
				}),
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-004",
		Limit:         10,
		Sort:          domain.SortDesc,
		Level:         "ERROR",
		Service:       "my-service",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Count)
	assert.Equal(t, "ERROR", result.Logs[0].Level)
	assert.Equal(t, "my-service", result.Logs[0].Service)
}

func TestQueryLogs_NextPageDetected(t *testing.T) {
	// Request limit=2 — store fetches 3. If 3 rows come back, there's a next page.
	ts1 := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 10, 11, 0, 0, 0, time.UTC)
	ts3 := time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			// Verify LIMIT is limit+1 = 3
			assert.Contains(t, query, "LIMIT {fetchLimit:UInt32}")
			for _, a := range args {
				if np, ok := a.(driver.NamedValue); ok && np.Name == "fetchLimit" {
					assert.Equal(t, uint32(3), np.Value)
				}
			}
			return &clickhousetest.MockRows{
				NextData: []bool{true, true, true},
				ScanFunc: scannerFor([][]any{
					{ts1, "INFO", "s", "c", "cl", "az", "p", "n", "m1", "i", "j", "r", "im", "b", uint64(10)},
					{ts2, "INFO", "s", "c", "cl", "az", "p", "n", "m2", "i", "j", "r", "im", "b", uint64(20)},
					{ts3, "INFO", "s", "c", "cl", "az", "p", "n", "m3", "i", "j", "r", "im", "b", uint64(30)},
				}),
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-005",
		Limit:         2,
		Sort:          domain.SortDesc,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Count)
	require.Len(t, result.Logs, 2)
	assert.Equal(t, encodeCursor(ts2, 20), result.NextCursor)
	// The third (extra) row should be trimmed
	assert.Equal(t, "m1", result.Logs[0].Message)
	assert.Equal(t, "m2", result.Logs[1].Message)
}

func TestQueryLogs_QueryError(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return nil, errors.New("connection refused")
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-err",
		Limit:         10,
		Sort:          domain.SortDesc,
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to query ci job logs")
}

func TestQueryLogs_EmptyResults(t *testing.T) {
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{},
				ScanData: [][]any{},
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-empty",
		Limit:         10,
		Sort:          domain.SortDesc,
	})

	require.NoError(t, err)
	assert.Equal(t, 0, result.Count)
	assert.Empty(t, result.NextCursor)
	assert.Empty(t, result.Logs)
}

func TestQueryLogs_InvalidCursor(t *testing.T) {
	session := &clickhousetest.MockSession{}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-bad-cursor",
		Limit:         10,
		Cursor:        "not-a-timestamp",
		Sort:          domain.SortDesc,
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, errors.Is(err, domain.ErrInvalidCursor))
}

func TestQueryLogs_AllFiltersApplied(t *testing.T) {
	ts := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, _ ...any) (ch.Rows, error) {
			// Verify all 13 filter columns appear in the query
			for _, col := range []string{
				"Level", "Service", "Component", "Cluster", "Cloud",
				"Environment", "Namespace", "Message",
				"CiJobInstance", "CiJobType",
				"BuildRepository", "BuildImage", "BuildBranch",
			} {
				assert.True(t, strings.Contains(query, col+" = "), "expected filter for %s", col)
			}
			return &clickhousetest.MockRows{
				NextData: []bool{true},
				ScanFunc: scannerFor([][]any{
					{
						ts,
						"ERROR",
						"svc",
						"comp",
						"cl",
						"az",
						"prod",
						"ns",
						"msg",
						"inst",
						"deploy",
						"repo",
						"img",
						"main",
						uint64(400),
					},
				}),
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID:   "corr-all-filters",
		Limit:           10,
		Sort:            domain.SortDesc,
		Level:           "ERROR",
		Service:         "svc",
		Component:       "comp",
		Cluster:         "cl",
		Cloud:           "az",
		Environment:     "prod",
		Namespace:       "ns",
		Message:         "msg",
		CIJobInstance:   "inst",
		CIJobType:       "deploy",
		BuildRepository: "repo",
		BuildImage:      "img",
		BuildBranch:     "main",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Count)
}

func TestBuildColumnFilters_OnlyNonEmpty(t *testing.T) {
	q := &domain.CIJobLogQuery{
		Level:   "ERROR",
		Service: "",
		Cloud:   "aws",
	}
	filters := buildColumnFilters(q)
	assert.Len(t, filters, 2)
	assert.Equal(t, "Level", filters[0].column)
	assert.Equal(t, "ERROR", filters[0].value)
	assert.Equal(t, "Cloud", filters[1].column)
	assert.Equal(t, "aws", filters[1].value)
}

func TestQueryLogs_TimestampTieBreaker(t *testing.T) {
	// All three rows share the same timestamp — the row hash tiebreaker ensures
	// no rows are lost during cursor pagination.
	ts := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, _ string, _ ...any) (ch.Rows, error) {
			return &clickhousetest.MockRows{
				NextData: []bool{true, true, true},
				ScanFunc: scannerFor([][]any{
					{ts, "INFO", "s", "c", "cl", "az", "p", "n", "m1", "i", "j", "r", "im", "b", uint64(300)},
					{ts, "INFO", "s", "c", "cl", "az", "p", "n", "m2", "i", "j", "r", "im", "b", uint64(200)},
					{ts, "INFO", "s", "c", "cl", "az", "p", "n", "m3", "i", "j", "r", "im", "b", uint64(100)},
				}),
			}, nil
		},
	}

	store := NewCIJobLogStore(session)
	result, err := store.QueryLogs(context.Background(), &domain.CIJobLogQuery{
		CorrelationID: "corr-tie",
		Limit:         2,
		Sort:          domain.SortDesc,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Count)
	require.Len(t, result.Logs, 2)
	// Cursor should include both timestamp and hash for the tiebreaker
	assert.Equal(t, encodeCursor(ts, 200), result.NextCursor)
	assert.Equal(t, "m1", result.Logs[0].Message)
	assert.Equal(t, "m2", result.Logs[1].Message)
}

func TestParseCursor_Valid(t *testing.T) {
	ts := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	cursor := encodeCursor(ts, 42)

	gotTS, gotHash, err := parseCursor(cursor)
	require.NoError(t, err)
	assert.Equal(t, ts, gotTS)
	assert.Equal(t, uint64(42), gotHash)
}

func TestParseCursor_Invalid(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{"no separator", "2026-03-10T12:00:00Z"},
		{"bad timestamp", "not-a-time|42"},
		{"bad hash", "2026-03-10T12:00:00Z|xyz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseCursor(tt.cursor)
			require.Error(t, err)
		})
	}
}
