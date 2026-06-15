package infrastructure

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse/clickhousetest"
)

// settingsRowsScanner returns a ScanFunc that copies the supplied (name, value)
// pairs into the destination scan args row-by-row. Used to seed
// clickhousetest.MockRows with system.settings result data.
func settingsRowsScanner(pairs [][2]string) func(dest ...any) error {
	idx := 0
	return func(dest ...any) error {
		if idx >= len(pairs) {
			return errors.New("settingsRowsScanner: scan called more times than seeded pairs")
		}
		if len(dest) < 2 {
			return errors.New("settingsRowsScanner: expected 2 scan targets (name, value)")
		}
		name, _ := dest[0].(*string)
		value, _ := dest[1].(*string)
		if name == nil || value == nil {
			return errors.New("settingsRowsScanner: scan targets must be *string")
		}
		*name = pairs[idx][0]
		*value = pairs[idx][1]
		idx++
		return nil
	}
}

func newSettingsSession(pairs [][2]string, queryErr error) *clickhousetest.MockSession {
	next := make([]bool, len(pairs)+1) // one extra to terminate
	for i := range pairs {
		next[i] = true
	}
	return &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (driver.Rows, error) {
			if queryErr != nil {
				return nil, queryErr
			}
			// Sanity: the assertion must query system.settings with both required
			// names as placeholders.
			if want := "FROM system.settings"; !contains(query, want) {
				return nil, errors.New("expected query to reference system.settings, got: " + query)
			}
			if len(args) != 2 {
				return nil, errors.New("expected two placeholder args for setting names")
			}
			return &clickhousetest.MockRows{
				NextData: next,
				ScanFunc: settingsRowsScanner(pairs),
			}, nil
		},
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestAssertAsyncInsertEnabled_BothOne_OK(t *testing.T) {
	session := newSettingsSession([][2]string{
		{"async_insert", "1"},
		{"wait_for_async_insert", "1"},
	}, nil)
	err := AssertAsyncInsertEnabled(context.Background(), session)
	require.NoError(t, err)
}

func TestAssertAsyncInsertEnabled_AsyncInsertDisabled_Fails(t *testing.T) {
	session := newSettingsSession([][2]string{
		{"async_insert", "0"},
		{"wait_for_async_insert", "1"},
	}, nil)
	err := AssertAsyncInsertEnabled(context.Background(), session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "async_insert")
	assert.Contains(t, err.Error(), `"0"`)
	assert.Contains(t, err.Error(), "ADO #82468")
}

func TestAssertAsyncInsertEnabled_WaitForAsyncInsertDisabled_Fails(t *testing.T) {
	session := newSettingsSession([][2]string{
		{"async_insert", "1"},
		{"wait_for_async_insert", "0"},
	}, nil)
	err := AssertAsyncInsertEnabled(context.Background(), session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait_for_async_insert")
}

func TestAssertAsyncInsertEnabled_MissingSetting_Fails(t *testing.T) {
	// Only one of the two required settings is present.
	session := newSettingsSession([][2]string{
		{"async_insert", "1"},
	}, nil)
	err := AssertAsyncInsertEnabled(context.Background(), session)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait_for_async_insert")
	assert.Contains(t, err.Error(), "not present")
}

func TestAssertAsyncInsertEnabled_QueryError_Wrapped(t *testing.T) {
	sentinel := errors.New("ch unreachable")
	session := newSettingsSession(nil, sentinel)
	err := AssertAsyncInsertEnabled(context.Background(), session)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

func TestAssertAsyncInsertEnabled_NilSession_Fails(t *testing.T) {
	err := AssertAsyncInsertEnabled(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil session")
}

// Compile-time guard: the helper accepts the public CH interface.
var _ func(context.Context, ch.ClickhouseSessionInterface) error = AssertAsyncInsertEnabled
