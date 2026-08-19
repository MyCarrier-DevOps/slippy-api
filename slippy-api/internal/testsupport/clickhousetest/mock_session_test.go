package clickhousetest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

// stubBatchColumn is a non-nil driver.BatchColumn for asserting that
// MockBatch.Column returns the stored value (never called, only compared).
type stubBatchColumn struct{ driver.BatchColumn }

func TestMockSession_DefaultsAndRecording(t *testing.T) {
	ctx := context.Background()
	rows := &MockRows{}
	row := &MockRow{}
	conn := &MockConn{}
	m := &MockSession{
		ConnectErr:        errBoom,
		PingErr:           errBoom,
		QueryRows:         rows,
		QueryErr:          errBoom,
		QueryWithArgsRows: rows,
		QueryWithArgsErr:  errBoom,
		QueryRowRow:       row,
		ExecErr:           errBoom,
		ExecWithArgsErr:   errBoom,
		CloseErr:          errBoom,
		ConnConn:          conn,
	}

	assert.ErrorIs(t, m.Connect(nil, ctx), errBoom)
	assert.ErrorIs(t, m.Ping(ctx), errBoom)

	gotRows, err := m.Query(ctx, "SELECT 1")
	assert.Same(t, rows, gotRows)
	assert.ErrorIs(t, err, errBoom)

	gotRows, err = m.QueryWithArgs(ctx, "SELECT ?", 1)
	assert.Same(t, rows, gotRows)
	assert.ErrorIs(t, err, errBoom)

	assert.Same(t, row, m.QueryRow(ctx, "SELECT ?", 1))
	assert.ErrorIs(t, m.Exec(ctx, "TRUNCATE t"), errBoom)
	assert.ErrorIs(t, m.ExecWithArgs(ctx, "DELETE ?", 1), errBoom)
	assert.ErrorIs(t, m.Close(), errBoom)
	assert.Same(t, conn, m.Conn())

	// every call above was recorded
	assert.Len(t, m.ConnectCalls, 1)
	assert.Equal(t, 1, m.PingCalls)
	require.Len(t, m.QueryCalls, 1)
	assert.Equal(t, "SELECT 1", m.QueryCalls[0].Query)
	require.Len(t, m.QueryWithArgsCalls, 1)
	assert.Equal(t, []any{1}, m.QueryWithArgsCalls[0].Args)
	require.Len(t, m.QueryRowCalls, 1)
	assert.Equal(t, "SELECT ?", m.QueryRowCalls[0].Query)
	require.Len(t, m.ExecCalls, 1)
	assert.Equal(t, "TRUNCATE t", m.ExecCalls[0].Stmt)
	require.Len(t, m.ExecWithArgsCalls, 1)
	assert.Equal(t, []any{1}, m.ExecWithArgsCalls[0].Args)
	assert.Equal(t, 1, m.CloseCalls)
	assert.Equal(t, 1, m.ConnCalls)

	m.Reset()
	assert.Empty(t, m.ConnectCalls)
	assert.Zero(t, m.PingCalls)
	assert.Empty(t, m.QueryCalls)
	assert.Empty(t, m.QueryWithArgsCalls)
	assert.Empty(t, m.QueryRowCalls)
	assert.Empty(t, m.ExecCalls)
	assert.Empty(t, m.ExecWithArgsCalls)
	assert.Zero(t, m.CloseCalls)
	assert.Zero(t, m.ConnCalls)
}

func TestMockSession_FuncOverridesWin(t *testing.T) {
	ctx := context.Background()
	rows := &MockRows{}
	row := &MockRow{}
	conn := &MockConn{}
	m := &MockSession{
		ConnectFunc: func(context.Context) error { return errBoom },
		PingFunc:    func(context.Context) error { return errBoom },
		QueryFunc: func(context.Context, string) (driver.Rows, error) {
			return rows, errBoom
		},
		QueryWithArgsFunc: func(context.Context, string, ...any) (driver.Rows, error) {
			return rows, errBoom
		},
		QueryRowFunc: func(context.Context, string, ...any) driver.Row { return row },
		ExecFunc:     func(context.Context, string) error { return errBoom },
		ExecWithArgsFunc: func(context.Context, string, ...any) error {
			return errBoom
		},
		CloseFunc: func() error { return errBoom },
		ConnFunc:  func() driver.Conn { return conn },
	}

	assert.ErrorIs(t, m.Connect(nil, ctx), errBoom)
	assert.ErrorIs(t, m.Ping(ctx), errBoom)
	gotRows, err := m.Query(ctx, "q")
	assert.Same(t, rows, gotRows)
	assert.ErrorIs(t, err, errBoom)
	gotRows, err = m.QueryWithArgs(ctx, "q", 1)
	assert.Same(t, rows, gotRows)
	assert.ErrorIs(t, err, errBoom)
	assert.Same(t, row, m.QueryRow(ctx, "q"))
	assert.ErrorIs(t, m.Exec(ctx, "s"), errBoom)
	assert.ErrorIs(t, m.ExecWithArgs(ctx, "s", 1), errBoom)
	assert.ErrorIs(t, m.Close(), errBoom)
	assert.Same(t, conn, m.Conn())
}

func TestMockRows(t *testing.T) {
	t.Run("defaults and data-driven iteration", func(t *testing.T) {
		m := &MockRows{
			ColumnsData: []string{"a"},
			NextData:    []bool{true, false},
			ScanData:    [][]any{{"v1"}},
			CloseErr:    errBoom,
			ErrErr:      errBoom,
			TotalsErr:   errBoom,
		}
		assert.Equal(t, []string{"a"}, m.Columns())
		assert.Nil(t, m.ColumnTypes())
		assert.True(t, m.HasData())

		assert.True(t, m.Next())
		var got string
		require.NoError(t, m.Scan(&got))
		assert.Equal(t, "v1", got)
		assert.False(t, m.Next())
		assert.False(t, m.Next()) // exhausted NextData
		assert.False(t, m.HasData())

		assert.ErrorIs(t, m.Close(), errBoom)
		assert.ErrorIs(t, m.Err(), errBoom)
		assert.ErrorIs(t, m.Totals(), errBoom)
		assert.NoError(t, m.ScanStruct(nil))

		m.Reset()
		assert.True(t, m.Next()) // iterator rewound
	})

	t.Run("scan error and extra scan past data", func(t *testing.T) {
		m := &MockRows{ScanErr: errBoom}
		assert.ErrorIs(t, m.Scan(new(string)), errBoom)

		empty := &MockRows{}
		assert.NoError(t, empty.Scan(new(string))) // no data: no-op
		assert.False(t, empty.HasData())
	})

	t.Run("scan copies min(dest, data) with mismatched lengths", func(t *testing.T) {
		var only string
		wide := &MockRows{ScanData: [][]any{{"a", "b"}}}
		require.NoError(t, wide.Scan(&only)) // more data than dest: no overrun
		assert.Equal(t, "a", only)

		var first, second string
		narrow := &MockRows{ScanData: [][]any{{"a"}}}
		require.NoError(t, narrow.Scan(&first, &second)) // more dest than data
		assert.Equal(t, "a", first)
		assert.Empty(t, second) // untouched
	})

	t.Run("func overrides win", func(t *testing.T) {
		m := &MockRows{
			NextFunc:   func() bool { return true },
			ScanFunc:   func(...any) error { return errBoom },
			TotalsFunc: func(...any) error { return errBoom },
		}
		assert.True(t, m.Next())
		assert.True(t, m.HasData()) // NextFunc set means always has data
		assert.ErrorIs(t, m.Scan(), errBoom)
		assert.ErrorIs(t, m.Totals(), errBoom)
	})
}

func TestMockRow(t *testing.T) {
	t.Run("scan copies data", func(t *testing.T) {
		m := &MockRow{ScanData: []any{"v", int64(7)}}
		var s string
		var n int64
		require.NoError(t, m.Scan(&s, &n))
		assert.Equal(t, "v", s)
		assert.Equal(t, int64(7), n)
		assert.NoError(t, m.Err())
		assert.NoError(t, m.ScanStruct(nil))
	})

	t.Run("scan copies min(dest, data) with mismatched lengths", func(t *testing.T) {
		var only string
		wide := &MockRow{ScanData: []any{"a", "b"}}
		require.NoError(t, wide.Scan(&only)) // more data than dest: no overrun
		assert.Equal(t, "a", only)

		var first, second string
		narrow := &MockRow{ScanData: []any{"a"}}
		require.NoError(t, narrow.Scan(&first, &second)) // more dest than data
		assert.Equal(t, "a", first)
		assert.Empty(t, second) // untouched
	})

	t.Run("errors and overrides", func(t *testing.T) {
		m := &MockRow{ScanErr: errBoom, ScanStructErr: errBoom}
		assert.ErrorIs(t, m.Scan(new(string)), errBoom)
		assert.ErrorIs(t, m.Err(), errBoom)
		assert.ErrorIs(t, m.ScanStruct(nil), errBoom)

		o := &MockRow{
			ScanFunc:       func(...any) error { return errBoom },
			ScanStructFunc: func(any) error { return errBoom },
		}
		assert.ErrorIs(t, o.Scan(), errBoom)
		assert.ErrorIs(t, o.ScanStruct(nil), errBoom)
	})
}

func TestMockConn_Defaults(t *testing.T) {
	ctx := context.Background()
	rows := &MockRows{}
	row := &MockRow{}
	version := &driver.ServerVersion{}
	body := io.NopCloser(strings.NewReader("x"))
	m := &MockConn{
		QueryRows:         rows,
		QueryErr:          errBoom,
		QueryRowRow:       row,
		ExecErr:           errBoom,
		PrepareBatchErr:   errBoom,
		AsyncInsertErr:    errBoom,
		InsertFormatErr:   errBoom,
		QueryFormatReader: body,
		QueryFormatErr:    errBoom,
		PingErr:           errBoom,
		CloseErr:          errBoom,
		ContributorsData:  []string{"c"},
		ServerVersionData: version,
		ServerVersionErr:  errBoom,
		SelectErr:         errBoom,
	}

	gotRows, err := m.Query(ctx, "q")
	assert.Same(t, rows, gotRows)
	assert.ErrorIs(t, err, errBoom)
	assert.Same(t, row, m.QueryRow(ctx, "q"))
	assert.ErrorIs(t, m.Exec(ctx, "q"), errBoom)

	gotBatch, err := m.PrepareBatch(ctx, "q")
	assert.Nil(t, gotBatch) // MockBatch can't satisfy driver.Batch (see package note)
	assert.ErrorIs(t, err, errBoom)

	assert.ErrorIs(t, m.AsyncInsert(ctx, "q", true), errBoom)
	assert.ErrorIs(t, m.InsertFormat(ctx, "CSV", "q", strings.NewReader("d")), errBoom)

	gotReader, err := m.QueryFormat(ctx, "CSV", "q")
	assert.Equal(t, body, gotReader)
	assert.ErrorIs(t, err, errBoom)

	assert.ErrorIs(t, m.Ping(ctx), errBoom)
	assert.Equal(t, driver.Stats{}, m.Stats())
	assert.ErrorIs(t, m.Close(), errBoom)
	assert.Equal(t, []string{"c"}, m.Contributors())

	gotVersion, err := m.ServerVersion()
	assert.Same(t, version, gotVersion)
	assert.ErrorIs(t, err, errBoom)

	assert.ErrorIs(t, m.Select(ctx, nil, "q"), errBoom)
}

func TestMockConn_FuncOverridesWin(t *testing.T) {
	ctx := context.Background()
	m := &MockConn{
		QueryFunc: func(context.Context, string, ...any) (driver.Rows, error) {
			return nil, errBoom
		},
		QueryRowFunc: func(context.Context, string, ...any) driver.Row {
			return &MockRow{ScanErr: errBoom}
		},
		ExecFunc: func(context.Context, string, ...any) error { return errBoom },
		PrepareBatchFunc: func(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
			return nil, errBoom
		},
		AsyncInsertFunc: func(context.Context, string, bool, ...any) error { return errBoom },
		InsertFormatFunc: func(context.Context, string, string, io.Reader) error {
			return errBoom
		},
		QueryFormatFunc: func(context.Context, string, string, ...any) (io.ReadCloser, error) {
			return nil, errBoom
		},
		PingFunc:         func(context.Context) error { return errBoom },
		ContributorsFunc: func() []string { return []string{"f"} },
		ServerVersionFunc: func() (*driver.ServerVersion, error) {
			return nil, errBoom
		},
		SelectFunc: func(context.Context, any, string, ...any) error { return errBoom },
	}

	_, err := m.Query(ctx, "q")
	assert.ErrorIs(t, err, errBoom)
	assert.ErrorIs(t, m.QueryRow(ctx, "q").Err(), errBoom)
	assert.ErrorIs(t, m.Exec(ctx, "q"), errBoom)
	_, err = m.PrepareBatch(ctx, "q")
	assert.ErrorIs(t, err, errBoom)
	assert.ErrorIs(t, m.AsyncInsert(ctx, "q", false), errBoom)
	assert.ErrorIs(t, m.InsertFormat(ctx, "CSV", "q", nil), errBoom)
	_, err = m.QueryFormat(ctx, "CSV", "q")
	assert.ErrorIs(t, err, errBoom)
	assert.ErrorIs(t, m.Ping(ctx), errBoom)
	assert.Equal(t, []string{"f"}, m.Contributors())
	_, err = m.ServerVersion()
	assert.ErrorIs(t, err, errBoom)
	assert.ErrorIs(t, m.Select(ctx, nil, "q"), errBoom)
}

func TestMockBatch(t *testing.T) {
	t.Run("defaults record calls", func(t *testing.T) {
		m := &MockBatch{
			AppendErr:       errBoom,
			AppendStructErr: errBoom,
			FlushErr:        errBoom,
			SendErr:         errBoom,
			AbortErr:        errBoom,
			IsSentData:      true,
			RowsData:        3,
		}
		assert.ErrorIs(t, m.Append("v"), errBoom)
		assert.Equal(t, [][]any{{"v"}}, m.AppendCalls)
		assert.ErrorIs(t, m.AppendStruct("s"), errBoom)
		assert.Equal(t, []any{"s"}, m.AppendStructCalls)
		assert.Nil(t, m.Column(0)) // no ColumnFunc/ColumnData
		assert.ErrorIs(t, m.Flush(), errBoom)
		assert.Equal(t, 1, m.FlushCalls)
		assert.ErrorIs(t, m.Send(), errBoom)
		assert.Equal(t, 1, m.SendCalls)
		assert.ErrorIs(t, m.Abort(), errBoom)
		assert.Equal(t, 1, m.AbortCalls)
		assert.True(t, m.IsSent())
		assert.Equal(t, 3, m.Rows())
		assert.NoError(t, m.Close())
		assert.Nil(t, m.Columns())
	})

	t.Run("func overrides and column data", func(t *testing.T) {
		m := &MockBatch{
			AppendFunc:       func(...any) error { return errBoom },
			AppendStructFunc: func(any) error { return errBoom },
			ColumnFunc:       func(int) driver.BatchColumn { return nil },
			FlushFunc:        func() error { return errBoom },
			SendFunc:         func() error { return errBoom },
			AbortFunc:        func() error { return errBoom },
		}
		assert.ErrorIs(t, m.Append(), errBoom)
		assert.ErrorIs(t, m.AppendStruct(nil), errBoom)
		assert.Nil(t, m.Column(0))
		assert.ErrorIs(t, m.Flush(), errBoom)
		assert.ErrorIs(t, m.Send(), errBoom)
		assert.ErrorIs(t, m.Abort(), errBoom)

		col := stubBatchColumn{}
		withData := &MockBatch{ColumnData: map[int]driver.BatchColumn{1: col}}
		assert.Equal(t, col, withData.Column(1))
		assert.Nil(t, withData.Column(0))
	})
}

func TestCopyValue_AllTypes(t *testing.T) {
	// Exercised through MockRow.Scan, the public entry point.
	tests := []struct {
		name string
		src  any
		dest any
		want any
	}{
		{"int", int(1), new(int), int(1)},
		{"int8", int8(2), new(int8), int8(2)},
		{"int16", int16(3), new(int16), int16(3)},
		{"int32", int32(4), new(int32), int32(4)},
		{"int64", int64(5), new(int64), int64(5)},
		{"uint", uint(6), new(uint), uint(6)},
		{"uint8", uint8(7), new(uint8), uint8(7)},
		{"uint16", uint16(8), new(uint16), uint16(8)},
		{"uint32", uint32(9), new(uint32), uint32(9)},
		{"uint64", uint64(10), new(uint64), uint64(10)},
		{"string", "s", new(string), "s"},
		{"float32", float32(1.5), new(float32), float32(1.5)},
		{"float64", float64(2.5), new(float64), float64(2.5)},
		{"bool", true, new(bool), true},
		{"bytes", []byte("b"), new([]byte), []byte("b")},
		{"any", "anything", new(any), "anything"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MockRow{ScanData: []any{tt.src}}
			require.NoError(t, m.Scan(tt.dest))
			got := derefAny(t, tt.dest)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("nil src and mismatched types are no-ops", func(t *testing.T) {
		var s string
		require.NoError(t, (&MockRow{ScanData: []any{nil}}).Scan(&s))
		assert.Empty(t, s)

		var n int
		require.NoError(t, (&MockRow{ScanData: []any{"not an int"}}).Scan(&n))
		assert.Zero(t, n)

		var f float32
		require.NoError(t, (&MockRow{ScanData: []any{float64(1)}}).Scan(&f))
		assert.Zero(t, f)

		var u uint
		require.NoError(t, (&MockRow{ScanData: []any{int(1)}}).Scan(&u))
		assert.Zero(t, u)
	})
}

func derefAny(t *testing.T, p any) any {
	t.Helper()
	switch d := p.(type) {
	case *int:
		return *d
	case *int8:
		return *d
	case *int16:
		return *d
	case *int32:
		return *d
	case *int64:
		return *d
	case *uint:
		return *d
	case *uint8:
		return *d
	case *uint16:
		return *d
	case *uint32:
		return *d
	case *uint64:
		return *d
	case *string:
		return *d
	case *float32:
		return *d
	case *float64:
		return *d
	case *bool:
		return *d
	case *[]byte:
		return *d
	case *any:
		return *d
	default:
		t.Fatalf("unhandled dest type %T", p)
		return nil
	}
}
