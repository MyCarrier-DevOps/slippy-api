package infrastructure

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// DEVOPS-314 3c: transient Postgres store failures must surface as ErrStoreUnavailable (503,
// retryable) instead of falling through to the StepError 422 default.
func TestTranslateStoreError_TransientIsUnavailable(t *testing.T) {
	pg := func(code string) error { return &pgconn.PgError{Code: code} }
	tests := []struct {
		name string
		err  error
	}{
		{"08000 connection exception", pg("08000")},
		{"08006 connection failure", pg("08006")},
		{"08P01 protocol violation (pooler transient)", pg("08P01")},
		{"08001 unable to establish", pg("08001")},
		{"53000 insufficient resources", pg("53000")},
		{"53300 too many connections", pg("53300")},
		{"53200 out of memory", pg("53200")},
		{"57P01 admin shutdown", pg("57P01")},
		{"57P02 crash shutdown", pg("57P02")},
		{"57P03 cannot connect now", pg("57P03")},
		{"40001 serialization failure", pg("40001")},
		{"40P01 deadlock detected", pg("40P01")},
		{"25006 read only transaction (failover)", pg("25006")},
		{"wrapped begin transaction failure", fmt.Errorf("begin transaction: %w", pg("53300"))},
		{"real pgx ConnectError (refused)", refusedConnectError(t)},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF},
		{"net.OpError", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}},
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"pgconn.ErrConnClosed", pgconn.ErrConnClosed},
		{
			"StepError wrapping 57P01",
			slippy.NewStepError("update", "id", "step", "", fmt.Errorf("failed: %w", pg("57P01"))),
		},
		{"SlipError wrapping 08006", slippy.NewSlipError("update", "id", fmt.Errorf("x: %w", pg("08006")))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translateStoreError(tc.err)
			assert.ErrorIs(t, got, domain.ErrStoreUnavailable)
			assert.ErrorIs(t, got, tc.err, "original cause must stay in the chain")
		})
	}
}

func TestTranslateStoreError_NonTransientUnchanged(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		err  error
	}{
		{"plain error", boom},
		{"23505 unique violation", &pgconn.PgError{Code: "23505"}},
		{"53100 disk full", &pgconn.PgError{Code: "53100"}},
		{"53400 configuration limit", &pgconn.PgError{Code: "53400"}},
		{"42P01 undefined table", &pgconn.PgError{Code: "42P01"}},
		{"22P02 invalid text", &pgconn.PgError{Code: "22P02"}},
		{"step error", slippy.NewStepError("update", "id", "step", "", boom)},
		{"slip not found", slippy.ErrSlipNotFound},
		{"terminal already exists", slippy.ErrTerminalAlreadyExists},
		{"os.ErrNotExist", os.ErrNotExist},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.err, translateStoreError(tc.err))
		})
	}
}

// A client cancel or write-op deadline stays on its existing 504 path even when the driver
// wraps it in a net error or a transient-looking pg error.
func TestTranslateStoreError_ContextErrorsNotReclassified(t *testing.T) {
	for _, base := range []error{context.Canceled, context.DeadlineExceeded} {
		for name, err := range map[string]error{
			"bare":        base,
			"net wrapped": &net.OpError{Op: "read", Err: base},
			"begin":       fmt.Errorf("begin transaction: %w", base),
		} {
			t.Run(base.Error()+"/"+name, func(t *testing.T) {
				assert.NotErrorIs(t, translateStoreError(err), domain.ErrStoreUnavailable)
			})
		}
	}
}

func TestTranslateStoreError_ExistingSentinelsUnchanged(t *testing.T) {
	assert.NoError(t, translateStoreError(nil))
	assert.ErrorIs(t, translateStoreError(&pgconn.PgError{Code: "55P03"}), domain.ErrWriteContended)
	assert.ErrorIs(t, translateStoreError(&pgconn.PgError{Code: "57014"}), domain.ErrStatementTimeout)
}

// CreateSlipForPush bypasses instrumentedWrite, so it must translate on its own: a transient
// store failure on create is 503 (retryable), a deterministic one is unchanged.
func TestSlipWriterAdapter_CreateSlipForPush_TransientStoreErrorIs503Sentinel(t *testing.T) {
	newStore := func(createErr error) *mockSlipStore {
		return &mockSlipStore{
			loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
				return nil, slippy.ErrSlipNotFound
			},
			createFn: func(_ context.Context, _ *slippy.Slip) error { return createErr },
		}
	}
	opts := domain.PushOptions{CorrelationID: "abc-123", Repository: "org/repo", CommitSHA: "deadbeef"}

	_, err := newTestWriterAdapter(newStore(&pgconn.PgError{Code: "53300"})).
		CreateSlipForPush(context.Background(), opts)
	assert.ErrorIs(t, err, domain.ErrStoreUnavailable)

	boom := errors.New("boom")
	_, err = newTestWriterAdapter(newStore(boom)).CreateSlipForPush(context.Background(), opts)
	assert.NotErrorIs(t, err, domain.ErrStoreUnavailable)
}

// refusedConnectError produces the *pgconn.ConnectError pgx returns when a pool cannot dial.
func refusedConnectError(t *testing.T) error {
	t.Helper()
	_, err := pgconn.Connect(context.Background(), "host=127.0.0.1 port=1 connect_timeout=2")
	var ce *pgconn.ConnectError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *pgconn.ConnectError, got %T: %v", err, err)
	}
	return err
}

// The create path's two other CreateSlipForPush arms (dedup lock acquired, and lock backend
// down -> fail-open) must translate too.
func TestSlipWriterAdapter_CreateSlipForPush_DedupArmsTranslateTransient(t *testing.T) {
	opts := domain.PushOptions{CorrelationID: "abc-123", Repository: "org/repo", CommitSHA: "deadbeef"}
	failingStore := func() *mockSlipStore {
		return &mockSlipStore{
			loadByCommitFn: func(_ context.Context, _, _ string) (*slippy.Slip, error) {
				return nil, slippy.ErrSlipNotFound
			},
			createFn: func(_ context.Context, _ *slippy.Slip) error { return &pgconn.PgError{Code: "57P01"} },
		}
	}
	t.Run("lock acquired", func(t *testing.T) {
		locker := &stubLocker{acquireFn: func(context.Context, string, time.Duration) (bool, string, error) {
			return true, "tok", nil
		}}
		_, err := newWriterAdapterWithDeps(failingStore(), locker, nil).CreateSlipForPush(context.Background(), opts)
		assert.ErrorIs(t, err, domain.ErrStoreUnavailable)
	})
	t.Run("lock backend down (fail-open)", func(t *testing.T) {
		locker := &stubLocker{acquireFn: func(context.Context, string, time.Duration) (bool, string, error) {
			return false, "", errors.New("dragonfly down")
		}}
		_, err := newWriterAdapterWithDeps(failingStore(), locker, nil).CreateSlipForPush(context.Background(), opts)
		assert.ErrorIs(t, err, domain.ErrStoreUnavailable)
	})
}
