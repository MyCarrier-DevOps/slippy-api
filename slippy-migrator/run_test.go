package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/postgres"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

func testPipelineConfig(t *testing.T) *slippy.PipelineConfig {
	t.Helper()
	cfg, err := slippy.ParsePipelineConfig([]byte(`{
		"version": "1.0", "name": "mig-test",
		"steps": [
			{"name": "push_parsed", "description": "d"},
			{"name": "builds", "description": "d", "aggregates": "component_builds", "prerequisites": ["push_parsed"]}
		]
	}`))
	require.NoError(t, err)
	return cfg
}

// okDeps returns deps whose loaders/migrate all succeed, recording calls via the pointers.
func okDeps(t *testing.T, closed *bool, captured *slippy.PostgresMigrateOptions) deps {
	t.Helper()
	return deps{
		loadPipelineConfig: func() (*slippy.PipelineConfig, error) { return testPipelineConfig(t), nil },
		loadPGConfig:       func() (*postgres.PostgresConfig, error) { return &postgres.PostgresConfig{}, nil },
		openPool: func(context.Context, *postgres.PostgresConfig) (*pgxpool.Pool, func(), error) {
			return nil, func() { *closed = true }, nil
		},
		migrate: func(_ context.Context, _ *pgxpool.Pool, opts slippy.PostgresMigrateOptions) (*slippy.MigrateResult, error) {
			*captured = opts
			return &slippy.MigrateResult{Direction: "up", StartVersion: 0, EndVersion: 4, MigrationsApplied: 4}, nil
		},
		lock: func(context.Context, *pgxpool.Pool) (func(), error) { return func() {}, nil },
		logf: func(string, ...any) {},
	}
}

func TestParseArgs(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		o, err := parseArgs(nil)
		require.NoError(t, err)
		assert.False(t, o.dryRun)
		assert.Equal(t, 0, o.targetVersion)
	})
	t.Run("flags", func(t *testing.T) {
		o, err := parseArgs([]string{"-dry-run", "-target-version", "3"})
		require.NoError(t, err)
		assert.True(t, o.dryRun)
		assert.Equal(t, 3, o.targetVersion)
	})
	t.Run("verbose flag", func(t *testing.T) {
		o, err := parseArgs([]string{"-verbose"})
		require.NoError(t, err)
		assert.True(t, o.verbose)
	})
	t.Run("negative target rejected", func(t *testing.T) {
		_, err := parseArgs([]string{"-target-version", "-1"})
		require.Error(t, err)
	})
	t.Run("unknown flag rejected", func(t *testing.T) {
		_, err := parseArgs([]string{"-nope"})
		require.Error(t, err)
		assert.NotErrorIs(t, err, flag.ErrHelp, "a bad flag is not a help request")
	})
	t.Run("help requested returns ErrHelp", func(t *testing.T) {
		_, err := parseArgs([]string{"-h"})
		require.ErrorIs(t, err, flag.ErrHelp)
	})
}

func TestRun_HelpRequestedExitsClean(t *testing.T) {
	// -h is a clean exit (0), not a failure — even before any dependency is touched.
	err := run(context.Background(), []string{"-h"}, deps{})
	require.NoError(t, err)
}

func TestRun_AppliesMigration(t *testing.T) {
	var closed bool
	var opts slippy.PostgresMigrateOptions
	err := run(context.Background(), nil, okDeps(t, &closed, &opts))
	require.NoError(t, err)
	assert.True(t, closed, "pool must be closed")
	assert.False(t, opts.DryRun)
	assert.Equal(t, 0, opts.TargetVersion)
	require.NotNil(t, opts.PipelineConfig)
}

func TestRun_DryRunAndTargetVersion(t *testing.T) {
	var closed bool
	var opts slippy.PostgresMigrateOptions
	err := run(context.Background(), []string{"-dry-run", "-target-version", "2"}, okDeps(t, &closed, &opts))
	require.NoError(t, err)
	assert.True(t, opts.DryRun)
	assert.Equal(t, 2, opts.TargetVersion)
}

func TestRun_Errors(t *testing.T) {
	boom := errors.New("boom")

	t.Run("bad args", func(t *testing.T) {
		require.Error(t, run(context.Background(), []string{"-nope"}, deps{}))
	})

	t.Run("pipeline config error", func(t *testing.T) {
		d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
		d.loadPipelineConfig = func() (*slippy.PipelineConfig, error) { return nil, boom }
		require.ErrorIs(t, run(context.Background(), nil, d), boom)
	})

	t.Run("pg config error", func(t *testing.T) {
		d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
		d.loadPGConfig = func() (*postgres.PostgresConfig, error) { return nil, boom }
		require.ErrorIs(t, run(context.Background(), nil, d), boom)
	})

	t.Run("open pool error", func(t *testing.T) {
		d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
		d.openPool = func(context.Context, *postgres.PostgresConfig) (*pgxpool.Pool, func(), error) {
			return nil, nil, boom
		}
		require.ErrorIs(t, run(context.Background(), nil, d), boom)
	})

	t.Run("migrate error closes pool", func(t *testing.T) {
		var closed bool
		d := okDeps(t, &closed, new(slippy.PostgresMigrateOptions))
		d.migrate = func(context.Context, *pgxpool.Pool, slippy.PostgresMigrateOptions) (*slippy.MigrateResult, error) {
			return nil, boom
		}
		require.ErrorIs(t, run(context.Background(), nil, d), boom)
		assert.True(t, closed, "pool must be closed even on migrate error")
	})
}

func TestCloseAndLog(t *testing.T) {
	t.Run("logs on error", func(t *testing.T) {
		var logged bool
		closeAndLog("thing", func() error { return errors.New("boom") }, func(string, ...any) { logged = true })
		assert.True(t, logged)
	})
	t.Run("silent on success", func(t *testing.T) {
		var logged bool
		closeAndLog("thing", func() error { return nil }, func(string, ...any) { logged = true })
		assert.False(t, logged)
	})
}

func TestRealDeps(t *testing.T) {
	d := realDeps()
	assert.NotNil(t, d.loadPipelineConfig)
	assert.NotNil(t, d.loadPGConfig)
	assert.NotNil(t, d.openPool)
	assert.NotNil(t, d.migrate)
	assert.NotNil(t, d.lock)
	assert.NotNil(t, d.logf)
}

func TestParseArgs_StrayPositionalRejected(t *testing.T) {
	// `slippy-migrator 3` (meant as -target-version 3) must error, not silently run defaults.
	_, err := parseArgs([]string{"3"})
	require.Error(t, err)
	assert.NotErrorIs(t, err, flag.ErrHelp)
	assert.Contains(t, err.Error(), "positional")
}

func TestRun_TargetVersionAboveLatestRejected(t *testing.T) {
	d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
	err := run(context.Background(), []string{"-target-version", "9999"}, d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the latest schema version")
}

func TestRun_DowngradeLogsWarning(t *testing.T) {
	var logs []string
	d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
	d.logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
	d.migrate = func(context.Context, *pgxpool.Pool, slippy.PostgresMigrateOptions) (*slippy.MigrateResult, error) {
		return &slippy.MigrateResult{Direction: "down", StartVersion: 2, EndVersion: 1}, nil
	}
	require.NoError(t, run(context.Background(), []string{"-target-version", "1"}, d))
	joined := strings.Join(logs, "\n")
	assert.Contains(t, joined, "WARNING: downgrade")
	assert.Contains(t, joined, "NOT reverted")
}

func TestRun_DryRunLogsEnsurerCount(t *testing.T) {
	var logs []string
	d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
	d.logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
	require.NoError(t, run(context.Background(), []string{"-dry-run"}, d))
	assert.Contains(t, strings.Join(logs, "\n"), "ensurers would also run")
}

func TestRun_DryRunDowngradeDoesNotWarn(t *testing.T) {
	var logs []string
	d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
	d.logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
	// Dry-run reports Direction "down" for a lower target, but EndVersion == StartVersion and
	// nothing is applied — the warning must NOT fire.
	d.migrate = func(context.Context, *pgxpool.Pool, slippy.PostgresMigrateOptions) (*slippy.MigrateResult, error) {
		return &slippy.MigrateResult{Direction: "down", StartVersion: 4, EndVersion: 4}, nil
	}
	require.NoError(t, run(context.Background(), []string{"-dry-run", "-target-version", "1"}, d))
	assert.NotContains(t, strings.Join(logs, "\n"), "WARNING: downgrade")
}

func TestRun_LockFailureAborts(t *testing.T) {
	migrated := false
	d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
	d.migrate = func(context.Context, *pgxpool.Pool, slippy.PostgresMigrateOptions) (*slippy.MigrateResult, error) {
		migrated = true
		return &slippy.MigrateResult{Direction: "up"}, nil
	}
	d.lock = func(context.Context, *pgxpool.Pool) (func(), error) { return nil, errors.New("lock busy") }

	err := run(context.Background(), nil, d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "acquire migration lock")
	assert.False(t, migrated, "migrate must not run when the lock isn't held")
}

func TestRun_LockReleasedAfterRun(t *testing.T) {
	unlocked := false
	d := okDeps(t, new(bool), new(slippy.PostgresMigrateOptions))
	d.lock = func(context.Context, *pgxpool.Pool) (func(), error) {
		return func() { unlocked = true }, nil
	}
	require.NoError(t, run(context.Background(), nil, d))
	assert.True(t, unlocked, "the migration lock must be released")
}

// fakeExecer records the SQL passed to Exec and can fail the first (lock) call.
type fakeExecer struct {
	calls   []string
	lockErr error
}

func (f *fakeExecer) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, sql)
	if len(f.calls) == 1 {
		return pgconn.CommandTag{}, f.lockErr
	}
	return pgconn.CommandTag{}, nil
}

func TestAdvisoryLock(t *testing.T) {
	t.Run("locks then unlocks and releases", func(t *testing.T) {
		f := &fakeExecer{}
		released := false
		unlock, err := advisoryLock(context.Background(), f, func() { released = true })
		require.NoError(t, err)
		require.NotNil(t, unlock)
		require.Len(t, f.calls, 1)
		assert.Contains(t, f.calls[0], "pg_advisory_lock")

		unlock()
		require.Len(t, f.calls, 2)
		assert.Contains(t, f.calls[1], "pg_advisory_unlock")
		assert.True(t, released, "release must run after unlock")
	})

	t.Run("lock error returns and does not release", func(t *testing.T) {
		f := &fakeExecer{lockErr: errors.New("lock busy")}
		released := false
		unlock, err := advisoryLock(context.Background(), f, func() { released = true })
		require.Error(t, err)
		assert.Nil(t, unlock)
		assert.False(t, released, "release is the caller's responsibility on lock failure")
	})
}

func TestAcquireMigrationLock_AcquireError(t *testing.T) {
	// A closed pool fails Acquire immediately (no network, deterministic), exercising the
	// connection-acquire error path of acquireMigrationLock without a real database.
	cfg, err := pgxpool.ParseConfig("postgres://localhost:5432/postgres?sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	pool.Close()

	unlock, err := acquireMigrationLock(context.Background(), pool)
	require.Error(t, err)
	assert.Nil(t, unlock)
	assert.Contains(t, err.Error(), "acquire connection")
}
