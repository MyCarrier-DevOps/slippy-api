package main

import (
	"context"
	"errors"
	"flag"
	"testing"

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
	assert.NotNil(t, d.logf)
}
