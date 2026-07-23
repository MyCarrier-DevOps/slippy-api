package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/postgres"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// options are the parsed command-line options.
type options struct {
	dryRun        bool
	targetVersion int
	verbose       bool
}

// deps are the injectable dependencies of run, so the orchestration is unit-testable
// without a database. main wires the real implementations via realDeps.
type deps struct {
	loadPipelineConfig func() (*slippy.PipelineConfig, error)
	loadPGConfig       func() (*postgres.PostgresConfig, error)
	openPool           func(context.Context, *postgres.PostgresConfig) (*pgxpool.Pool, func(), error)
	migrate            func(context.Context, *pgxpool.Pool, slippy.PostgresMigrateOptions) (*slippy.MigrateResult, error)
	logf               func(string, ...any)
}

// closeAndLog runs a close function and logs a non-nil error under name (best-effort).
func closeAndLog(name string, closeFn func() error, logf func(string, ...any)) {
	if err := closeFn(); err != nil {
		logf("slippy-migrator: %s: %v", name, err)
	}
}

// parseArgs parses the migrator's flags.
func parseArgs(args []string) (options, error) {
	fs := flag.NewFlagSet("slippy-migrator", flag.ContinueOnError)
	// Own all flag output. By default flag writes the error AND usage to stderr, which the
	// caller (main -> log.Fatalf) would then re-print — a double report. Suppress it and
	// surface a single wrapped error instead.
	fs.SetOutput(io.Discard)
	var o options
	fs.BoolVar(&o.dryRun, "dry-run", false, "report the pending migration without applying it")
	fs.IntVar(&o.targetVersion, "target-version", 0, "schema version to migrate to (0 = latest)")
	fs.BoolVar(&o.verbose, "verbose", false, "enable slippy debug/trace logging")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			// -h/-help is a help request, not an error: print usage and let the caller exit 0.
			fs.SetOutput(os.Stdout)
			fs.Usage()
			return options{}, flag.ErrHelp
		}
		return options{}, fmt.Errorf("invalid arguments: %w", err)
	}
	if o.targetVersion < 0 {
		return options{}, fmt.Errorf("target-version must be >= 0, got %d", o.targetVersion)
	}
	return o, nil
}

// run executes the schema migration: load the pipeline config, connect to Postgres, and
// apply slippy's Postgres migrations + ensurers via RunPostgresMigrations. It is the whole
// job of the migrate-schema k8s Job; the one-time data backfill is a separate standalone
// script (see DEVOPS-127), not a mode here.
func run(ctx context.Context, args []string, d deps) error {
	opts, err := parseArgs(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // help requested; usage already printed, exit 0
		}
		return err
	}

	pipelineCfg, err := d.loadPipelineConfig()
	if err != nil {
		return fmt.Errorf("pipeline config: %w", err)
	}
	d.logf("pipeline config loaded (%s, %d steps)", pipelineCfg.Name, len(pipelineCfg.Steps))

	pgCfg, err := d.loadPGConfig()
	if err != nil {
		return fmt.Errorf("postgres config: %w", err)
	}

	pool, closePool, err := d.openPool(ctx, pgCfg)
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}
	defer closePool()
	d.logf("postgres connected")

	if opts.dryRun {
		d.logf("[dry-run] reporting pending migration; no changes will be applied")
	}

	res, err := d.migrate(ctx, pool, slippy.PostgresMigrateOptions{
		PipelineConfig: pipelineCfg,
		TargetVersion:  opts.targetVersion,
		DryRun:         opts.dryRun,
		Logger:         slippy.NewStdLogger(opts.verbose),
	})
	if err != nil {
		return fmt.Errorf("migrate-schema: %w", err)
	}

	d.logf("migrate-schema complete: direction=%s from=v%d to=v%d applied=%d",
		res.Direction, res.StartVersion, res.EndVersion, res.MigrationsApplied)
	return nil
}
