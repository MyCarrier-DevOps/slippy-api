package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

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
	// lock serializes concurrent migrator runs; it returns a release func. Injected so tests
	// run without a real pool (realDeps wires acquireMigrationLock).
	lock func(context.Context, *pgxpool.Pool) (func(), error)
	logf func(string, ...any)
}

// migrationLockKey is a fixed application-defined key for the session-level advisory lock
// that serializes concurrent slippy-migrator runs against one database.
const migrationLockKey int64 = 0x736C6970 // "slip"

// acquireMigrationLock takes a session-level pg_advisory_lock on a dedicated pooled
// connection so only one migrator applies DDL at a time; it blocks (bounded by ctx) until
// the lock is free. The returned func releases the lock and the connection.
func acquireMigrationLock(ctx context.Context, pool *pgxpool.Pool) (func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		conn.Release()
		return nil, fmt.Errorf("pg_advisory_lock: %w", err)
	}
	return func() {
		// Returning the connection to the pool does NOT drop a session-level advisory lock,
		// so unlock explicitly first (best-effort, on an independent short context).
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", migrationLockKey); err != nil {
			log.Printf("slippy-migrator: pg_advisory_unlock: %v", err)
		}
		conn.Release()
	}, nil
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
	// Reject stray positionals: Go's flag package stops at the first non-flag token and
	// leaves the rest in Args(). For a one-shot Job whose flags pick what it does, silently
	// dropping `slippy-migrator 3` (meant as -target-version 3) would run the wrong thing.
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf(
			"unexpected positional argument(s) %v; use flags such as -target-version N", fs.Args())
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

	// Reject a target above the latest registered version. RunPostgresMigrations would
	// otherwise no-op and exit 0, so an operator would think they reached a version that
	// doesn't exist. (0 = latest, so only guard explicit positive targets.)
	if opts.targetVersion > 0 {
		if latest := slippy.GetPostgresDynamicMigrationVersion(pipelineCfg); opts.targetVersion > latest {
			return fmt.Errorf("target-version %d exceeds the latest schema version %d", opts.targetVersion, latest)
		}
	}

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

	// Serialize concurrent migrator runs (a manual re-trigger racing an ArgoCD retry, or a
	// second PreSync Job) behind a session-level advisory lock so their DDL can't interleave.
	// The DDL is idempotent, but this turns a possible interleave into a clean wait.
	unlock, err := d.lock(ctx, pool)
	if err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer unlock()

	if opts.dryRun {
		// DryRun reports pending *versioned* migrations only. Per-step columns on
		// routing_slips are added by unversioned ensurers, which run on a real apply but not
		// in dry-run — so surface their count here, or the most common change (a new pipeline
		// step needing a new column) would show up as "no change".
		ensurers := slippy.GetPostgresDynamicEnsurers(pipelineCfg, slippy.NewStdLogger(opts.verbose))
		d.logf("[dry-run] reporting pending migration; no changes will be applied "+
			"(%d idempotent ensurers would also run on a real apply)", len(ensurers))
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

	// A down-migration reverts versioned DownSQL but NOT the unversioned ensurer columns on
	// routing_slips (ensurers only run on the up path), so the DB can retain columns from a
	// newer version than it now reports. Surface that rather than leaving it silent — but not
	// on a dry run, where Direction is "down" for a lower target yet EndVersion == StartVersion
	// and nothing was applied (a false "downgrade applied" line would trigger needless alarm).
	if res.Direction == "down" && !opts.dryRun {
		d.logf("WARNING: downgrade v%d -> v%d applied; ensurer-added columns on routing_slips are "+
			"NOT reverted by a down-migration, so the schema may retain columns from newer versions",
			res.StartVersion, res.EndVersion)
	}
	return nil
}
