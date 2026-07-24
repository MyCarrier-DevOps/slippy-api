// Command slippy-migrator applies the slippy Postgres schema (migrations + ensurers) as a
// dedicated k8s Job. It is schema-only: the one-time ClickHouse->Postgres data backfill is
// a separate standalone script run manually at cutover (see DEVOPS-127), not a mode here.
//
// Configuration comes from the environment: SLIPPY_PIPELINE_CONFIG (the pipeline JSON that
// drives the dynamic schema) and the POSTGRES_* variables consumed by
// goLibMyCarrier/postgres.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/postgres"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

func main() {
	// SIGTERM/Interrupt (e.g. Kubernetes terminating the Job) cancels the context so
	// RunPostgresMigrations aborts the in-flight DDL and the deferred pool close runs — pgx
	// sends a cancel request and Postgres releases the migration's ACCESS EXCLUSIVE locks
	// promptly, instead of holding them until it notices the dropped connection.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Runtime bounds: postgresmigrator wraps each migration in a 5-minute context deadline
	// (its default migrationTimeout; RunPostgresMigrations does not expose an override), and
	// v1.3.98's uncapSessionTimeouts sets the pool's statement_timeout/lock_timeout to 0
	// inside each migration transaction — so that 5-minute client deadline is the effective
	// per-migration bound. The Kubernetes Job's activeDeadlineSeconds bounds the whole run.
	// A very slow DDL (e.g. an index build on a large routing_slips) could hit the 5-minute
	// limit; raise migrationTimeout upstream in goLibMyCarrier if that becomes real.
	if err := run(ctx, os.Args[1:], realDeps()); err != nil {
		log.Fatalf("slippy-migrator: %v", err)
	}
}

// realDeps wires the production dependencies used by main.
func realDeps() deps {
	return deps{
		loadPipelineConfig: slippy.LoadPipelineConfig,
		loadPGConfig:       postgres.PostgresLoadConfig,
		openPool: func(ctx context.Context, cfg *postgres.PostgresConfig) (*pgxpool.Pool, func(), error) {
			sess, err := postgres.NewPostgresSession(ctx, cfg)
			if err != nil {
				return nil, nil, err
			}
			return sess.Pool(), func() { closeAndLog("postgres session close", sess.Close, log.Printf) }, nil
		},
		migrate: slippy.RunPostgresMigrations,
		lock:    acquireMigrationLock,
		logf:    log.Printf,
	}
}
