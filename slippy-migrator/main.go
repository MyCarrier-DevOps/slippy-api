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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/postgres"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

func main() {
	// A one-shot schema Job: overall runtime is bounded by the Kubernetes Job's
	// activeDeadlineSeconds (set in the Helm chart), not by a client-side context deadline.
	// A per-migration client timeout is deliberately avoided — severing an in-flight DDL or
	// lock wait mid-statement is worse than letting the Job deadline stop it.
	if err := run(context.Background(), os.Args[1:], realDeps()); err != nil {
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
		logf:    log.Printf,
	}
}
