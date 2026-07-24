package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/logger"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/postgres"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/config"
	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
	"github.com/MyCarrier-DevOps/slippy-api/internal/handler"
	"github.com/MyCarrier-DevOps/slippy-api/internal/infrastructure"
	"github.com/MyCarrier-DevOps/slippy-api/internal/middleware"
	"github.com/MyCarrier-DevOps/slippy-api/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

// buildHandler creates the fully-wired HTTP handler with auth, routes, and
// OpenTelemetry instrumentation. This is extracted from run() for testability.
// The imageTagReader, ciJobLogReader, automationTestResultsReader,
// automationTestsReader, and adminHandler are optional — if nil, their endpoints
// are not registered.
func buildHandler(
	cfg *config.Config,
	reader domain.SlipReader,
	writer domain.SlipWriter,
	imageTagReader domain.ImageTagReader,
	ciJobLogReader domain.CIJobLogReader,
	automationTestResultsReader domain.AutomationTestResultsReader,
	automationTestsReader domain.AutomationTestsReader,
	pipelineCfg *slippy.PipelineConfig,
	adminHandler *handler.AdminHandler,
) http.Handler {
	mux := http.NewServeMux()
	apiConfig := huma.DefaultConfig("Slippy API", "1.0.0")
	apiConfig.Info.Description = "API for CI/CD routing slips"

	// Define the security schemes used by protected operations.
	apiConfig.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"apiKey":      {Type: "http", Scheme: "bearer"},
		"writeApiKey": {Type: "http", Scheme: "bearer"},
	}

	api := humago.New(mux, apiConfig)

	// Register authentication middleware.
	api.UseMiddleware(middleware.NewAPIKeyAuth(cfg.APIKey, cfg.WriteAPIKey))

	// Register routes on both unversioned (legacy) and /v1 paths.
	// The empty prefix keeps existing routes unchanged for backward compatibility.
	// The "/v1" prefix registers versioned routes with "v1-" prefixed OperationIDs.
	grp := huma.NewGroup(api, "", "/v1")

	handler.RegisterHealthRoutes(grp)
	h := handler.NewSlipHandler(reader)
	handler.RegisterRoutes(grp, h)

	// Register image tag routes when a reader is available.
	if imageTagReader != nil {
		ith := handler.NewImageTagHandler(imageTagReader)
		handler.RegisterImageTagRoutes(grp, ith)
	}

	// Register CI job log routes when a reader is available.
	if ciJobLogReader != nil {
		clh := handler.NewCIJobLogHandler(ciJobLogReader)
		handler.RegisterCIJobLogRoutes(grp, clh)
	}

	// Register v1-only routes (no legacy unversioned paths) below.
	v1Only := huma.NewGroup(api, "/v1")

	// Pipeline config: v1-only.
	pch := handler.NewPipelineConfigHandler(pipelineCfg)
	handler.RegisterPipelineConfigRoutes(v1Only, pch)

	// Step prerequisites: v1-only.
	sprh := handler.NewStepPrerequisitesHandler(reader, pipelineCfg)
	handler.RegisterStepPrerequisitesRoutes(v1Only, sprh)

	// Automation test results: v1-only. The optional automationTestsReader
	// powers the per-test drill-down endpoints; when nil, only the parent
	// run-summary routes are registered.
	if automationTestResultsReader != nil {
		atrh := handler.NewAutomationTestResultsHandler(automationTestResultsReader, automationTestsReader)
		handler.RegisterAutomationTestResultsRoutes(v1Only, atrh)
	}

	// Write routes: v1-only.
	// Extract cache invalidator from reader when available (CachedSlipReader implements it).
	if writer != nil {
		var inv domain.Invalidator
		if i, ok := reader.(domain.Invalidator); ok {
			inv = i
		}
		wh := handler.NewSlipWriteHandler(writer, inv)
		handler.RegisterWriteRoutes(v1Only, wh)
	}

	// Admin routes: v1-only (no auth required — diagnostic only).
	if adminHandler != nil {
		handler.RegisterAdminRoutes(v1Only, adminHandler)
	}

	// Wrap with OpenTelemetry instrumentation.
	return otelhttp.NewHandler(mux, "slippy-api")
}

// redisDial is the default factory for creating Redis clients.
// Extracted as a variable so tests can verify the connectCache path without
// requiring a real Redis instance.
var redisDial = func(opts *redis.Options) redis.Cmdable {
	return redis.NewClient(opts)
}

// connectCache optionally wraps reader with a Dragonfly/Redis caching layer.
// If caching is not enabled in cfg, or the Redis ping fails, the original reader
// is returned unchanged and the returned client is nil. The dial function creates
// the Redis client.
//
// The returned redis.Cmdable (nil when caching is disabled or the ping failed) is
// surfaced so the write path can build a dedup Locker from the SAME connection —
// see run(). A nil client there means dedup is disabled (fail-open), exactly
// mirroring the cache graceful-degrade.
func connectCache(
	cfg *config.Config,
	reader domain.SlipReader,
	dial func(*redis.Options) redis.Cmdable,
) (domain.SlipReader, redis.Cmdable) {
	if !cfg.CacheEnabled() {
		return reader, nil
	}
	rdb := dial(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.DragonflyHost, cfg.DragonflyPort),
		Password: cfg.DragonflyPassword,
	})
	// Verify connectivity at startup.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("warning: dragonfly ping failed, caching disabled: %v", err)
		closeRedis(rdb) // don't leak the dialed client on the failure path
		return reader, nil
	}
	return infrastructure.NewCachedSlipReader(reader, rdb, cfg.CacheTTL), rdb
}

// closeRedis closes a redis.Cmdable when it is backed by an io.Closer (*redis.Client is).
// nil-safe: a nil Cmdable interface fails the type assertion and is a no-op.
func closeRedis(rdb redis.Cmdable) {
	if c, ok := rdb.(io.Closer); ok {
		if err := c.Close(); err != nil {
			log.Printf("warning: redis client close: %v", err)
		}
	}
}

// startupConnectTimeout bounds an initial datastore connect+ping at startup (Postgres and
// ClickHouse) so a stalled network surfaces as a clean readiness failure rather than a hung,
// green-but-not-ready pod.
const startupConnectTimeout = 8 * time.Second

// isEncryptingSSLMode reports whether a libpq sslmode guarantees encryption in transit.
// disable/allow/prefer do not (prefer downgrades to plaintext if the server refuses TLS);
// require/verify-ca/verify-full do. verify-full is the library default and recommended.
func isEncryptingSSLMode(mode string) bool {
	switch mode {
	case "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

// verifyPostgresSchema fails fast when the operational tables are absent. A ping succeeds
// against an un-migrated database and slippy-api no longer runs migrations (the
// slippy-migrator Job owns the schema), so this is a cheap startup readiness gate.
func verifyPostgresSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, tbl := range []string{"routing_slips", "slip_component_states", "slip_ancestry"} {
		var reg *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::text", tbl).Scan(&reg); err != nil {
			return fmt.Errorf("postgres schema check (%s): %w", tbl, err)
		}
		if reg == nil {
			return fmt.Errorf(
				"postgres schema not initialized: table %q is missing — has the slippy-migrator Job run?",
				tbl,
			)
		}
	}
	return nil
}

// run wires up all components and starts the HTTP server with graceful shutdown.
func run() error {
	// --- OpenTelemetry ---
	otelShutdown, err := telemetry.Init(context.Background())
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(ctx); err != nil {
			log.Printf("warning: otel shutdown: %v", err)
		}
	}()

	// --- Configuration ---
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log.Printf("config loaded (port=%d, cache=%v, db=%s)",
		cfg.Port, cfg.CacheEnabled(), cfg.SlipDatabase)

	// --- Library logger ---
	// Single shared logger for the slippy library, ClickHouse store, migrations,
	// and GitHub client. Without this, library-level operations (incl. migration
	// execution) run silently. Debug mode is off; flip to true to surface trace
	// detail.
	libLogger := logger.NewStdLogger(false)

	// --- Pipeline configuration ---
	// The slippy library requires a PipelineConfig for all store operations because
	// the schema is dynamic — step columns in ClickHouse are determined by the config.
	pipelineCfg, err := slippy.LoadPipelineConfig()
	if err != nil {
		return fmt.Errorf("pipeline config: %w", err)
	}
	log.Printf("pipeline config loaded (%s, %d steps)", pipelineCfg.Name, len(pipelineCfg.Steps))

	// --- Postgres slip store (command + query path) ---
	// Slips live in Postgres: writes and read-modify-write reads go directly to PG
	// (atomic under MVCC), and the query path reads PG directly too. Schema is owned
	// by the migrator Job (PreSync hook); slippy-api never migrates.
	pgCfg, err := postgres.PostgresLoadConfig()
	if err != nil {
		return fmt.Errorf("postgres config: %w", err)
	}
	// Refuse to start on a non-encrypting sslmode. The library default is verify-full, but
	// PostgresValidateConfig also accepts disable/allow/prefer; this floor stops a
	// misconfigured POSTGRES_SSLMODE from silently sending DB credentials + slip data in
	// cleartext. require is kept as the library's documented last-resort (encrypted, server
	// not authenticated).
	if !isEncryptingSSLMode(pgCfg.PgSSLMode) {
		return fmt.Errorf(
			"refusing to start: POSTGRES_SSLMODE=%q does not guarantee TLS; use verify-full (default), verify-ca, or require",
			pgCfg.PgSSLMode,
		)
	}
	// Bound the initial connect+ping so a DNS/network stall at boot surfaces as a clean
	// readiness failure instead of a hung, green-but-not-ready pod (every sibling startup
	// dependency — Redis, otel, HTTP — fails fast the same way).
	pgConnectCtx, pgConnectCancel := context.WithTimeout(context.Background(), startupConnectTimeout)
	defer pgConnectCancel()
	pgSession, err := postgres.NewPostgresSession(pgConnectCtx, pgCfg)
	if err != nil {
		return fmt.Errorf("postgres session: %w", err)
	}
	defer func() {
		if closeErr := pgSession.Close(); closeErr != nil {
			log.Printf("warning: postgres session close: %v", closeErr)
		}
	}()
	// A ping succeeds against a database that has no schema yet, and slippy-api no longer
	// migrates (the slippy-migrator Job owns the schema via a PreSync hook). Fail fast if the
	// tables aren't there rather than booting green and face-planting on the first request.
	if err := verifyPostgresSchema(pgConnectCtx, pgSession.Pool()); err != nil {
		return err
	}
	store, err := slippy.NewPostgresStore(pgSession.Pool(), pipelineCfg, libLogger)
	if err != nil {
		return fmt.Errorf("postgres slip store: %w", err)
	}
	// verifyPostgresSchema only proves the tables exist; the per-step {step}_status and
	// aggregate columns are config-derived (added by ensurers), so a config-only step change
	// can leave the DB lagging even with the tables present. Probe the full column set the
	// store actually selects via a Load of the nil-UUID sentinel: ErrSlipNotFound means the
	// schema is complete, anything else (e.g. Postgres 42703 undefined_column) means it lags
	// the config and the migrator Job needs to re-run.
	if _, probeErr := store.Load(pgConnectCtx, "00000000-0000-0000-0000-000000000000"); probeErr != nil &&
		!errors.Is(probeErr, slippy.ErrSlipNotFound) {
		return fmt.Errorf("postgres schema lags the pipeline config (re-run the slippy-migrator Job): %w", probeErr)
	}
	log.Printf("postgres slip store connected")

	// --- Standalone ClickHouse session (non-slip readers only) ---
	// ci.buildinfo / ci.repoproperties, observability.ciJob, and autotest_results.*
	// stay in ClickHouse. The slip store no longer provides a CH session, so these
	// readers get their own.
	chCfg, err := clickhouse.ClickhouseLoadConfig()
	if err != nil {
		return fmt.Errorf("clickhouse config: %w", err)
	}
	// ClickHouse now backs only the non-slip readers (buildinfo/ciJob/autotest) and the admin
	// diagnostic — the slip path is 100% Postgres. So a ClickHouse outage must NOT gate
	// startup of a healthy slip API: log a warning and run degraded (those routes are left
	// unregistered → 404) while the Postgres slip endpoints serve normally.
	chConnectCtx, chConnectCancel := context.WithTimeout(context.Background(), startupConnectTimeout)
	defer chConnectCancel()
	chSession, chErr := clickhouse.NewClickhouseSession(chCfg, chConnectCtx)
	if chErr != nil {
		log.Printf("warning: clickhouse session unavailable — non-slip readers + admin run degraded: %v", chErr)
		chSession = nil
	} else {
		defer func() {
			if closeErr := chSession.Close(); closeErr != nil {
				log.Printf("warning: clickhouse session close: %v", closeErr)
			}
		}()
		log.Printf("clickhouse session connected (non-slip readers)")
	}

	// Adapt the read+write store to our read-only interface (PG-direct query path).
	adapter := infrastructure.NewSlipStoreAdapter(store)

	// --- GitHub ancestry resolution ---
	// When a commit doesn't have a routing slip, walk backwards through commit
	// history via the GitHub GraphQL API to find an ancestor that does.
	ghCfg := slippy.GitHubConfig{
		AppID:         cfg.GitHubAppID,
		PrivateKey:    cfg.GitHubPrivateKey,
		EnterpriseURL: cfg.GitHubEnterpriseURL,
	}
	ghClient, ghErr := slippy.NewGitHubClient(ghCfg, libLogger)
	if ghErr != nil {
		return fmt.Errorf("github client: %w", ghErr)
	}
	slippyClient := slippy.NewClientWithDependencies(store, ghClient, slippy.Config{
		AncestryDepth:  cfg.AncestryDepth,
		PipelineConfig: pipelineCfg,
		Logger:         libLogger,
	})
	slipReader := infrastructure.NewSlipResolverAdapter(slippyClient, adapter)
	log.Printf("github ancestry resolution enabled (depth=%d)", cfg.AncestryDepth)

	// --- Optional Dragonfly/Redis cache ---
	// rdb is the shared cache connection (nil when caching is disabled or the
	// startup ping failed); it is reused below to build the slip-creation dedup
	// Locker so we never open a second Redis connection (TLS/options stay aligned).
	reader, rdb := connectCache(cfg, slipReader, redisDial)
	defer closeRedis(rdb) // released at shutdown (no-op when caching is disabled / ping failed)

	// --- ClickHouse-backed non-slip readers + admin (nil ⇒ routes skipped in degraded mode) ---
	// All of these query ClickHouse via the standalone session. When that session is
	// unavailable (above), they stay nil and buildHandler leaves their routes unregistered,
	// so the Postgres slip API keeps serving.
	var (
		imageTagReader              domain.ImageTagReader
		ciJobLogReader              domain.CIJobLogReader
		automationTestResultsReader domain.AutomationTestResultsReader
		automationTestsReader       domain.AutomationTestsReader
		adminH                      *handler.AdminHandler
	)
	if chSession != nil {
		imageTagReader = infrastructure.NewBuildInfoReader(
			chSession,
			reader,
		) // ci.buildinfo / ci.repoproperties
		ciJobLogReader = infrastructure.NewCIJobLogStore(chSession) // observability.ciJob
		automationTestResultsReader = infrastructure.NewAutomationTestResultsStore(
			chSession,
		) // autotest_results.RunResults
		automationTestsReader = infrastructure.NewAutomationTestsStore(
			chSession,
		) // autotest_results.TestResults
		adminH = handler.NewAdminHandler(
			chSession,
			cfg.SlipDatabase,
		) // legacy CH schema-version diagnostic
	}

	// --- Write support ---
	// SLIPPY_WRITE_API_KEY is required; config.Load() already validated it.
	//
	// Slip-creation dedup: when the shared cache connection is live, build a
	// repo:sha Locker so duplicate GitHub push webhooks cannot create two routing
	// slips ("phantom slip"). A nil Locker (cache disabled / ping failed) preserves
	// the original lock-free behavior — fail-open, CI never depends on cache uptime.
	// The cache-decorated reader powers the lock-miss poll so it observes committed
	// rows.
	var locker infrastructure.Locker
	if rdb != nil {
		locker = infrastructure.NewRedisLocker(rdb)
		log.Printf("slip-creation dedup lock enabled")
	} else {
		log.Printf("slip-creation dedup lock disabled (no cache)")
	}
	writer := infrastructure.NewSlipWriterAdapter(slippyClient, locker, reader)
	log.Printf("write endpoints enabled")

	// --- HTTP Server ---
	otelHandler := buildHandler(
		cfg, reader, writer, imageTagReader, ciJobLogReader,
		automationTestResultsReader, automationTestsReader, pipelineCfg, adminH,
	)
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           otelHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// --- Graceful shutdown ---
	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on :%d", cfg.Port)
		errCh <- srv.ListenAndServe()
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Printf("received %s, shutting down", sig)
	case err := <-errCh:
		return fmt.Errorf("server: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	log.Println("server stopped")
	return nil
}
