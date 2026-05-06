package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/logger"
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
// The imageTagReader, ciJobLogReader, automationTestResultsReader, and
// automationTestsReader are optional — if nil, their endpoints are not registered.
func buildHandler(
	cfg *config.Config,
	reader domain.SlipReader,
	writer domain.SlipWriter,
	imageTagReader domain.ImageTagReader,
	ciJobLogReader domain.CIJobLogReader,
	automationTestResultsReader domain.AutomationTestResultsReader,
	automationTestsReader domain.AutomationTestsReader,
	pipelineCfg *slippy.PipelineConfig,
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
	if writer != nil {
		wh := handler.NewSlipWriteHandler(writer)
		handler.RegisterWriteRoutes(v1Only, wh)
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
// is returned unchanged. The dial function creates the Redis client.
func connectCache(
	cfg *config.Config,
	reader domain.SlipReader,
	dial func(*redis.Options) redis.Cmdable,
) domain.SlipReader {
	if !cfg.CacheEnabled() {
		return reader
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
		return reader
	}
	return infrastructure.NewCachedSlipReader(reader, rdb, cfg.CacheTTL)
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
	log.Printf("config loaded (port=%d, cache=%v, db=%s)", cfg.Port, cfg.CacheEnabled(), cfg.SlipDatabase)

	// --- Pipeline configuration ---
	// The slippy library requires a PipelineConfig for all store operations because
	// the schema is dynamic — step columns in ClickHouse are determined by the config.
	pipelineCfg, err := slippy.LoadPipelineConfig()
	if err != nil {
		return fmt.Errorf("pipeline config: %w", err)
	}
	log.Printf("pipeline config loaded (%s, %d steps)", pipelineCfg.Name, len(pipelineCfg.Steps))

	// --- ClickHouse store ---
	chCfg, err := clickhouse.ClickhouseLoadConfig()
	if err != nil {
		return fmt.Errorf("clickhouse config: %w", err)
	}
	store, err := slippy.NewClickHouseStoreFromConfig(chCfg, slippy.ClickHouseStoreOptions{
		SkipMigrations: cfg.SkipMigrations,
		PipelineConfig: pipelineCfg,
		Database:       cfg.SlipDatabase,
	})
	if err != nil {
		return fmt.Errorf("clickhouse store: %w", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			log.Printf("warning: clickhouse store close: %v", closeErr)
		}
	}()
	log.Printf("clickhouse store connected")

	// Adapt the read+write store to our read-only interface.
	adapter := infrastructure.NewSlipStoreAdapter(store)

	// --- GitHub ancestry resolution ---
	// When a commit doesn't have a routing slip, walk backwards through commit
	// history via the GitHub GraphQL API to find an ancestor that does.
	ghCfg := slippy.GitHubConfig{
		AppID:         cfg.GitHubAppID,
		PrivateKey:    cfg.GitHubPrivateKey,
		EnterpriseURL: cfg.GitHubEnterpriseURL,
	}
	ghClient, ghErr := slippy.NewGitHubClient(ghCfg, logger.NewStdLogger(false))
	if ghErr != nil {
		return fmt.Errorf("github client: %w", ghErr)
	}
	slippyClient := slippy.NewClientWithDependencies(store, ghClient, slippy.Config{
		AncestryDepth:  cfg.AncestryDepth,
		PipelineConfig: pipelineCfg,
	})
	slipReader := infrastructure.NewSlipResolverAdapter(slippyClient, adapter)
	log.Printf("github ancestry resolution enabled (depth=%d)", cfg.AncestryDepth)

	// --- Optional Dragonfly/Redis cache ---
	reader := connectCache(cfg, slipReader, redisDial)

	// --- BuildInfo reader for image tag resolution ---
	// Uses the same ClickHouse session as the slip store to query ci.buildinfo
	// and ci.repoproperties without opening a second connection.
	imageTagReader := infrastructure.NewBuildInfoReader(store.Session(), reader)

	// --- CI Job Log reader ---
	// Uses the same ClickHouse session to query observability.ciJob.
	ciJobLogReader := infrastructure.NewCIJobLogStore(store.Session())

	// --- Automation test results reader ---
	// Uses the same ClickHouse session to query autotest_results.RunResults.
	automationTestResultsReader := infrastructure.NewAutomationTestResultsStore(store.Session())

	// --- Automation tests (per-test) reader ---
	// Uses the same ClickHouse session to query autotest_results.TestResults.
	automationTestsReader := infrastructure.NewAutomationTestsStore(store.Session())

	// --- Optional write support ---
	// When SLIPPY_WRITE_API_KEY is configured, expose write endpoints.
	var writer domain.SlipWriter
	if cfg.WriteAPIKey != "" {
		writer = infrastructure.NewSlipWriterAdapter(slippyClient)
		log.Printf("write endpoints enabled")
	}

	// --- HTTP Server ---
	otelHandler := buildHandler(
		cfg, reader, writer, imageTagReader, ciJobLogReader,
		automationTestResultsReader, automationTestsReader, pipelineCfg,
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
