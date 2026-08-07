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
	"sort"
	"strings"
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

// newServeMux builds the router buildHandler registers on. Extracted as a package
// variable so TestBuildHandler_CredentialFreeSurfaceIsClosed can substitute a recording
// mux and observe the routes huma registers straight on the adapter, outside the
// middleware chain. Those never enter the OpenAPI document, so there is no other way to
// enumerate them.
//
// This is NOT the seam redisDial uses. redisDial is injected as a parameter —
// connectCache takes a dial func and run() passes redisDial to it — and is never
// reassigned; every test supplies its own closure as an argument. A package-level var
// that a test mutates is the weaker form: it forecloses t.Parallel() anywhere in package
// main, and `reassign` cannot flag it because .golangci.yml sets `tests: false`. Thread
// the mux through the parameter list the next time buildHandler's signature is revisited
// — the sole consumer of this seam is one test.
var newServeMux = func() humago.Mux { return http.NewServeMux() }

// Access tiers an operation can be served at, as enforced by the middleware.
const (
	tierPublic = "public" // no credential; must also be in middleware.PublicRoutes()
	tierRead   = "read"   // apiKey — either key accepted
	tierWrite  = "write"  // writeApiKey — write key only
)

// operationTiers is the tier every registered operation must be served at.
//
// The middleware tiers on scheme names, and "apiKey" is a *known* name — so an
// operation that should be write-tier but declares apiKeySecurity is served at the read
// tier by design, and every structural check passes: the declaration is non-empty, the
// requirement is non-empty, and the scheme is in the document. That is the likelier of
// the two declaration mistakes, because slip_write_handler.go carries eight hand-written
// writeApiKeySecurity registrations while apiKeySecurity is exported from a sibling file
// in the same package, so a copy-pasted block compiles, lints, passes, and publishes a
// spec saying the read key may mutate.
//
// Tiers are never inferred from the HTTP method: find-by-commits and find-all-by-commits
// are read-tier POSTs.
//
// This table is read in two places with deliberately different strictness.
// verifyRouteSecurity checks only the escalation direction at startup (see there for
// why); the route audit in main_test.go checks every tier in both directions against the
// fully-wired fixture.
var operationTiers = map[string]string{
	"health-check":    tierPublic,
	"v1-health-check": tierPublic,

	"get-slip":                                     tierRead,
	"v1-get-slip":                                  tierRead,
	"get-slip-by-commit":                           tierRead,
	"v1-get-slip-by-commit":                        tierRead,
	"find-by-commits":                              tierRead,
	"v1-find-by-commits":                           tierRead,
	"find-all-by-commits":                          tierRead,
	"v1-find-all-by-commits":                       tierRead,
	"get-image-tags":                               tierRead,
	"v1-get-image-tags":                            tierRead,
	"get-logs":                                     tierRead,
	"v1-get-logs":                                  tierRead,
	"get-pipeline-config":                          tierRead,
	"get-step-prerequisites":                       tierRead,
	"get-automation-test-results":                  tierRead,
	"get-automation-test-results-tests":            tierRead,
	"get-automation-test-result-by-id-correlation": tierRead,
	"get-clickhouse-schema-version":                tierRead,

	"create-slip":   tierWrite,
	"start-step":    tierWrite,
	"complete-step": tierWrite,
	"fail-step":     tierWrite,
	"skip-step":     tierWrite,
	"set-image-tag": tierWrite,
	"promote-slip":  tierWrite,
	"abandon-slip":  tierWrite,
}

// verifyRouteSecurity reports an error when the registered routes contradict the auth
// policy in a way that is unrecoverable at request time.
//
// These are the same assertions the route audit makes in main_test.go, moved to process
// startup so they cannot be routed around. The repository's branch ruleset does require
// the unit-test check on main, but it also carries bypass actors with bypass_mode
// "always", so a red test is one deliberate merge away from not applying.
//
// Three conditions are checked, and each maps to a failure the middleware cannot
// mitigate once the process is serving:
//
//  1. Requires no credential and is not allowlisted → 401 for every caller. The shape
//     is an in-place path rename that leaves the allowlist on the old path.
//  2. Allowlisted but requires a credential → also 401 for every caller. The middleware
//     consults publicRoutes only for operations that require no credential, so the
//     allowlist entry goes dead. This is the same probe outage as (1) arriving from the
//     opposite direction: kubelet's liveness AND readiness, every replica, at once.
//  3. Listed write-tier but not served at the write tier → silent privilege escalation.
//     Unlike (1) and (2) the route keeps working, so nothing surfaces while the read key
//     gains a mutation.
//
// (Dropping a prefix instead is harmless: the route is de-registered from the mux
// entirely, so requests 404 and the middleware never runs.)
//
// Deliberately NOT checked here, though the audit checks both against the wired fixture:
//
//   - The reverse direction of operationTiers — that every row names a registered
//     operation. buildHandler registers conditionally: when ClickHouse is unavailable it
//     leaves five collaborators nil and eight rows match nothing. That degraded mode is
//     supported, so asserting it at startup would convert a documented fallback into a
//     crashloop — inverting the availability benefit this guard exists to provide.
//   - Read-tier declarations. A read-tier operation declaring something else costs one
//     route a 403, not a fleet-wide outage, and refusing to boot would be the heavier
//     failure of the two.
//
// This walks the OpenAPI document, so it shares that document's blind spot for
// operations marked Hidden. Those are still enforced by the middleware at runtime.
func verifyRouteSecurity(api huma.API) error {
	var undeclared, contradicted, mistiered []string
	for path, item := range api.OpenAPI().Paths {
		// PathItem has no operation iterator, so the verbs are enumerated explicitly.
		for _, op := range []*huma.Operation{
			item.Get, item.Put, item.Post, item.Delete,
			item.Options, item.Head, item.Patch, item.Trace,
		} {
			if op == nil {
				continue
			}
			route := fmt.Sprintf("%s %s (operationId %q)", strings.ToUpper(op.Method), path, op.OperationID)
			needsCredential := middleware.RequiresCredential(op)
			switch {
			case middleware.IsPublicRoute(op.Method, op.Path) && needsCredential:
				contradicted = append(contradicted, route)
			case !middleware.IsPublicRoute(op.Method, op.Path) && !needsCredential:
				undeclared = append(undeclared, route)
			case operationTiers[op.OperationID] == tierWrite && !middleware.ServedAtWriteTier(op):
				mistiered = append(mistiered, route)
			}
		}
	}

	switch {
	case len(contradicted) > 0:
		sort.Strings(contradicted)
		return fmt.Errorf(
			"refusing to start: %d allowlisted route(s) declare a Security requirement: %s — the "+
				"middleware consults publicRoutes only for operations that require no credential, so "+
				"these would return 401 to every caller including the kubelet probes. Drop the Security "+
				"declaration, or remove the route from publicRoutes in internal/middleware/auth.go",
			len(contradicted), strings.Join(contradicted, ", "))
	case len(undeclared) > 0:
		sort.Strings(undeclared)
		return fmt.Errorf(
			"refusing to start: %d route(s) require no credential and are not in publicRoutes, so they "+
				"would return 401 to every caller: %s — declare Security on the operation, or add the "+
				"route to publicRoutes in internal/middleware/auth.go",
			len(undeclared), strings.Join(undeclared, ", "))
	case len(mistiered) > 0:
		sort.Strings(mistiered)
		return fmt.Errorf(
			"refusing to start: %d route(s) are write-tier in operationTiers but would be served at the "+
				"read tier: %s — the declaration must name writeApiKey, otherwise SLIPPY_API_KEY is "+
				"accepted for a mutation",
			len(mistiered), strings.Join(mistiered, ", "))
	}
	return nil
}

// buildHandler creates the fully-wired HTTP handler with auth, routes, and
// OpenTelemetry instrumentation. This is extracted from run() for testability.
// The imageTagReader, ciJobLogReader, automationTestResultsReader,
// automationTestsReader, and diagnosticsHandler are optional — if nil, their
// endpoints are not registered.
//
// It returns an error when the registered routes fail verifyRouteSecurity, so a
// wiring mistake stops the process at boot instead of serving 401s.
func buildHandler(
	cfg *config.Config,
	reader domain.SlipReader,
	writer domain.SlipWriter,
	imageTagReader domain.ImageTagReader,
	ciJobLogReader domain.CIJobLogReader,
	automationTestResultsReader domain.AutomationTestResultsReader,
	automationTestsReader domain.AutomationTestsReader,
	pipelineCfg *slippy.PipelineConfig,
	diagnosticsHandler *handler.DiagnosticsHandler,
) (http.Handler, error) {
	mux := newServeMux()
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

	// Diagnostic routes: v1-only. Read-only probes of the service's own datastores;
	// they require the read key like every other read operation.
	if diagnosticsHandler != nil {
		handler.RegisterDiagnosticsRoutes(v1Only, diagnosticsHandler)
	}

	// Fail the wiring rather than the requests: a route that requires no credential
	// and is not allowlisted would 401 for everyone, health probes included.
	if err := verifyRouteSecurity(api); err != nil {
		return nil, err
	}

	// Wrap with OpenTelemetry instrumentation.
	return otelhttp.NewHandler(mux, "slippy-api"), nil
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
	// Warn rather than refuse: the deployed values live in Vault/ManagementInfra, so a
	// hard failure here could take every replica down on a config this process cannot
	// verify from the inside. The condition is otherwise undetectable at runtime — the
	// tiering evaluates correctly and simply returns the same answer for both keys.
	if cfg.TiersCollapsed() {
		log.Printf(
			"WARNING: SLIPPY_API_KEY equals SLIPPY_WRITE_API_KEY — the read/write tier split is " +
				"inert and every read-key holder can mutate slips; issue distinct keys")
	}

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
	// ClickHouse now backs only the non-slip readers (buildinfo/ciJob/autotest) and the
	// schema-version diagnostic — the slip path is 100% Postgres. So a ClickHouse outage must NOT gate
	// startup of a healthy slip API: log a warning and run degraded (those routes are left
	// unregistered → 404) while the Postgres slip endpoints serve normally.
	chConnectCtx, chConnectCancel := context.WithTimeout(context.Background(), startupConnectTimeout)
	defer chConnectCancel()
	chSession, chErr := clickhouse.NewClickhouseSession(chCfg, chConnectCtx)
	if chErr != nil {
		log.Printf("warning: clickhouse session unavailable — non-slip readers + diagnostics run degraded: %v", chErr)
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

	// --- ClickHouse-backed non-slip readers + diagnostics (nil ⇒ routes skipped in degraded mode) ---
	// All of these query ClickHouse via the standalone session. When that session is
	// unavailable (above), they stay nil and buildHandler leaves their routes unregistered,
	// so the Postgres slip API keeps serving.
	var (
		imageTagReader              domain.ImageTagReader
		ciJobLogReader              domain.CIJobLogReader
		automationTestResultsReader domain.AutomationTestResultsReader
		automationTestsReader       domain.AutomationTestsReader
		diagnosticsH                *handler.DiagnosticsHandler
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
		diagnosticsH = handler.NewDiagnosticsHandler(
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
	otelHandler, err := buildHandler(
		cfg, reader, writer, imageTagReader, ciJobLogReader,
		automationTestResultsReader, automationTestsReader, pipelineCfg, diagnosticsH,
	)
	if err != nil {
		return err
	}
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
