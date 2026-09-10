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

// handlerDeps carries everything buildHandler wires together.
//
// A struct rather than a positional list because the list had already reached nine
// parameters, most of them nil at most call sites — `buildHandler(cfg, reader, nil, nil,
// nil, nil, nil, nil, nil)` says nothing about which nil is which. Named fields also let
// a caller supply exactly the collaborators it needs without counting commas.
//
// Every field is injected, following connectCache's dial parameter rather than a package
// global: a collaborator reached for from package scope is invisible at the call site,
// forecloses t.Parallel(), and — for a var a test reassigns — escapes the `reassign`
// linter entirely, since .golangci.yml sets `tests: false`.
type handlerDeps struct {
	// Required, and dereferenced before any route is registered — a nil panics on
	// deps.cfg.APIKey rather than returning the error buildHandler otherwise returns.
	cfg *config.Config

	// mux is the router to register on. Nil means a fresh http.ServeMux, which is what
	// run() wants; TestBuildHandler_CredentialFreeSurfaceIsClosed passes a recording mux
	// to enumerate the routes huma registers straight on the adapter, outside the
	// middleware chain. Those never enter the OpenAPI document, so there is no other way
	// to see them.
	mux humago.Mux

	// Required. Neither is gated: their operationTiers rows carry gateAlways and their
	// Register* calls run unconditionally, so a nil here does not withhold a route — it
	// leaves one registered that fails on every request. A nil pipelineCfg serves 500
	// "pipeline config not available" from internal/handler/pipeline_config.go for both
	// get-pipeline-config and get-step-prerequisites.
	reader      domain.SlipReader
	pipelineCfg *slippy.PipelineConfig

	// Optional: a nil field leaves the corresponding routes unregistered. That is how the
	// service runs degraded when ClickHouse is unavailable — with two exceptions worth
	// naming, because the shorthand is wrong for both. writer is not a ClickHouse
	// collaborator at all (writes go to Postgres); and automationTestsReader's routes are
	// dropped one level down, by the `h.testsReader != nil` guard inside
	// RegisterAutomationTestResultsRoutes, not by any nil check in buildHandler — which is
	// precisely why gateAutomationTests has to mirror that check across a package boundary.
	writer                      domain.SlipWriter
	imageTagReader              domain.ImageTagReader
	ciJobLogReader              domain.CIJobLogReader
	automationTestResultsReader domain.AutomationTestResultsReader
	automationTestsReader       domain.AutomationTestsReader

	// chSession backs the ClickHouse schema-version diagnostic, and slipDatabase names the
	// database it probes. The session rather than a built *handler.DiagnosticsHandler: this
	// struct carries collaborators and buildHandler owns handler construction — it builds
	// the other seven — so receiving one pre-built made run() reach into the handler layer
	// for no reason and left one field a product where every other is an ingredient.
	chSession    clickhouse.ClickhouseSessionInterface
	slipDatabase string

	// rateLimiter applies per-identity backoff to failed authentication. Nil disables it,
	// which is the default until SLIPPY_RATE_LIMIT_ENABLED is set — and also what happens
	// when Dragonfly is unavailable, since the counters have nowhere shared to live.
	rateLimiter *middleware.RateLimiter
}

// Access tiers an operation can be served at, as enforced by the middleware.
const (
	tierPublic = "public" // no credential; must also be in middleware.PublicRoutes()
	tierRead   = "read"   // apiKey — either key accepted
	tierWrite  = "write"  // writeApiKey — write key only
)

// Gates naming the optional collaborator whose presence registers an operation.
//
// buildHandler registers conditionally: when ClickHouse is unavailable it leaves five
// collaborators nil and their routes go unregistered, which main.go documents as a
// supported degraded mode. Recording which gate gates which row is what lets the startup
// guard check the reverse direction of operationTiers — that every row still names a
// registered operation — without turning that degraded boot into a crashloop. A row
// whose gate is down is expected to be absent; a row whose gate is up and is absent
// anyway is a stale entry.
const (
	gateAlways     = ""                            // registered unconditionally
	gateImageTags  = "imageTagReader"              // ci.buildinfo / ci.repoproperties
	gateCIJobLogs  = "ciJobLogReader"              // observability.ciJob
	gateAutomation = "automationTestResultsReader" // autotest_results.* run summaries
	// gateAutomationTests is separate from gateAutomation because
	// RegisterAutomationTestResultsRoutes is the one Register* function with an internal
	// conditional: it registers the run-summary route unconditionally, then guards the two
	// per-test drill-downs behind `h.testsReader != nil`. Conflating the two gates makes a
	// documented, supported wiring (parent reader set, drill-down reader nil) refuse to boot.
	//
	// This mirrors a nil check across a package boundary with no compile-time link. If the
	// drill-downs ever become unconditional, or gain a third condition, this diverges again
	// with the same failure mode — it is a mirror, not a link.
	gateAutomationTests = "automationTestsReader" // autotest_results.* per-test drill-down
	gateWrites          = "writer"                // slip mutations
	gateDiagnostics     = "chSession"             // legacy CH schema-version probe
)

// clickHouseSessionOrNil converts the concrete session run() receives into the interface
// handlerDeps carries, returning a genuinely nil interface when there is no usable session.
//
// This exists because the conversion is a trap rather than a formality. A nil *pointer*
// stored in an interface produces a NON-nil interface, so assigning a failed
// *clickhouse.ClickhouseSession straight into an interface-typed field makes every
// `!= nil` check downstream read true. The gate would come up during a ClickHouse outage,
// registering and publishing a diagnostics route whose handler dereferences the nil session
// and drops the connection — the opposite of the degraded-boot behaviour main.go documents.
//
// Taking the concrete type as a parameter is what makes the nil check here a real pointer
// comparison, and what makes the trap unit-testable without reflection.
func clickHouseSessionOrNil(
	sess *clickhouse.ClickhouseSession,
	err error,
) clickhouse.ClickhouseSessionInterface {
	if err != nil || sess == nil {
		return nil
	}
	return sess
}

// gateStatus is the single definition of which gates exist and which of them are up.
//
// Both facts used to be written more than once. The five nil checks that decide
// registration in buildHandler were restated — not derived — in the map handed to
// verifyRouteSecurity a few lines below, and the set of gates appeared a third time in a
// standalone knownGates. Both desync directions were reachable: a gate truer than its
// registration fires the stale-row check during a *degraded* boot and turns a tolerated
// ClickHouse outage into a crashloop, while a gate falser than its registration strands its
// rows in silence — as does a gate added to the const block and forgotten in the map.
//
// Go cannot enforce the correspondence; there is no way to require a new constant to
// acquire a map entry. What one definition buys is that there is nothing to keep in sync
// rather than a rule to remember.
//
// The key set is the declared gate set no matter what is wired, which is what lets
// verifyGateNames validate operationTiers against gateStatus(handlerDeps{}).
func gateStatus(deps handlerDeps) map[string]bool {
	automation := deps.automationTestResultsReader != nil
	return map[string]bool{
		gateAlways:     true,
		gateImageTags:  deps.imageTagReader != nil,
		gateCIJobLogs:  deps.ciJobLogReader != nil,
		gateAutomation: automation,
		// A conjunction, not just deps.automationTestsReader: the inner `h.testsReader != nil`
		// guard inside RegisterAutomationTestResultsRoutes only runs when the outer check did.
		// This is the one entry that genuinely mirrors a nil check across a package boundary
		// with no compile-time link — see gateAutomationTests above.
		gateAutomationTests: automation && deps.automationTestsReader != nil,
		gateWrites:          deps.writer != nil,
		gateDiagnostics:     deps.chSession != nil,
	}
}

// verifyGateNames reports rows naming a gate that is not a declared constant. A row naming
// anything else would be skipped forever by the stale-row check, because a map lookup on a
// missing key is indistinguishable from "gate is down" — the one fail-open default in a
// file whose every other decision fails closed.
//
// gates supplies the declared set: buildHandler passes gateStatus(handlerDeps{}), whose key
// set IS the gate constants, so there is no second list to drift from the first. Only the
// keys are read here; the values belong to verifyRouteSecurity, which takes its own map, so
// requiring presence in this function does not tighten that one's "absent or false means
// down" contract or disturb the degraded-boot subtest that passes it an empty map.
func verifyGateNames(gates map[string]bool) error {
	var unknown []string
	for opID, policy := range operationTiers {
		if _, ok := gates[policy.gate]; !ok {
			unknown = append(unknown, fmt.Sprintf("%s (gate %q)", opID, policy.gate))
		}
	}
	return routeSecurityError(unknown,
		"operationTiers row(s) name a gate that is not a declared constant",
		"an unknown gate reads as permanently down, so the stale-row check silently skips those "+
			"rows. Add the gate to the const block and give it an entry in gateStatus")
}

// operationPolicy is the tier an operation must be served at, plus the gate that decides
// whether it is registered at all.
type operationPolicy struct {
	tier string
	gate string
}

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
// This table is the single inventory of the service's route policy. verifyRouteSecurity
// enforces it at startup; the route audit in main_test.go re-checks it against the
// fully-wired fixture, where every gate is up and read-tier declarations can be pinned
// exactly.
var operationTiers = map[string]operationPolicy{
	"health-check":    {tierPublic, gateAlways},
	"v1-health-check": {tierPublic, gateAlways},

	"get-slip":               {tierRead, gateAlways},
	"v1-get-slip":            {tierRead, gateAlways},
	"get-slip-by-commit":     {tierRead, gateAlways},
	"v1-get-slip-by-commit":  {tierRead, gateAlways},
	"find-by-commits":        {tierRead, gateAlways},
	"v1-find-by-commits":     {tierRead, gateAlways},
	"find-all-by-commits":    {tierRead, gateAlways},
	"v1-find-all-by-commits": {tierRead, gateAlways},
	"get-pipeline-config":    {tierRead, gateAlways},
	"get-step-prerequisites": {tierRead, gateAlways},

	"get-image-tags":    {tierRead, gateImageTags},
	"v1-get-image-tags": {tierRead, gateImageTags},
	"get-logs":          {tierRead, gateCIJobLogs},
	"v1-get-logs":       {tierRead, gateCIJobLogs},

	"get-automation-test-results":                  {tierRead, gateAutomation},
	"get-automation-test-results-tests":            {tierRead, gateAutomationTests},
	"get-automation-test-result-by-id-correlation": {tierRead, gateAutomationTests},

	"get-clickhouse-schema-version": {tierRead, gateDiagnostics},

	"create-slip":   {tierWrite, gateWrites},
	"start-step":    {tierWrite, gateWrites},
	"complete-step": {tierWrite, gateWrites},
	"fail-step":     {tierWrite, gateWrites},
	"skip-step":     {tierWrite, gateWrites},
	"set-image-tag": {tierWrite, gateWrites},
	"promote-slip":  {tierWrite, gateWrites},
	"abandon-slip":  {tierWrite, gateWrites},
	"claim-slip":    {tierWrite, gateWrites},
}

// verifyRouteSecurity reports an error when the registered routes contradict the auth
// policy in a way that is unrecoverable at request time.
//
// These are the same assertions the route audit makes in main_test.go, moved to process
// startup so they cannot be routed around. The repository's branch ruleset does require
// the unit-test check on main, but it also carries bypass actors with bypass_mode
// "always", so a red test is one deliberate merge away from not applying.
//
// Seven conditions are checked, and each maps to a failure the middleware cannot
// mitigate once the process is serving:
//
//  1. Requires no credential and is not allowlisted → 401 for every caller. The shape
//     is an in-place path rename that leaves the allowlist on the old path.
//
//  2. Allowlisted but requires a credential → also 401 for every caller. The middleware
//     consults publicRoutes only for operations that require no credential, so the
//     allowlist entry goes dead. This is the same probe outage as (1) arriving from the
//     opposite direction: kubelet's liveness AND readiness, every replica, at once.
//
//  3. Registered but absent from operationTiers → never tier-checked at all, because the
//     map lookup yields the zero policy whose tier matches nothing. A mutation that was
//     copy-pasted with apiKeySecurity and never added to the table is served to the read
//     key, and that is one author making one lapse.
//
//  4. Public-tier in operationTiers but demanding a credential → the probe outage of (1)
//     and (2) arriving with the path renamed AND the declaration added. Each half alone is
//     caught by an earlier check; the combination falls between them.
//
//  5. Not public-tier in operationTiers but requiring no credential → an OPEN ROUTE. The
//     mirror of (4), and the only one of the pair that opens a route rather than closing
//     one: the route is allowlisted so (2) skips it, and the tier arms below only fire for
//     public and write rows, so a read-tier row lands on the allowlist and serves to anyone.
//
//  6. Listed, not named read or public, and not served at the write tier → silent
//     privilege escalation. Unlike the outage classes the route keeps working, so nothing
//     surfaces while the read key gains a mutation.
//
//     Stated negatively on purpose. `tier == tierWrite` would have matched only the two
//     spellings it knows, and policy.tier is a free-form string: a typo, a rename, or a
//     fourth tier constant added later falls through every arm, so the row is listed —
//     satisfying (3) — yet never tier-checked, which silently removes the one automated
//     check against a mutation declaring apiKeySecurity. Read-tier rows still short-circuit
//     here, so the deliberate decision not to check them (below) is unchanged; what is
//     newly refused is a row whose tier is not a name this function handles.
//
//     It also sits outside the switch. The chain is exclusive and (5) is simultaneously
//     true for a write-tier row on an allowlisted route with no Security, so (5) alone
//     reported and this class stayed hidden until the operator had fixed (5) and
//     redeployed — the one-class-per-boot cost the errors.Join below exists to avoid. The
//     `listed` guard keeps an unlisted route from being judged against a zero policy;
//     (3) already owns that case.
//
//  7. Listed with a live gate but not registered → a stale row. The table is the inventory
//     every other check is read against, so a row that no longer matches anything silently
//     narrows all of them.
//
// (Dropping a prefix is harmless by contrast: the route is de-registered from the mux
// entirely, so requests 404 and the middleware never runs.)
//
// Gates are what make (7) safe. buildHandler registers conditionally — a ClickHouse
// outage leaves five collaborators nil and their routes unregistered — so the check runs
// only for rows whose gate is up. A degraded boot stays a boot.
//
// Read-tier declarations are deliberately NOT checked here, though the audit checks them
// against the fully-wired fixture: a read-tier operation declaring something else costs
// one route a 403 rather than a fleet-wide outage, and refusing to boot would be the
// heavier of the two failures.
//
// On Hidden operations. This walks the OpenAPI document, and huma omits Hidden operations
// from it while still routing them — so checks (1) through (6) cannot see such a route,
// and it serves traffic no document-walking check can inspect. Check (7) is the exception
// and it points the other way: a Hidden operation that has a row with a live gate looks
// unregistered, so the process refuses to boot. That coupling is deliberate. Re-keying the
// stale scan on registered mux patterns would make Hidden routes boot silently, and since
// the middleware fails closed on a missing credential but not on the wrong tier, a Hidden
// mutation declaring apiKey would then be served to the read key with nothing to catch it.
// The boot failure is currently the only signal anywhere in the system. If Hidden is ever
// genuinely needed, add a mux-pattern-based tier check alongside the document walk — never
// instead of it.
func verifyRouteSecurity(api huma.API, liveGates map[string]bool) error {
	var undeclared, contradicted, unlisted, mispublic, overexposed, mistiered []string
	registered := map[string]struct{}{}
	for path, item := range api.OpenAPI().Paths {
		// PathItem has no operation iterator, so the verbs are enumerated explicitly.
		for _, op := range []*huma.Operation{
			item.Get, item.Put, item.Post, item.Delete,
			item.Options, item.Head, item.Patch, item.Trace,
		} {
			if op == nil {
				continue
			}
			registered[op.OperationID] = struct{}{}
			route := fmt.Sprintf("%s %s (operationId %q)", strings.ToUpper(op.Method), path, op.OperationID)
			needsCredential := middleware.RequiresCredential(op)
			policy, listed := operationTiers[op.OperationID]
			switch {
			case middleware.IsPublicRoute(op.Method, op.Path) && needsCredential:
				contradicted = append(contradicted, route)
			case !middleware.IsPublicRoute(op.Method, op.Path) && !needsCredential:
				undeclared = append(undeclared, route)
			case !listed:
				unlisted = append(unlisted, route)
			case policy.tier == tierPublic && needsCredential:
				mispublic = append(mispublic, route)
			case policy.tier != tierPublic && !needsCredential:
				overexposed = append(overexposed, route)
			}

			// Check (6), outside the exclusive chain and stated as "not read or public"
			// rather than "is write" — see the numbered list above for both reasons.
			if listed && policy.tier != tierRead && policy.tier != tierPublic &&
				!middleware.RequiresWriteKey(op) {
				mistiered = append(mistiered, route)
			}
		}
	}

	var stale []string
	for opID, policy := range operationTiers {
		if !liveGates[policy.gate] {
			continue
		}
		if _, ok := registered[opID]; !ok {
			stale = append(stale, opID)
		}
	}

	// Every class is reported, not just the first. Under a boot guard each hidden class
	// costs a full build-and-deploy cycle to discover, and the buckets are already
	// computed. errors.Join returns nil when every argument is nil.
	return errors.Join(
		routeSecurityError(contradicted,
			"allowlisted route(s) declare a Security requirement",
			"the middleware consults publicRoutes only for operations that require no credential, so "+
				"these would return 401 to every caller including the kubelet probes. Drop the Security "+
				"declaration, or remove the route from publicRoutes in internal/middleware/auth.go"),
		routeSecurityError(undeclared,
			"route(s) require no credential and are not in publicRoutes",
			"they would return 401 to every caller. Declare Security on the operation, or add the "+
				"route to publicRoutes in internal/middleware/auth.go"),
		routeSecurityError(unlisted,
			"registered route(s) are absent from operationTiers",
			"the table is the inventory every tier check is read against, so an unlisted route is "+
				"never tier-checked at all and a mutation declaring apiKey would be served to "+
				"SLIPPY_API_KEY. Add a row naming the tier and the gate"),
		routeSecurityError(mispublic,
			"route(s) are public-tier in operationTiers but demand a credential",
			"a public-tier row asserts the route serves with no credential, so this is the probe "+
				"outage above arriving with the path renamed AND the declaration added. Drop the "+
				"Security declaration, or change the row's tier"),
		routeSecurityError(overexposed,
			"route(s) are not public-tier in operationTiers but require no credential",
			"the route is on the publicRoutes allowlist, so it is served to anyone while the "+
				"inventory says a key is required. This is the mirror of the case above and the "+
				"only one of the pair that opens a route rather than closing one. Declare Security "+
				"on the operation, or change the row's tier to public and keep the allowlist entry"),
		routeSecurityError(mistiered,
			"route(s) would be served at the read tier but operationTiers does not name them read or public",
			"the row is write-tier, is misspelled, or names a tier constant no arm in "+
				"verifyRouteSecurity handles. Add writeApiKey to the declaration, correct the "+
				"spelling, or add an arm before using a new tier constant — otherwise "+
				"SLIPPY_API_KEY is accepted for a mutation"),
		routeSecurityError(stale,
			"operationTiers row(s) have a live gate but name no registered operation",
			"the table is the inventory every other route-security check is read against, so a stale "+
				"row silently narrows all of them. Remove the row, or restore the route"),
	)
}

// routeSecurityError renders one class of route-security defect, or nil when the class
// is empty. Names are sorted so the message is stable across map-iteration order.
func routeSecurityError(names []string, what, why string) error {
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	return fmt.Errorf("refusing to start: %d %s: %s — %s", len(names), what, strings.Join(names, ", "), why)
}

// buildHandler creates the fully-wired HTTP handler with auth, routes, and
// OpenTelemetry instrumentation. This is extracted from run() for testability.
// The imageTagReader, ciJobLogReader, automationTestResultsReader,
// automationTestsReader, and chSession are optional — if nil, their
// endpoints are not registered.
//
// It returns an error when the registered routes fail verifyRouteSecurity, so a
// wiring mistake stops the process at boot instead of serving 401s.
func buildHandler(deps handlerDeps) (http.Handler, error) {
	mux := deps.mux
	if mux == nil {
		mux = http.NewServeMux()
	}
	apiConfig := huma.DefaultConfig("Slippy API", "1.0.0")
	apiConfig.Info.Description = "API for CI/CD routing slips"

	// Define the security schemes used by protected operations.
	apiConfig.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"apiKey":      {Type: "http", Scheme: "bearer"},
		"writeApiKey": {Type: "http", Scheme: "bearer"},
	}

	api := humago.New(mux, apiConfig)

	// One evaluation of the nil checks, read by both the registration branches below and
	// the startup guard, so the guard's view of which operations should exist cannot
	// disagree with the code that registered them.
	gates := gateStatus(deps)

	// Register authentication middleware. The limiter is an option rather than a parameter
	// so a nil one simply means "no backoff" without a second constructor.
	authOpts := []middleware.AuthOption{}
	if deps.rateLimiter != nil {
		authOpts = append(authOpts, middleware.WithRateLimit(deps.rateLimiter))
	}
	api.UseMiddleware(middleware.NewAPIKeyAuth(deps.cfg.APIKey, deps.cfg.WriteAPIKey, authOpts...))

	// Register routes on both unversioned (legacy) and /v1 paths.
	// The empty prefix keeps existing routes unchanged for backward compatibility.
	// The "/v1" prefix registers versioned routes with "v1-" prefixed OperationIDs.
	grp := huma.NewGroup(api, "", "/v1")

	handler.RegisterHealthRoutes(grp)
	h := handler.NewSlipHandler(deps.reader)
	handler.RegisterRoutes(grp, h)

	// Register image tag routes when a deps.reader is available.
	if gates[gateImageTags] {
		ith := handler.NewImageTagHandler(deps.imageTagReader)
		handler.RegisterImageTagRoutes(grp, ith)
	}

	// Register CI job log routes when a deps.reader is available.
	if gates[gateCIJobLogs] {
		clh := handler.NewCIJobLogHandler(deps.ciJobLogReader)
		handler.RegisterCIJobLogRoutes(grp, clh)
	}

	// Register v1-only routes (no legacy unversioned paths) below.
	v1Only := huma.NewGroup(api, "/v1")

	// Pipeline config: v1-only.
	pch := handler.NewPipelineConfigHandler(deps.pipelineCfg)
	handler.RegisterPipelineConfigRoutes(v1Only, pch)

	// Step prerequisites: v1-only.
	sprh := handler.NewStepPrerequisitesHandler(deps.reader, deps.pipelineCfg)
	handler.RegisterStepPrerequisitesRoutes(v1Only, sprh)

	// Automation test results: v1-only. The optional deps.automationTestsReader
	// powers the per-test drill-down endpoints; when nil, only the parent
	// run-summary routes are registered.
	if gates[gateAutomation] {
		atrh := handler.NewAutomationTestResultsHandler(deps.automationTestResultsReader, deps.automationTestsReader)
		handler.RegisterAutomationTestResultsRoutes(v1Only, atrh)
	}

	// Write routes: v1-only.
	// Extract cache invalidator from deps.reader when available (CachedSlipReader implements it).
	if gates[gateWrites] {
		var inv domain.Invalidator
		if i, ok := deps.reader.(domain.Invalidator); ok {
			inv = i
		}
		wh := handler.NewSlipWriteHandler(deps.writer, inv)
		handler.RegisterWriteRoutes(v1Only, wh)
	}

	// Diagnostic routes: v1-only. Read-only probes of the service's own datastores;
	// they require the read key like every other read operation.
	if gates[gateDiagnostics] {
		dh := handler.NewDiagnosticsHandler(deps.chSession, deps.slipDatabase)
		handler.RegisterDiagnosticsRoutes(v1Only, dh)
	}

	// Fail the wiring rather than the requests: a route that requires no credential
	// and is not allowlisted would 401 for everyone, health probes included.
	//
	// The guard reads the same gates map the registration branches above branched on — one
	// evaluation, not a restatement — so its view of which operations *should* exist is
	// exact in a degraded boot as well as a full one.
	//
	// verifyGateNames gets gateStatus(handlerDeps{}) rather than gates: it validates the
	// table against the set of gates that EXIST, which must not vary with what happens to be
	// wired on this boot.
	//
	// Joined, not sequential: an unknown gate would otherwise suppress every route-security
	// class for that boot, which is the "one class per boot" cost the guard exists to avoid.
	if err := errors.Join(
		verifyGateNames(gateStatus(handlerDeps{})),
		verifyRouteSecurity(api, gates),
	); err != nil {
		return nil, err
	}

	// Wrap with security headers and OpenTelemetry instrumentation.
	return otelhttp.NewHandler(securityHeaders(mux), "slippy-api"), nil
}

// securityHeaders sets response headers on every route.
//
// It wraps the mux rather than joining huma's middleware chain because that chain only
// covers operations registered through huma.Register — the six spec and docs routes
// huma registers straight on the adapter would otherwise be uncovered, and those are
// exactly the routes served without a credential.
//
// Headers are set before the inner handler runs, so a handler that sets its own value
// wins. That matters for /docs: huma installs a Content-Security-Policy pinning
// script-src and style-src to the Stoplight bundle it loads, and a default-src 'none'
// policy would blank the page. Everything else keeps the restrictive default.
//
// Cache-Control: no-store is belt-and-braces. RFC 9111 §3.5 already bars a shared cache
// from reusing a response to a request carrying Authorization, but slip payloads carry
// repository names, commit SHAs and pipeline state, and the routes that need no
// credential are not covered by that rule at all.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
		// Set unconditionally even though this listener is plaintext (TLS terminates at the
		// ingress). RFC 6797 §8.1 requires a client to ignore the header on a non-secure
		// connection, so the in-cluster hop is unaffected, and the ingress re-serves it over
		// TLS where it takes effect. Note §7.2 says an HSTS host MUST NOT send the header over
		// non-secure transport — so the strictly correct home for this is the ingress; it is
		// set here so the guarantee does not depend on infrastructure this repo cannot see.
		// No preload: that is a domain-wide commitment belonging to whoever owns the apex.
		h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		next.ServeHTTP(w, r)
	})
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
	// The slip-claim history marker must not share a name with a configured step, or
	// the library's step-timing reconstruction would backfill that step's StartedAt
	// from the marker. The live config is only known here, so this is the one place
	// the invariant can be checked. A warning, not a boot failure: the consequence is
	// a wrong derived timestamp on one step, which does not justify refusing to serve.
	if step := infrastructure.ClaimMarkerStepCollision(pipelineCfg); step != "" {
		log.Printf("WARNING: pipeline config defines a step named %q, which collides with the "+
			"slip-claim history marker; derived step timing for it will be wrong", step)
	}

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
	chSess, chErr := clickhouse.NewClickhouseSession(chCfg, chConnectCtx)
	chSession := clickHouseSessionOrNil(chSess, chErr)
	if chSession == nil {
		log.Printf("warning: clickhouse session unavailable — non-slip readers + diagnostics run degraded: %v", chErr)
	} else {
		defer func() {
			if closeErr := chSess.Close(); closeErr != nil {
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

	// Auth rate limiting. Counters live in the shared cache because the service runs more
	// than one replica: per-process state would hand an attacker one budget per pod and
	// reset the ladder on every deploy. No cache means no limiter — the same fail-open
	// stance the dedup lock takes, and auth itself still fails closed without it.
	var rateLimiter *middleware.RateLimiter
	switch {
	case !cfg.RateLimitEnabled:
		log.Printf("auth rate limiting disabled (SLIPPY_RATE_LIMIT_ENABLED not set)")
	case rdb == nil:
		// WARN, not a passing mention: this is the degradation the design fails open to,
		// and with no cache there is no per-request signal that it happened — so the boot
		// log is the only place an operator can learn the credential-guessing brake is off.
		log.Printf("WARNING: SLIPPY_RATE_LIMIT_ENABLED=true but no cache is available — " +
			"auth rate limiting is OFF for the life of this process; a cache outage at boot " +
			"disables the only credential-guessing control until the next deploy")
	default:
		rateLimiter = middleware.NewRateLimiter(
			infrastructure.NewRedisRateLimitStore(rdb), cfg.XFFDepth, cfg.TrustedProxies)
		if len(cfg.TrustedProxies) == 0 {
			// Not fatal — the limiter is safe without it (it attributes by the unforgeable
			// rightmost hop) — but it collapses all edge traffic onto a handful of edge
			// addresses, so one attacker can lock the fleet. Loud so it is caught in the
			// rollout's "confirm the resolved address" step rather than in production.
			log.Printf("WARNING: auth rate limiting enabled with no SLIPPY_TRUSTED_PROXY_CIDRS — " +
				"edge callers collapse onto the edge address; set it to your edge ranges " +
				"before trusting per-client attribution")
		}
		log.Printf("auth rate limiting enabled (xff_depth=%d, trusted_proxy_cidrs=%d)",
			cfg.XFFDepth, len(cfg.TrustedProxies))
	}

	// --- HTTP Server ---
	otelHandler, err := buildHandler(handlerDeps{
		cfg:                         cfg,
		reader:                      reader,
		writer:                      writer,
		imageTagReader:              imageTagReader,
		ciJobLogReader:              ciJobLogReader,
		automationTestResultsReader: automationTestResultsReader,
		automationTestsReader:       automationTestsReader,
		pipelineCfg:                 pipelineCfg,
		rateLimiter:                 rateLimiter,
		chSession:                   chSession,
		slipDatabase:                cfg.SlipDatabase,
	})
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           otelHandler,
		ReadHeaderTimeout: 10 * time.Second,
		// Without ReadTimeout AND IdleTimeout, Server.idleTimeout() falls back to ReadTimeout
		// (zero), so conn.serve clears the read deadline entirely and a keep-alive connection
		// is never reaped. An unauthenticated caller can hold connections open on /health
		// until file descriptors run out.
		//
		// WriteTimeout is now safe to set because the ancestry fan-out behind
		// find-by-commits/find-all-by-commits is bounded (maxAncestryResolutions, plus a
		// maxItems cap on the request). Before that bound a write deadline would have cut
		// legitimate slow requests instead of the pathological ones. 60s leaves ample room
		// for a capped ancestry walk while stopping a request from pinning a goroutine.
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
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
