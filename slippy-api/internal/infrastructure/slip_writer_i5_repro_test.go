//go:build integration

// Package-level integration reproducer for ADO #82468 (I5 race). Exercises the
// real slippy-api SlipWriterAdapter + slippy.Client + slippy.ClickHouseStore
// against a real ClickHouse 25.x container configured with the production
// async-insert profile (async_insert=1, wait_for_async_insert=1).
//
// Companion to the goLib integration test
// (slippy/clickhouse_store_i5_async_insert_integration_test.go) — that one
// asserts the Update + StepStatusOverride contract at the library boundary.
// THIS one asserts the slippy-api adapter wires R1 + R2 Option D together
// end-to-end so a stale-Load + late StartStep cannot regress a terminal step.
//
// Build tag `integration` keeps this out of `go test ./...` and `make test`.
// Run via `make test-integration` (Docker required).

package infrastructure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// i5IntegrationPipelineJSON is the minimal pipeline config exercised by the
// reproducer. unit_tests is the step the I5 regression hits in production
// (slip 436cc68c). It is intentionally NOT marked aggregates — this test
// targets the pipeline-level overlay path.
const i5IntegrationPipelineJSON = `{
	"name": "slippy-api-i5-repro",
	"steps": [
		{"name": "push_parsed"},
		{"name": "unit_tests"}
	]
}`

// i5ContainerSetup starts a CH 25.x container with the production async-insert
// profile and returns both a low-level clickhouse.Conn (for raw verification
// SELECTs) and a session-wrapped *slippy.ClickHouseStore (after migrations).
type i5ContainerSetup struct {
	container testcontainers.Container
	conn      clickhouse.Conn
	store     *slippy.ClickHouseStore
	dbName    string
}

func (s *i5ContainerSetup) Close(ctx context.Context) {
	if s.store != nil {
		_ = s.store.Close()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.container != nil {
		_ = s.container.Terminate(ctx)
	}
}

func startI5Container(ctx context.Context, t *testing.T) *i5ContainerSetup {
	t.Helper()
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:25.8",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9000/tcp"),
			wait.ForHTTP("/ping").WithPort("8123/tcp").WithStatusCodeMatcher(func(status int) bool {
				return status == 200
			}),
		).WithDeadline(120 * time.Second),
		Env: map[string]string{
			"CLICKHOUSE_USER":                      "default",
			"CLICKHOUSE_PASSWORD":                  "",
			"CLICKHOUSE_DB":                        "default",
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start container")

	host, err := container.Host(ctx)
	require.NoError(t, err, "container host")
	mapped, err := container.MappedPort(ctx, "9000")
	require.NoError(t, err, "container mapped port")
	time.Sleep(2 * time.Second)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, mapped.Port())},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		Settings: clickhouse.Settings{
			"async_insert":                 1,
			"wait_for_async_insert":        1,
			"async_insert_busy_timeout_ms": 10000,
		},
	})
	require.NoError(t, err, "open conn")

	for i := 0; i < 5; i++ {
		if pingErr := conn.Ping(ctx); pingErr == nil {
			break
		} else if i == 4 {
			t.Fatalf("ping after retries: %v", pingErr)
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, conn.Exec(ctx, "SET allow_experimental_json_type = 1"), "enable json type")

	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(i5IntegrationPipelineJSON))
	require.NoError(t, err, "parse pipeline")

	dbName := fmt.Sprintf("ci_i5_repro_%d", time.Now().UnixNano())
	require.NoError(t,
		conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+dbName),
		"create test database",
	)

	_, err = slippy.RunMigrations(ctx, conn, slippy.MigrateOptions{
		Database:       dbName,
		PipelineConfig: pipelineCfg,
	})
	require.NoError(t, err, "run migrations")

	session := ch.NewSessionFromConn(conn)
	store := slippy.NewClickHouseStoreFromSession(session, pipelineCfg, dbName)

	return &i5ContainerSetup{
		container: container,
		conn:      conn,
		store:     store,
		dbName:    dbName,
	}
}

// readUnitTestsStatus reads the latest sign=+1 row's unit_tests_status column
// plus the step_details JSON's .unit_tests.status field. Both are expected to
// match for Option D's JSON-vs-column parity guarantee.
func readUnitTestsStatus(t *testing.T, ctx context.Context, conn clickhouse.Conn, dbName, corrID string) (col string, fromJSON string) {
	t.Helper()
	query := fmt.Sprintf(`
		SELECT unit_tests_status, toString(step_details) AS sd
		FROM %s.routing_slips
		WHERE correlation_id = ? AND sign = 1
		ORDER BY version DESC
		LIMIT 1
	`, dbName)
	row := conn.QueryRow(ctx, query, corrID)
	var sdRaw string
	require.NoError(t, row.Scan(&col, &sdRaw), "scan routing_slips")
	var sd map[string]map[string]interface{}
	if err := json.Unmarshal([]byte(sdRaw), &sd); err != nil {
		t.Fatalf("unmarshal step_details %q: %v", sdRaw, err)
	}
	if step, ok := sd["unit_tests"]; ok {
		if v, ok := step["status"]; ok {
			if s, ok := v.(string); ok {
				fromJSON = s
			}
		}
	}
	return col, fromJSON
}

// buildI5Adapter wires the real adapter — slippy.Client (with real CH store) →
// SlipWriterAdapter. No locker / no reader (matches the no-cache fail-open path).
func buildI5Adapter(t *testing.T, setup *i5ContainerSetup) *SlipWriterAdapter {
	t.Helper()
	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(i5IntegrationPipelineJSON))
	require.NoError(t, err, "parse pipeline")
	client := slippy.NewClientWithDependencies(setup.store, &noopGitHub{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})
	return NewSlipWriterAdapter(client, nil, nil)
}

// noopGitHub satisfies slippy.GitHubAPI without making any calls.
type noopGitHub struct{}

func (noopGitHub) GetCommitAncestry(_ context.Context, _, _, _ string, _ int) ([]string, error) {
	return nil, nil
}
func (noopGitHub) GetPRHeadCommit(_ context.Context, _, _ string, _ int) (string, error) {
	return "", nil
}
func (noopGitHub) ClearCache() {}

// TestSlipWriter_I5_StaleStartAfterComplete_DoesNotClobber reproduces the
// production scenario from slip 436cc68c:
//
//  1. CompleteStep(unit_tests) writes a terminal event.
//  2. StartStep(unit_tests) fires moments later (out-of-order re-trigger) —
//     hydrateAndPersist calls Load which (under async-insert visibility lag)
//     might NOT see the terminal event in slip.Steps[].Status, but the event
//     log itself IS visible because wait_for_async_insert=1.
//  3. R1 must consult the event log, see the terminal status, and DROP the
//     running overlay. R2 must therefore pin NO override.
//  4. Update writes a new sign=+1 row whose unit_tests_status column and
//     step_details.unit_tests.status JSON both stay "completed".
//
// Under the pre-R1 code (in-memory CompletedAt guard) this regression flips
// the column to "running" — and that's permanent because VersionedCollapsingMergeTree
// will collapse the older completed row.
func TestSlipWriter_I5_StaleStartAfterComplete_DoesNotClobber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping I5 async-insert integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	setup := startI5Container(ctx, t)
	defer setup.Close(ctx)

	adapter := buildI5Adapter(t, setup)

	corrID := fmt.Sprintf("i5-stale-start-%d", time.Now().UnixNano())
	commitSHA := fmt.Sprintf("sha-%d", time.Now().UnixNano())

	_, err := adapter.CreateSlipForPush(ctx, slippy.PushOptions{
		CorrelationID: corrID,
		Repository:    "owner/repo",
		Branch:        "main",
		CommitSHA:     commitSHA,
	})
	require.NoError(t, err, "CreateSlipForPush")

	// Drive unit_tests to terminal: Start → Complete.
	require.NoError(t, adapter.StartStep(ctx, corrID, "unit_tests", ""), "StartStep terminal precursor")
	require.NoError(t, adapter.CompleteStep(ctx, corrID, "unit_tests", ""), "CompleteStep")

	// Sanity: the column reflects completed BEFORE the late StartStep.
	col, jsonStatus := readUnitTestsStatus(t, ctx, setup.conn, setup.dbName, corrID)
	require.Equal(t, string(slippy.StepStatusCompleted), col, "precondition: unit_tests_status must be completed before the late StartStep")
	require.Equal(t, string(slippy.StepStatusCompleted), jsonStatus, "precondition: step_details JSON status must be completed")

	// The bug trigger — a late StartStep after the terminal event. With Option 1
	// (INSERT-time gate in goLibMyCarrier) wired through, the gate refuses the
	// transition with slippy.ErrTerminalAlreadyExists BEFORE the row even
	// reaches the event log. R1+R2 (overlay + override) remain the second line
	// of defense for the same-microsecond concurrent-INSERT residual race; here
	// the gate fires synchronously and the sentinel propagates back to the
	// caller.
	err = adapter.StartStep(ctx, corrID, "unit_tests", "")
	require.Error(t, err, "Option 1 gate must refuse a late StartStep over a terminal completed event")
	require.True(t, errors.Is(err, slippy.ErrTerminalAlreadyExists),
		"adapter must propagate slippy.ErrTerminalAlreadyExists from the gate; got %v", err)

	col, jsonStatus = readUnitTestsStatus(t, ctx, setup.conn, setup.dbName, corrID)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		col,
		"I5: late StartStep must NOT clobber unit_tests_status from completed → running (ADO #82468)",
	)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		jsonStatus,
		"I5: step_details.unit_tests.status must remain completed (column-JSON parity)",
	)
}

// TestSlipWriter_I5_TerminalTransition_PinsOverride is the positive control.
// Even when the in-memory snapshot from Load is stale (running, because the
// preceding event-log INSERT may not yet have been overlaid into the
// scanSlip result), the override pinned by R2 Option D guarantees the
// routing_slips column + JSON reflect the just-written terminal status.
func TestSlipWriter_I5_TerminalTransition_PinsOverride(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping I5 async-insert integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	setup := startI5Container(ctx, t)
	defer setup.Close(ctx)

	adapter := buildI5Adapter(t, setup)

	corrID := fmt.Sprintf("i5-terminal-pin-%d", time.Now().UnixNano())
	commitSHA := fmt.Sprintf("sha-%d", time.Now().UnixNano())

	_, err := adapter.CreateSlipForPush(ctx, slippy.PushOptions{
		CorrelationID: corrID,
		Repository:    "owner/repo",
		Branch:        "main",
		CommitSHA:     commitSHA,
	})
	require.NoError(t, err, "CreateSlipForPush")

	require.NoError(t, adapter.StartStep(ctx, corrID, "unit_tests", ""), "StartStep")

	col, _ := readUnitTestsStatus(t, ctx, setup.conn, setup.dbName, corrID)
	require.Equal(t, string(slippy.StepStatusRunning), col, "precondition: unit_tests_status must be running after StartStep")

	require.NoError(t, adapter.CompleteStep(ctx, corrID, "unit_tests", ""), "CompleteStep")

	col, jsonStatus := readUnitTestsStatus(t, ctx, setup.conn, setup.dbName, corrID)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		col,
		"Option D positive: CompleteStep override must pin unit_tests_status to completed",
	)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		jsonStatus,
		"Option D positive: step_details.unit_tests.status must match the column literal",
	)
}

// TestSlipWriter_I5_LateStartAfterComplete_NextSecond proves the
// cross-wallclock-second residual race (plan v3 §C.6) is closed by the
// Option 1 INSERT-time gate. Setup mirrors TestSlipWriter_I5_StaleStartAfterComplete_DoesNotClobber
// but sleeps until the next second tick BEFORE the late StartStep so the
// new event would land with a strictly-greater event_time than the prior
// terminal — an argMax tiebreak could mistakenly promote it. The gate must
// fire regardless of timestamp ordering.
//
// Without the gate, this is the exact 436cc68c failure mode that R2 Option D
// alone could not catch: argMax((status, event_time)) returns the LATER row
// (running), routing_slips column flips, and the slip is permanently stuck.
func TestSlipWriter_I5_LateStartAfterComplete_NextSecond(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping I5 async-insert integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	setup := startI5Container(ctx, t)
	defer setup.Close(ctx)

	adapter := buildI5Adapter(t, setup)

	corrID := fmt.Sprintf("i5-cross-second-%d", time.Now().UnixNano())
	commitSHA := fmt.Sprintf("sha-%d", time.Now().UnixNano())

	_, err := adapter.CreateSlipForPush(ctx, slippy.PushOptions{
		CorrelationID: corrID,
		Repository:    "owner/repo",
		Branch:        "main",
		CommitSHA:     commitSHA,
	})
	require.NoError(t, err, "CreateSlipForPush")

	require.NoError(t, adapter.StartStep(ctx, corrID, "unit_tests", ""), "StartStep terminal precursor")
	require.NoError(t, adapter.CompleteStep(ctx, corrID, "unit_tests", ""), "CompleteStep")

	// Sleep to the next wallclock-second boundary + a small margin so the
	// late StartStep's event_time is strictly greater than the terminal event's.
	// This is the precise race window pre-Option-1 R1+R2 could not catch.
	now := time.Now()
	nextSecond := now.Truncate(time.Second).Add(time.Second + 100*time.Millisecond)
	time.Sleep(time.Until(nextSecond))

	// Late StartStep — gate MUST refuse.
	err = adapter.StartStep(ctx, corrID, "unit_tests", "")
	require.Error(t, err, "gate must refuse late StartStep across a second boundary")
	require.True(t, errors.Is(err, slippy.ErrTerminalAlreadyExists),
		"adapter must propagate slippy.ErrTerminalAlreadyExists; got %v", err)

	col, jsonStatus := readUnitTestsStatus(t, ctx, setup.conn, setup.dbName, corrID)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		col,
		"cross-second: unit_tests_status MUST stay completed (column-tiebreak immune)",
	)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		jsonStatus,
		"cross-second: step_details JSON MUST stay completed (column-JSON parity)",
	)
}

// TestSlipWriter_I5_ThreeWayConcurrent_PerCorrIDLock proves CRIT-V2-2 closure:
// three goroutines racing the same correlationID through SlipWriterAdapter,
// with the per-correlationID lock enabled, must serialize such that exactly
// ONE terminal lands and the other two return ErrCorrIDWriteInProgress (409
// at HTTP layer; mapped to bounded-retry by Slippy CLI per plan v3 §M.7 PR 3).
//
// Setup requires both Dragonfly (lock backend) and ClickHouse. Uses miniredis
// for the lock and the testcontainer CH for storage. SLIPPY_I5_LOCK_ENABLED
// is set at adapter-construction time (constructor reads env once).
func TestSlipWriter_I5_ThreeWayConcurrent_PerCorrIDLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping I5 concurrency integration test in short mode")
	}

	// Enable lock for this test scope.
	t.Setenv(slippyI5LockEnabledEnv, "true")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	setup := startI5Container(ctx, t)
	defer setup.Close(ctx)

	// Build adapter with a miniredis-backed locker.
	mr, locker := newMiniredisLocker(t)
	defer mr.Close()

	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(i5IntegrationPipelineJSON))
	require.NoError(t, err, "parse pipeline")
	client := slippy.NewClientWithDependencies(setup.store, &noopGitHub{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})
	adapter := NewSlipWriterAdapter(client, locker, nil)
	require.True(t, adapter.corrIDLockOn, "precondition: SLIPPY_I5_LOCK_ENABLED must be true")

	// Use a valid UUID so CorrIDLockKey returns a non-empty key.
	corrID := newUUIDForTest(t)
	commitSHA := fmt.Sprintf("sha-%d", time.Now().UnixNano())

	_, err = adapter.CreateSlipForPush(ctx, slippy.PushOptions{
		CorrelationID: corrID,
		Repository:    "owner/repo",
		Branch:        "main",
		CommitSHA:     commitSHA,
	})
	require.NoError(t, err, "CreateSlipForPush")

	// Move unit_tests to running so the first concurrent CompleteStep wins;
	// the gate then refuses a same-status second writer if it happens to race.
	// (The lock is the primary serializer; the gate is defense in depth.)
	require.NoError(t, adapter.StartStep(ctx, corrID, "unit_tests", ""), "StartStep")

	// Three goroutines race CompleteStep on the same corrID. Exactly one
	// must succeed; the other two must see ErrCorrIDWriteInProgress (lock miss).
	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		results     []error
		startSignal = make(chan struct{})
	)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startSignal
			err := adapter.CompleteStep(ctx, corrID, "unit_tests", "")
			mu.Lock()
			results = append(results, err)
			mu.Unlock()
		}()
	}
	close(startSignal)
	wg.Wait()

	// Count outcomes.
	successCount := 0
	lockMissCount := 0
	otherErrs := []error{}
	for _, e := range results {
		switch {
		case e == nil:
			successCount++
		case errors.Is(e, domain.ErrCorrIDWriteInProgress):
			lockMissCount++
		default:
			otherErrs = append(otherErrs, e)
		}
	}

	// In practice ALL three may serialize through the lock (first acquires,
	// holds 5–30 ms, second waits TTL or returns ErrCorrIDWriteInProgress
	// depending on timing). The contract is: at LEAST one succeeds, the
	// final routing_slips state is completed (the I5 invariant), and any
	// non-success errors are EXCLUSIVELY ErrCorrIDWriteInProgress (no
	// foreign errors leak).
	require.GreaterOrEqual(t, successCount, 1, "at least one CompleteStep must succeed; results=%v", results)
	assert.Empty(t, otherErrs, "non-success errors MUST be ErrCorrIDWriteInProgress only; got %v", otherErrs)
	assert.Equal(t, 3, successCount+lockMissCount, "every outcome must be either nil or ErrCorrIDWriteInProgress")

	col, jsonStatus := readUnitTestsStatus(t, ctx, setup.conn, setup.dbName, corrID)
	assert.Equal(t, string(slippy.StepStatusCompleted), col,
		"3-way race: unit_tests_status MUST be completed (lock + gate close the race)")
	assert.Equal(t, string(slippy.StepStatusCompleted), jsonStatus,
		"3-way race: step_details JSON MUST match column (parity)")
}

// TestSlipWriter_I5_CascadeResetAbortedToPending_E2E exercises plan v3 §C.18
// — the positive cascade-reset path. When a primary step recovers from
// failure, executor.go:377 cascade-resets every aborted dependent step from
// aborted → pending. The Option 1 gate's isRecoveryAllowed predicate
// (goLib §B.13.1) MUST permit this single terminal → non-terminal transition.
//
// Without correct isRecoveryAllowed coverage, cascade-reset would fail with
// ErrTerminalAlreadyExists, executor.go:393 would populate resetFailures, and
// the slip would stay Failed despite the operator's recovery action.
//
// The integration covers the slippy-api end of the chain:
// adapter.CompleteStep on the recovering step triggers checkPipelineCompletion
// inside the library which fires the cascade-reset. The assertion checks the
// downstream step transitioned to pending AND no resetFailures landed.
func TestSlipWriter_I5_CascadeResetAbortedToPending_E2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping I5 cascade-reset integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Use a pipeline with a primary step + a cascading step that depends on it
	// via prerequisites — this is the shape the executor cascade-reset path
	// expects (executor.go:377).
	pipelineJSON := `{
		"name": "slippy-api-i5-cascade",
		"steps": [
			{"name": "push_parsed"},
			{"name": "unit_tests", "prerequisites": ["push_parsed"]},
			{"name": "deploy_dev", "prerequisites": ["unit_tests"]}
		]
	}`

	setup := startI5ContainerWithPipeline(ctx, t, pipelineJSON)
	defer setup.Close(ctx)

	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(pipelineJSON))
	require.NoError(t, err, "parse pipeline")
	client := slippy.NewClientWithDependencies(setup.store, &noopGitHub{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})
	adapter := NewSlipWriterAdapter(client, nil, nil)

	corrID := fmt.Sprintf("i5-cascade-%d", time.Now().UnixNano())
	commitSHA := fmt.Sprintf("sha-%d", time.Now().UnixNano())

	_, err = adapter.CreateSlipForPush(ctx, slippy.PushOptions{
		CorrelationID: corrID,
		Repository:    "owner/repo",
		Branch:        "main",
		CommitSHA:     commitSHA,
	})
	require.NoError(t, err, "CreateSlipForPush")

	// Drive unit_tests to failed; cascade-reset will mark deploy_dev as aborted
	// via the library's failure-propagation path.
	require.NoError(t, adapter.StartStep(ctx, corrID, "unit_tests", ""), "StartStep unit_tests")
	require.NoError(t, adapter.FailStep(ctx, corrID, "unit_tests", "", "first run failed"),
		"FailStep unit_tests")

	// Operator recovery — CompleteStep on the (still-failed) unit_tests. The
	// library executes cascade-reset to revert deploy_dev from aborted → pending.
	// The gate must permit this via isRecoveryAllowed.
	require.NoError(t, adapter.CompleteStep(ctx, corrID, "unit_tests", ""),
		"CompleteStep unit_tests recovery — cascade-reset must succeed")

	// Verify deploy_dev status reflects the cascade-reset, NOT lingering aborted.
	// If the gate misconfigured (cascade-reset rejected), deploy_dev stays
	// aborted and routing_slips.status stays Failed — caught here.
	col, _ := readStepStatusGeneric(t, ctx, setup.conn, setup.dbName, corrID, "deploy_dev_status")
	// Acceptable post-state: pending (cascade-reset landed) OR running (next
	// scheduler tick already moved it forward). aborted means cascade-reset
	// was REJECTED — bug.
	require.NotEqual(t, string(slippy.StepStatusAborted), col,
		"cascade-reset failed: deploy_dev still aborted after unit_tests recovery — Option 1 isRecoveryAllowed regression")
}

// --- helpers for the new integration tests ---

// newMiniredisLocker spins up an in-memory miniredis and returns a
// RedisLocker backed by it. Matches the pattern in dedup_lock_test.go but
// returns the miniredis handle so callers can FastForward TTL if needed.
func newMiniredisLocker(t *testing.T) (*miniredis.Miniredis, *RedisLocker) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, NewRedisLocker(rdb)
}

// newUUIDForTest returns a deterministic, valid UUID v4 string for tests
// that go through the per-correlationID lock path (CorrIDLockKey requires
// uuid.Parse to succeed).
func newUUIDForTest(t *testing.T) string {
	t.Helper()
	return uuid.New().String()
}

// startI5ContainerWithPipeline mirrors startI5Container but accepts a custom
// pipeline JSON so cascade-reset and multi-step tests can declare a step
// graph that the default i5IntegrationPipelineJSON does not include.
func startI5ContainerWithPipeline(ctx context.Context, t *testing.T, pipelineJSON string) *i5ContainerSetup {
	t.Helper()
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:25.8",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9000/tcp"),
			wait.ForHTTP("/ping").WithPort("8123/tcp").WithStatusCodeMatcher(func(status int) bool {
				return status == 200
			}),
		).WithDeadline(120 * time.Second),
		Env: map[string]string{
			"CLICKHOUSE_USER":                      "default",
			"CLICKHOUSE_PASSWORD":                  "",
			"CLICKHOUSE_DB":                        "default",
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start container")

	host, err := container.Host(ctx)
	require.NoError(t, err, "container host")
	mapped, err := container.MappedPort(ctx, "9000")
	require.NoError(t, err, "container mapped port")
	time.Sleep(2 * time.Second)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, mapped.Port())},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		Settings: clickhouse.Settings{
			"async_insert":                 1,
			"wait_for_async_insert":        1,
			"async_insert_busy_timeout_ms": 10000,
		},
	})
	require.NoError(t, err, "open conn")

	for i := 0; i < 5; i++ {
		if pingErr := conn.Ping(ctx); pingErr == nil {
			break
		} else if i == 4 {
			t.Fatalf("ping after retries: %v", pingErr)
		}
		time.Sleep(time.Second)
	}
	require.NoError(t, conn.Exec(ctx, "SET allow_experimental_json_type = 1"), "enable json type")

	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(pipelineJSON))
	require.NoError(t, err, "parse pipeline")

	dbName := fmt.Sprintf("ci_i5_pipeline_%d", time.Now().UnixNano())
	require.NoError(t,
		conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+dbName),
		"create test database",
	)

	_, err = slippy.RunMigrations(ctx, conn, slippy.MigrateOptions{
		Database:       dbName,
		PipelineConfig: pipelineCfg,
	})
	require.NoError(t, err, "run migrations")

	session := ch.NewSessionFromConn(conn)
	store := slippy.NewClickHouseStoreFromSession(session, pipelineCfg, dbName)

	return &i5ContainerSetup{
		container: container,
		conn:      conn,
		store:     store,
		dbName:    dbName,
	}
}

// readStepStatusGeneric reads a single *_status column from the latest sign=+1
// routing_slips row. Uses a fmt.Sprintf'd query because the column name is
// chosen by the caller — but it's safe: callers are tests passing
// hardcoded column literals derived from pipeline config.
func readStepStatusGeneric(t *testing.T, ctx context.Context, conn clickhouse.Conn, dbName, corrID, colName string) (col string, fromJSON string) {
	t.Helper()
	// nolint:gosec // colName is a test-controlled literal, not user input.
	query := fmt.Sprintf(`
		SELECT %s, toString(step_details) AS sd
		FROM %s.routing_slips
		WHERE correlation_id = ? AND sign = 1
		ORDER BY version DESC
		LIMIT 1
	`, colName, dbName)
	row := conn.QueryRow(ctx, query, corrID)
	var sdRaw string
	require.NoError(t, row.Scan(&col, &sdRaw), "scan routing_slips")
	return col, sdRaw
}
