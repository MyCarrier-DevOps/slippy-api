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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
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
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

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

	// The bug trigger — a late StartStep after the terminal event. With R1 +
	// R2 Option D wired through, the overlay is dropped AND no override is
	// pinned, so the column stays completed.
	require.NoError(t, adapter.StartStep(ctx, corrID, "unit_tests", ""), "StartStep AFTER CompleteStep (the I5 trigger)")

	col, jsonStatus = readUnitTestsStatus(t, ctx, setup.conn, setup.dbName, corrID)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		col,
		"I5 R1 regression: late StartStep must NOT clobber unit_tests_status from completed → running (ADO #82468)",
	)
	assert.Equal(
		t,
		string(slippy.StepStatusCompleted),
		jsonStatus,
		"I5 R2 Option D regression: step_details.unit_tests.status must remain completed (column-JSON parity)",
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
