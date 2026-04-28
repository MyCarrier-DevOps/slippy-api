package infrastructure

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

const automationTestsTracerName = "slippy-api/automation-tests"

// testResultsSelectColumns is the column list for TestResults queries.
const testResultsSelectColumns = `Feature, TestName, ResultMessage, ResultStatus, Duration, Description,
	ScenarioInfoTitle, ScenarioInfoDescription, ScenarioInfoTags,
	ScenarioExecutionStatus, StackTrace, ReleaseId, StackName, Stage,
	EnvironmentName, Attempt, StartTime, BranchName, TestId`

// AutomationTestsStore queries autotest_results.TestResults from ClickHouse.
type AutomationTestsStore struct {
	session ch.ClickhouseSessionInterface
}

// NewAutomationTestsStore creates an AutomationTestsReader backed by ClickHouse.
func NewAutomationTestsStore(session ch.ClickhouseSessionInterface) *AutomationTestsStore {
	return &AutomationTestsStore{session: session}
}

var _ domain.AutomationTestsReader = (*AutomationTestsStore)(nil)

// QueryTests fetches individual test rows for a set of resolved runs. The
// query is bounded by [MinStart, MaxFinish] to give ClickHouse partition
// pruning on TestResults (PARTITION BY toDate(StartTime)). When Runs is empty,
// the function returns an empty result without hitting ClickHouse.
func (s *AutomationTestsStore) QueryTests(
	ctx context.Context,
	q *domain.AutomationTestsQuery,
) (result *domain.AutomationTestsResult, err error) {
	if q == nil {
		return nil, errors.New("query must not be nil")
	}
	if q.Limit < 1 {
		return nil, errors.New("limit must be at least 1")
	}
	if len(q.Runs) == 0 {
		return &domain.AutomationTestsResult{Tests: nil, Count: 0}, nil
	}

	ctx, span := otel.Tracer(automationTestsTracerName).Start(ctx, "automationtests.QueryTests",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.operation", "QueryTests"),
			attribute.Int("test.runs_count", len(q.Runs)),
			attribute.String("test.status", q.Status),
			attribute.Int("test.limit", q.Limit),
		),
	)
	defer span.End()

	conditions := []string{
		"StartTime >= {minStart:DateTime}",
		"StartTime <= {maxFinish:DateTime}",
	}
	args := []any{
		ch.Named("minStart", q.MinStart),
		ch.Named("maxFinish", q.MaxFinish),
	}

	tupleClauses := make([]string, 0, len(q.Runs))
	for i, run := range q.Runs {
		rid := fmt.Sprintf("r%d", i)
		att := fmt.Sprintf("a%d", i)
		stg := fmt.Sprintf("s%d", i)
		env := fmt.Sprintf("e%d", i)
		stk := fmt.Sprintf("k%d", i)
		tupleClauses = append(tupleClauses, fmt.Sprintf(
			"(ReleaseId = {%s:String} AND Attempt = {%s:UInt8} "+
				"AND Stage ILIKE {%s:String} AND EnvironmentName ILIKE {%s:String} AND StackName ILIKE {%s:String})",
			rid, att, stg, env, stk,
		))
		args = append(args,
			ch.Named(rid, run.ReleaseID),
			ch.Named(att, run.Attempt),
			ch.Named(stg, run.Stage),
			ch.Named(env, run.EnvironmentName),
			ch.Named(stk, run.StackName),
		)
	}
	conditions = append(conditions, "("+strings.Join(tupleClauses, " OR ")+")")

	if q.Status != "" {
		conditions = append(conditions, "ResultStatus ILIKE {status:String}")
		args = append(args, ch.Named("status", q.Status))
	}

	if q.Cursor != "" {
		cursorTime, cursorID, parseErr := parseTestsCursor(q.Cursor)
		if parseErr != nil {
			span.RecordError(parseErr)
			span.SetStatus(codes.Error, "invalid cursor")
			return nil, fmt.Errorf("%w: %w", domain.ErrInvalidTestsCursor, parseErr)
		}
		conditions = append(conditions,
			"(StartTime > {cursorTime:DateTime} OR (StartTime = {cursorTime:DateTime} AND TestId > {cursorId:UUID}))",
		)
		args = append(args,
			ch.Named("cursorTime", cursorTime),
			ch.Named("cursorId", cursorID),
		)
	}

	fetchLimit := q.Limit + 1
	args = append(args, ch.Named("fetchLimit", uint32(fetchLimit)))

	query := fmt.Sprintf(
		`SELECT %s
		FROM autotest_results.TestResults
		WHERE %s
		ORDER BY StartTime ASC, TestId ASC
		LIMIT {fetchLimit:UInt32}`,
		testResultsSelectColumns,
		strings.Join(conditions, " AND "),
	)

	rows, err := s.session.QueryWithArgs(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("query failed: %v", err))
		return nil, fmt.Errorf("failed to query automation tests: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	tests := make([]domain.AutomationTestResult, 0, q.Limit)
	for rows.Next() {
		var (
			row    domain.AutomationTestResult
			testID uuid.UUID
		)
		if scanErr := rows.Scan(
			&row.Feature, &row.TestName, &row.ResultMessage, &row.ResultStatus, &row.Duration,
			&row.Description, &row.ScenarioInfoTitle, &row.ScenarioInfoDescription,
			&row.ScenarioInfoTags, &row.ScenarioExecutionStatus, &row.StackTrace,
			&row.ReleaseID, &row.StackName, &row.Stage, &row.EnvironmentName,
			&row.Attempt, &row.StartTime, &row.BranchName, &testID,
		); scanErr != nil {
			return nil, fmt.Errorf("failed to scan automation tests row: %w", scanErr)
		}
		row.TestID = testID.String()
		tests = append(tests, row)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("error iterating automation tests rows: %w", rowsErr)
	}

	result = &domain.AutomationTestsResult{}
	if len(tests) > q.Limit {
		tests = tests[:q.Limit]
		last := tests[len(tests)-1]
		result.NextCursor = encodeTestsCursor(last.StartTime, last.TestID)
	}
	result.Tests = tests
	result.Count = len(tests)

	span.SetAttributes(attribute.Int("test.result_count", result.Count))
	span.SetStatus(codes.Ok, "")
	return result, nil
}

// encodeTestsCursor produces a composite cursor string "RFC3339Nano|UUID".
func encodeTestsCursor(ts time.Time, testID string) string {
	return ts.Format(time.RFC3339Nano) + "|" + testID
}

// parseTestsCursor splits a "RFC3339Nano|UUID" cursor into its components.
func parseTestsCursor(cursor string) (time.Time, uuid.UUID, error) {
	parts := strings.SplitN(cursor, "|", 2)
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, fmt.Errorf("cursor must be in 'timestamp|uuid' format")
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor uuid: %w", err)
	}
	return ts, id, nil
}
