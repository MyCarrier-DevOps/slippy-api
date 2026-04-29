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

// testsLookbackDays bounds the StartTime predicate used in every
// TestResultsCor query so ClickHouse can prune partitions
// (PARTITION BY toDate(StartTime)). Anything older than this is invisible
// to these endpoints, even if still within the 3-month TTL.
const testsLookbackDays = 14

// testResultsCorFullColumns selects every column we surface from
// TestResultsCor, used by the single-test drill-down (LoadTestByCorrelation).
const testResultsCorFullColumns = `Feature, TestName, ResultMessage, ResultStatus, Duration, Description,
	ScenarioInfoTitle, ScenarioInfoDescription, ScenarioInfoTags,
	ScenarioExecutionStatus, StackTrace, ReleaseId, StackName, Stage,
	EnvironmentName, Attempt, StartTime, BranchName, TestId, CorrelationId`

// testResultsCorListColumns is the same set minus StackTrace. The list
// endpoint can return many rows and stack traces can be very large; callers
// who want a trace fetch one row at a time via LoadTestByCorrelation.
const testResultsCorListColumns = `Feature, TestName, ResultMessage, ResultStatus, Duration, Description,
	ScenarioInfoTitle, ScenarioInfoDescription, ScenarioInfoTags,
	ScenarioExecutionStatus, ReleaseId, StackName, Stage,
	EnvironmentName, Attempt, StartTime, BranchName, TestId, CorrelationId`

// chDateTimeFormat is the ClickHouse-native DateTime literal format. We bind
// time values as strings + cast via toDateTime() because the clickhouse-go
// driver wraps DateTime-typed parameters with `toDateTime('...')` literals
// that CH's parameter parser refuses (CH error 41).
const chDateTimeFormat = "2006-01-02 15:04:05"

// formatCHDateTime renders a time as the literal string toDateTime() expects.
func formatCHDateTime(t time.Time) string { return t.UTC().Format(chDateTimeFormat) }

// AutomationTestsStore queries autotest_results.TestResultsCor from ClickHouse.
type AutomationTestsStore struct {
	session ch.ClickhouseSessionInterface
}

// NewAutomationTestsStore creates an AutomationTestsReader backed by ClickHouse.
func NewAutomationTestsStore(session ch.ClickhouseSessionInterface) *AutomationTestsStore {
	return &AutomationTestsStore{session: session}
}

var _ domain.AutomationTestsReader = (*AutomationTestsStore)(nil)

// startTimeLookback is the partition-pruning predicate used by every
// TestResultsCor query.
func startTimeLookback() string {
	return fmt.Sprintf("StartTime >= now() - INTERVAL %d DAY", testsLookbackDays)
}

// appendTestRowFilters appends the optional environment / stack / stage /
// attempt filters for a TestResultsCor query.
func appendTestRowFilters(
	inConditions []string,
	inArgs []any,
	environment, stack, stage string,
	attempt uint8,
) (conditions []string, args []any) {
	conditions, args = inConditions, inArgs
	if environment != "" {
		conditions = append(conditions, "EnvironmentName ILIKE {environment:String}")
		args = append(args, ch.Named("environment", environment))
	}
	if stack != "" {
		conditions = append(conditions, "StackName ILIKE {stack:String}")
		args = append(args, ch.Named("stack", stack))
	}
	if stage != "" {
		conditions = append(conditions, "Stage ILIKE {stage:String}")
		args = append(args, ch.Named("stage", stage))
	}
	if attempt > 0 {
		conditions = append(conditions, "Attempt = {attempt:UInt8}")
		args = append(args, ch.Named("attempt", attempt))
	}
	return conditions, args
}

// scanTestResultFullRow reads one TestResultsCor row including StackTrace,
// matching testResultsCorFullColumns.
func scanTestResultFullRow(rows ch.Rows) (domain.AutomationTestResult, error) {
	var (
		row    domain.AutomationTestResult
		testID uuid.UUID
		corrID *uuid.UUID
	)
	if err := rows.Scan(
		&row.Feature, &row.TestName, &row.ResultMessage, &row.ResultStatus, &row.Duration,
		&row.Description, &row.ScenarioInfoTitle, &row.ScenarioInfoDescription,
		&row.ScenarioInfoTags, &row.ScenarioExecutionStatus, &row.StackTrace,
		&row.ReleaseID, &row.StackName, &row.Stage, &row.EnvironmentName,
		&row.Attempt, &row.StartTime, &row.BranchName, &testID, &corrID,
	); err != nil {
		return row, err
	}
	row.TestID = testID.String()
	if corrID != nil {
		s := corrID.String()
		row.CorrelationID = &s
	}
	return row, nil
}

// scanTestResultListRow reads one TestResultsCor row without StackTrace,
// matching testResultsCorListColumns. The list endpoint omits the trace to
// keep responses compact; callers fetch the trace via LoadTestByCorrelation.
func scanTestResultListRow(rows ch.Rows) (domain.AutomationTestResult, error) {
	var (
		row    domain.AutomationTestResult
		testID uuid.UUID
		corrID *uuid.UUID
	)
	if err := rows.Scan(
		&row.Feature, &row.TestName, &row.ResultMessage, &row.ResultStatus, &row.Duration,
		&row.Description, &row.ScenarioInfoTitle, &row.ScenarioInfoDescription,
		&row.ScenarioInfoTags, &row.ScenarioExecutionStatus,
		&row.ReleaseID, &row.StackName, &row.Stage, &row.EnvironmentName,
		&row.Attempt, &row.StartTime, &row.BranchName, &testID, &corrID,
	); err != nil {
		return row, err
	}
	row.TestID = testID.String()
	if corrID != nil {
		s := corrID.String()
		row.CorrelationID = &s
	}
	return row, nil
}

// QueryTestsByCorrelation returns TestResultsCor rows scoped to the given
// correlation ID, with optional environment/stack/stage/attempt/status
// filters and cursor-based pagination. Results are bounded by a 14-day
// lookback so ClickHouse can prune partitions.
func (s *AutomationTestsStore) QueryTestsByCorrelation(
	ctx context.Context,
	q *domain.AutomationTestsByCorrelationQuery,
) (result *domain.AutomationTestsResult, err error) {
	if q == nil {
		return nil, errors.New("query must not be nil")
	}
	if q.CorrelationID == uuid.Nil {
		return nil, errors.New("correlation ID is required")
	}
	if q.Limit < 1 {
		return nil, errors.New("limit must be at least 1")
	}

	ctx, span := otel.Tracer(automationTestsTracerName).Start(ctx, "automationtests.QueryByCorrelation",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.operation", "QueryTestsByCorrelation"),
			attribute.String("test.correlation_id", q.CorrelationID.String()),
			attribute.String("test.status", q.Status),
			attribute.Int("test.limit", q.Limit),
		),
	)
	defer span.End()

	// CorrelationId/TestId/DateTime are bound as String + cast in SQL because
	// clickhouse-go wraps these typed parameters in literals that CH's
	// parameter parser refuses (CH errors 457 and 41).
	conditions := []string{
		startTimeLookback(),
		"CorrelationId = toUUID({correlationId:String})",
	}
	args := []any{ch.Named("correlationId", q.CorrelationID.String())}
	conditions, args = appendTestRowFilters(conditions, args, q.Environment, q.Stack, q.Stage, q.Attempt)

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
			"(StartTime > toDateTime({cursorTime:String}) OR "+
				"(StartTime = toDateTime({cursorTime:String}) AND TestId > toUUID({cursorId:String})))",
		)
		args = append(args,
			ch.Named("cursorTime", formatCHDateTime(cursorTime)),
			ch.Named("cursorId", cursorID.String()),
		)
	}

	fetchLimit := q.Limit + 1
	args = append(args, ch.Named("fetchLimit", uint32(fetchLimit)))

	query := fmt.Sprintf(
		`SELECT %s
		FROM autotest_results.TestResultsCor
		WHERE %s
		ORDER BY StartTime ASC, TestId ASC
		LIMIT {fetchLimit:UInt32}`,
		testResultsCorListColumns,
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
		row, scanErr := scanTestResultListRow(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("failed to scan automation tests row: %w", scanErr)
		}
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

// LoadTestByCorrelation fetches a single TestResultsCor row by TestId,
// scoped to a CorrelationId so a TestId from an unrelated slip can't be
// returned. Returns domain.ErrTestNotFound when no row matches.
func (s *AutomationTestsStore) LoadTestByCorrelation(
	ctx context.Context,
	q *domain.LoadTestByCorrelationQuery,
) (result *domain.AutomationTestResult, err error) {
	if q == nil {
		return nil, errors.New("query must not be nil")
	}
	if q.CorrelationID == uuid.Nil {
		return nil, errors.New("correlation ID is required")
	}
	if q.TestID == uuid.Nil {
		return nil, errors.New("test ID is required")
	}

	ctx, span := otel.Tracer(automationTestsTracerName).Start(ctx, "automationtests.LoadByCorrelation",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.operation", "LoadTestByCorrelation"),
			attribute.String("test.correlation_id", q.CorrelationID.String()),
			attribute.String("test.test_id", q.TestID.String()),
		),
	)
	defer span.End()

	conditions := []string{
		startTimeLookback(),
		"CorrelationId = toUUID({correlationId:String})",
		"TestId = toUUID({testId:String})",
	}
	args := []any{
		ch.Named("correlationId", q.CorrelationID.String()),
		ch.Named("testId", q.TestID.String()),
	}

	query := fmt.Sprintf(
		`SELECT %s
		FROM autotest_results.TestResultsCor
		WHERE %s
		LIMIT 1`,
		testResultsCorFullColumns,
		strings.Join(conditions, " AND "),
	)

	rows, err := s.session.QueryWithArgs(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("query failed: %v", err))
		return nil, fmt.Errorf("failed to load automation test: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			return nil, fmt.Errorf("error iterating automation test rows: %w", rowsErr)
		}
		span.SetStatus(codes.Ok, "not found")
		return nil, domain.ErrTestNotFound
	}

	row, scanErr := scanTestResultFullRow(rows)
	if scanErr != nil {
		return nil, fmt.Errorf("failed to scan automation test row: %w", scanErr)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("error iterating automation test rows: %w", rowsErr)
	}

	span.SetStatus(codes.Ok, "")
	return &row, nil
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
