package infrastructure

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

const automationTestResultsTracerName = "slippy-api/automation-testresults"

// runResultsSelectColumns is the column list shared by all RunResults queries.
const runResultsSelectColumns = `Outcome, Passed, Failed, StartTime, FinishTime, ReleaseId, Attempt, Stage,
	EnvironmentName, StackName, ErrorMessage, BranchName, AttemptId,
	TestRunId, CorrelationId, JobNumber, BatchId, TotalTestJobCount`

// AutomationTestResultsStore queries autotest_results.RunResults from ClickHouse.
type AutomationTestResultsStore struct {
	session ch.ClickhouseSessionInterface
}

// NewAutomationTestResultsStore creates an AutomationTestResultsReader backed by ClickHouse.
func NewAutomationTestResultsStore(session ch.ClickhouseSessionInterface) *AutomationTestResultsStore {
	return &AutomationTestResultsStore{session: session}
}

var _ domain.AutomationTestResultsReader = (*AutomationTestResultsStore)(nil)

// QueryAutomationTestResults returns RunResults rows for the given correlation ID,
// optionally filtered by environment / stack / stage / attempt. When Attempt is 0,
// the result is collapsed to the highest Attempt per (EnvironmentName, StackName,
// Stage) tuple via ClickHouse `LIMIT 1 BY`.
func (s *AutomationTestResultsStore) QueryAutomationTestResults(
	ctx context.Context,
	q *domain.AutomationTestResultsQuery,
) (*domain.AutomationTestResultsResult, error) {
	if q == nil {
		return nil, errors.New("query must not be nil")
	}
	if q.CorrelationID == uuid.Nil {
		return nil, errors.New("correlation ID is required")
	}

	ctx, span := otel.Tracer(automationTestResultsTracerName).Start(
		ctx, "automationtestresults.Query",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.operation", "QueryAutomationTestResults"),
			attribute.String("test.correlation_id", q.CorrelationID.String()),
			attribute.String("test.environment", q.Environment),
			attribute.String("test.stack", q.Stack),
			attribute.String("test.stage", q.Stage),
			attribute.Int("test.attempt", int(q.Attempt)),
		),
	)
	defer span.End()

	conditions := []string{"CorrelationId = {correlationId:UUID}"}
	args := []any{ch.Named("correlationId", q.CorrelationID)}
	conditions, args = appendCommonFilters(conditions, args, q.Environment, q.Stack, q.Stage, q.Attempt)

	limitClause := ""
	if q.Attempt == 0 {
		limitClause = " LIMIT 1 BY (EnvironmentName, StackName, Stage)"
	}

	query := fmt.Sprintf(
		`SELECT %s
		FROM autotest_results.RunResults
		WHERE %s
		ORDER BY EnvironmentName, StackName, Stage, Attempt DESC, StartTime DESC%s`,
		runResultsSelectColumns,
		strings.Join(conditions, " AND "),
		limitClause,
	)

	return s.runQuery(ctx, span, query, args, "automation test results")
}

// QueryAutomationTestResultsByRelease returns RunResults rows whose ReleaseId
// contains the given substring (matched via `ILIKE %x%`), optionally filtered
// by environment / stack / stage / attempt. When Attempt is 0, results are
// collapsed to the highest Attempt per (ReleaseId, EnvironmentName, StackName,
// Stage) tuple so each matched release retains its own latest attempt.
func (s *AutomationTestResultsStore) QueryAutomationTestResultsByRelease(
	ctx context.Context,
	q *domain.AutomationTestResultsByReleaseQuery,
) (*domain.AutomationTestResultsResult, error) {
	if q == nil {
		return nil, errors.New("query must not be nil")
	}
	if q.ReleaseIDSubstring == "" {
		return nil, errors.New("release ID substring is required")
	}

	ctx, span := otel.Tracer(automationTestResultsTracerName).Start(
		ctx, "automationtestresults.QueryByRelease",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.operation", "QueryAutomationTestResultsByRelease"),
			attribute.String("test.release_id_substring", q.ReleaseIDSubstring),
			attribute.String("test.environment", q.Environment),
			attribute.String("test.stack", q.Stack),
			attribute.String("test.stage", q.Stage),
			attribute.Int("test.attempt", int(q.Attempt)),
		),
	)
	defer span.End()

	conditions := []string{"ReleaseId ILIKE {releaseId:String}"}
	args := []any{ch.Named("releaseId", "%"+q.ReleaseIDSubstring+"%")}
	conditions, args = appendCommonFilters(conditions, args, q.Environment, q.Stack, q.Stage, q.Attempt)

	limitClause := ""
	if q.Attempt == 0 {
		limitClause = " LIMIT 1 BY (ReleaseId, EnvironmentName, StackName, Stage)"
	}

	query := fmt.Sprintf(
		`SELECT %s
		FROM autotest_results.RunResults
		WHERE %s
		ORDER BY ReleaseId, EnvironmentName, StackName, Stage, Attempt DESC, StartTime DESC%s`,
		runResultsSelectColumns,
		strings.Join(conditions, " AND "),
		limitClause,
	)

	return s.runQuery(ctx, span, query, args, "automation test results by release")
}

// appendCommonFilters appends the optional environment/stack/stage/attempt
// filters shared by both query methods, returning the updated slices. The
// string filters use ILIKE for case-insensitive matching since the source
// data has inconsistent casing.
func appendCommonFilters(
	inConditions []string,
	inArgs []any,
	environment, stack, stage string,
	attempt uint32,
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
		conditions = append(conditions, "Attempt = {attempt:UInt32}")
		args = append(args, ch.Named("attempt", attempt))
	}
	return conditions, args
}

// runQuery executes the prepared query, scans every row, and returns the
// result. The opLabel is folded into error messages for context.
func (s *AutomationTestResultsStore) runQuery(
	ctx context.Context,
	span trace.Span,
	query string,
	args []any,
	opLabel string,
) (result *domain.AutomationTestResultsResult, err error) {
	rows, err := s.session.QueryWithArgs(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("query failed: %v", err))
		return nil, fmt.Errorf("failed to query %s: %w", opLabel, err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	runs := make([]domain.AutomationTestRunResult, 0)
	for rows.Next() {
		var (
			row                        domain.AutomationTestRunResult
			testRunID, corrID, batchID *uuid.UUID
		)
		if scanErr := rows.Scan(
			&row.Outcome, &row.Passed, &row.Failed, &row.StartTime, &row.FinishTime,
			&row.ReleaseID, &row.Attempt, &row.Stage,
			&row.EnvironmentName, &row.StackName, &row.ErrorMessage, &row.BranchName,
			&row.AttemptID, &testRunID, &corrID, &row.JobNumber, &batchID,
			&row.TotalTestJobCount,
		); scanErr != nil {
			return nil, fmt.Errorf("failed to scan %s row: %w", opLabel, scanErr)
		}
		if testRunID != nil {
			s := testRunID.String()
			row.TestRunID = &s
		}
		if corrID != nil {
			s := corrID.String()
			row.CorrelationID = &s
		}
		if batchID != nil {
			s := batchID.String()
			row.BatchID = &s
		}
		runs = append(runs, row)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("error iterating %s rows: %w", opLabel, rowsErr)
	}

	result = &domain.AutomationTestResultsResult{
		Runs:  runs,
		Count: len(runs),
	}
	span.SetAttributes(attribute.Int("test.result_count", result.Count))
	span.SetStatus(codes.Ok, "")
	return result, nil
}
