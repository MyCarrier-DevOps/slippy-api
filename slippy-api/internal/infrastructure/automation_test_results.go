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
// optionally filtered by environment / stack / stage / attempt. When LatestOnly is
// true and Attempt is unset, the result is collapsed to the highest Attempt per
// (EnvironmentName, StackName, Stage) tuple via ClickHouse `LIMIT 1 BY`.
func (s *AutomationTestResultsStore) QueryAutomationTestResults(
	ctx context.Context,
	q *domain.AutomationTestResultsQuery,
) (result *domain.AutomationTestResultsResult, err error) {
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
			attribute.Bool("test.latest_only", q.LatestOnly),
		),
	)
	defer span.End()

	conditions := []string{"CorrelationId = {correlationId:UUID}"}
	args := []any{ch.Named("correlationId", q.CorrelationID)}

	if q.Environment != "" {
		conditions = append(conditions, "EnvironmentName = {environment:String}")
		args = append(args, ch.Named("environment", q.Environment))
	}
	if q.Stack != "" {
		conditions = append(conditions, "StackName = {stack:String}")
		args = append(args, ch.Named("stack", q.Stack))
	}
	if q.Stage != "" {
		conditions = append(conditions, "Stage = {stage:String}")
		args = append(args, ch.Named("stage", q.Stage))
	}
	if q.Attempt > 0 {
		conditions = append(conditions, "Attempt = {attempt:UInt32}")
		args = append(args, ch.Named("attempt", q.Attempt))
	}

	limitClause := ""
	if q.Attempt == 0 && q.LatestOnly {
		limitClause = " LIMIT 1 BY (EnvironmentName, StackName, Stage)"
	}

	query := fmt.Sprintf(
		`SELECT Outcome, Passed, Failed, StartTime, FinishTime, ReleaseId, Attempt, Stage,
		        EnvironmentName, StackName, ErrorMessage, BranchName, AttemptId,
		        TestRunId, CorrelationId, JobNumber, BatchId, TotalTestJobCount
		FROM autotest_results.RunResults
		WHERE %s
		ORDER BY EnvironmentName, StackName, Stage, Attempt DESC, StartTime DESC%s`,
		strings.Join(conditions, " AND "),
		limitClause,
	)

	rows, err := s.session.QueryWithArgs(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("query failed: %v", err))
		return nil, fmt.Errorf("failed to query automation test results: %w", err)
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
			return nil, fmt.Errorf("failed to scan automation test results row: %w", scanErr)
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
		return nil, fmt.Errorf("error iterating automation test results rows: %w", rowsErr)
	}

	result = &domain.AutomationTestResultsResult{
		Runs:  runs,
		Count: len(runs),
	}
	span.SetAttributes(attribute.Int("test.result_count", result.Count))
	span.SetStatus(codes.Ok, "")
	return result, nil
}
