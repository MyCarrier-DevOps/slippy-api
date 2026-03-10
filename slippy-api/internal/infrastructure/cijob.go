package infrastructure

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

const cijobTracerName = "slippy-api/cijob"

// CIJobLogStore queries CI job logs from the observability.ciJob ClickHouse table.
type CIJobLogStore struct {
	session ch.ClickhouseSessionInterface
}

// NewCIJobLogStore creates a CIJobLogReader backed by ClickHouse.
func NewCIJobLogStore(session ch.ClickhouseSessionInterface) *CIJobLogStore {
	return &CIJobLogStore{session: session}
}

// Compile-time interface compliance check.
var _ domain.CIJobLogReader = (*CIJobLogStore)(nil)

type columnFilter struct {
	column    string
	paramName string
	value     string
}

func buildColumnFilters(q *domain.CIJobLogQuery) []columnFilter {
	var filters []columnFilter
	add := func(col, param, val string) {
		if val != "" {
			filters = append(filters, columnFilter{col, param, val})
		}
	}
	add("Level", "fLevel", q.Level)
	add("Service", "fService", q.Service)
	add("Component", "fComponent", q.Component)
	add("Cluster", "fCluster", q.Cluster)
	add("Cloud", "fCloud", q.Cloud)
	add("Environment", "fEnvironment", q.Environment)
	add("Namespace", "fNamespace", q.Namespace)
	add("Message", "fMessage", q.Message)
	add("CiJobInstance", "fCiJobInstance", q.CIJobInstance)
	add("CiJobType", "fCiJobType", q.CIJobType)
	add("BuildRepository", "fBuildRepository", q.BuildRepository)
	add("BuildImage", "fBuildImage", q.BuildImage)
	add("BuildBranch", "fBuildBranch", q.BuildBranch)
	return filters
}

// QueryLogs queries CI job logs by correlation ID with optional cursor pagination,
// configurable sort order, and per-column filters.
func (s *CIJobLogStore) QueryLogs(
	ctx context.Context,
	q *domain.CIJobLogQuery,
) (result *domain.CIJobLogResult, err error) {
	ctx, span := otel.Tracer(cijobTracerName).Start(ctx, "cijob.QueryLogs",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.operation", "QueryLogs"),
			attribute.String("log.correlation_id", q.CorrelationID),
			attribute.Int("log.limit", q.Limit),
			attribute.String("log.sort", string(q.Sort)),
		),
	)
	defer span.End()

	conditions := []string{"CorrelationId = {correlationId:String}"}
	args := []any{ch.Named("correlationId", q.CorrelationID)}

	if q.Cursor != "" {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, q.Cursor)
		if parseErr != nil {
			span.RecordError(parseErr)
			span.SetStatus(codes.Error, "invalid cursor")
			return nil, fmt.Errorf("%w: %w", domain.ErrInvalidCursor, parseErr)
		}
		if q.Sort == domain.SortAsc {
			conditions = append(conditions, "Timestamp > {cursor:DateTime64(9, 'UTC')}")
		} else {
			conditions = append(conditions, "Timestamp < {cursor:DateTime64(9, 'UTC')}")
		}
		args = append(args, ch.Named("cursor", cursorTime))
	}

	for _, f := range buildColumnFilters(q) {
		conditions = append(conditions, fmt.Sprintf("%s = {%s:String}", f.column, f.paramName))
		args = append(args, ch.Named(f.paramName, f.value))
	}

	sortDir := "DESC"
	if q.Sort == domain.SortAsc {
		sortDir = "ASC"
	}

	fetchLimit := q.Limit + 1

	query := fmt.Sprintf(
		`SELECT Timestamp, Level, Service, Component, Cluster, Cloud, Environment,
		        Namespace, Message, CiJobInstance, CiJobType,
		        BuildRepository, BuildImage, BuildBranch
		FROM observability.ciJob
		WHERE %s
		ORDER BY Timestamp %s
		LIMIT {fetchLimit:UInt32}`,
		strings.Join(conditions, " AND "),
		sortDir,
	)
	args = append(args, ch.Named("fetchLimit", uint32(fetchLimit)))

	rows, err := s.session.QueryWithArgs(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("query failed: %v", err))
		return nil, fmt.Errorf("failed to query ci job logs: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	logs := make([]domain.CIJobLog, 0, q.Limit)
	for rows.Next() {
		var entry domain.CIJobLog
		if scanErr := rows.Scan(
			&entry.Timestamp, &entry.Level, &entry.Service, &entry.Component,
			&entry.Cluster, &entry.Cloud, &entry.Environment, &entry.Namespace,
			&entry.Message, &entry.CIJobInstance, &entry.CIJobType,
			&entry.BuildRepository, &entry.BuildImage, &entry.BuildBranch,
		); scanErr != nil {
			return nil, fmt.Errorf("failed to scan ci job log row: %w", scanErr)
		}
		logs = append(logs, entry)
	}

	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("error iterating ci job log rows: %w", rowsErr)
	}

	result = &domain.CIJobLogResult{}

	if len(logs) > q.Limit {
		logs = logs[:q.Limit]
		result.NextCursor = logs[len(logs)-1].Timestamp.Format(time.RFC3339Nano)
	}

	result.Logs = logs
	result.Count = len(logs)

	span.SetAttributes(attribute.Int("log.result_count", result.Count))
	span.SetStatus(codes.Ok, "")
	return result, nil
}
