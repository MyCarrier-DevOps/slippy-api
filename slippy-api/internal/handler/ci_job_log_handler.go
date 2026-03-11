package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// CIJobLogHandler holds dependencies for CI job log route handlers.
type CIJobLogHandler struct {
	reader domain.CIJobLogReader
}

// NewCIJobLogHandler creates a handler backed by the given CIJobLogReader.
func NewCIJobLogHandler(reader domain.CIJobLogReader) *CIJobLogHandler {
	return &CIJobLogHandler{reader: reader}
}

// --- Input / Output types ------------------------------------------------

// GetLogsInput captures path and query parameters for log queries.
type GetLogsInput struct {
	CorrelationID   string `path:"correlationID" doc:"Correlation ID to look up logs for"`
	Limit           int    `                     doc:"Page size"                                query:"limit"            default:"100"  minimum:"1" maximum:"1000"`
	Cursor          string `                     doc:"Pagination cursor from previous response" query:"cursor"`
	Sort            string `                     doc:"Sort by timestamp"                        query:"sort"             default:"desc"                            enum:"asc,desc"`
	Level           string `                     doc:"Filter by log level"                      query:"level"`
	Service         string `                     doc:"Filter by service name"                   query:"service"`
	Component       string `                     doc:"Filter by component name"                 query:"component"`
	Cluster         string `                     doc:"Filter by cluster"                        query:"cluster"`
	Cloud           string `                     doc:"Filter by cloud provider"                 query:"cloud"`
	Environment     string `                     doc:"Filter by environment"                    query:"environment"`
	Namespace       string `                     doc:"Filter by namespace"                      query:"namespace"`
	Message         string `                     doc:"Filter by exact message"                  query:"message"`
	CIJobInstance   string `                     doc:"Filter by CI job instance"                query:"ci_job_instance"`
	CIJobType       string `                     doc:"Filter by CI job type"                    query:"ci_job_type"`
	BuildRepository string `                     doc:"Filter by build repository"               query:"build_repository"`
	BuildImage      string `                     doc:"Filter by build image"                    query:"build_image"`
	BuildBranch     string `                     doc:"Filter by build branch"                   query:"build_branch"`
}

// GetLogsOutput wraps the paginated log query response.
type GetLogsOutput struct {
	Body struct {
		Logs     []domain.CIJobLog `json:"logs"`
		NextPage string            `json:"next_page,omitempty" doc:"URL path for the next page of results"`
		Count    int               `json:"count" doc:"Number of logs returned in this page"`
	}
}

// --- Route Registration --------------------------------------------------

// RegisterCIJobLogRoutes registers CI job log routes on the given huma API.
func RegisterCIJobLogRoutes(api huma.API, h *CIJobLogHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-logs",
		Method:      http.MethodGet,
		Path:        "/logs/{correlationID}",
		Summary:     "Query CI job logs by correlation ID",
		Description: "Returns paginated CI job logs from observability.ciJob, filterable by any column. " +
			"Use the next_page URL from the response to fetch subsequent pages.",
		Security: apiKeySecurity,
	}, h.getLogs)
}

// --- Handler -------------------------------------------------------------

func (h *CIJobLogHandler) getLogs(ctx context.Context, input *GetLogsInput) (*GetLogsOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getLogs",
		trace.WithAttributes(
			attribute.String("log.correlation_id", input.CorrelationID),
			attribute.Int("log.limit", input.Limit),
			attribute.String("log.sort", input.Sort),
		),
	)
	defer span.End()

	query := &domain.CIJobLogQuery{
		CorrelationID:   input.CorrelationID,
		Limit:           input.Limit,
		Cursor:          input.Cursor,
		Sort:            domain.SortOrder(input.Sort),
		Level:           input.Level,
		Service:         input.Service,
		Component:       input.Component,
		Cluster:         input.Cluster,
		Cloud:           input.Cloud,
		Environment:     input.Environment,
		Namespace:       input.Namespace,
		Message:         input.Message,
		CIJobInstance:   input.CIJobInstance,
		CIJobType:       input.CIJobType,
		BuildRepository: input.BuildRepository,
		BuildImage:      input.BuildImage,
		BuildBranch:     input.BuildBranch,
	}

	result, err := h.reader.QueryLogs(ctx, query)
	if err != nil {
		recordHandlerError(span, err)
		if errors.Is(err, domain.ErrInvalidCursor) {
			return nil, huma.NewError(http.StatusBadRequest, "invalid cursor parameter")
		}
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	span.SetAttributes(attribute.Int("log.result_count", result.Count))
	span.SetStatus(codes.Ok, "")

	out := &GetLogsOutput{}
	out.Body.Logs = result.Logs
	if out.Body.Logs == nil {
		out.Body.Logs = []domain.CIJobLog{}
	}
	out.Body.Count = result.Count

	if result.NextCursor != "" {
		out.Body.NextPage = buildNextPageURL(input, result.NextCursor)
	}

	return out, nil
}

// buildNextPageURL constructs the URL path for the next page of results,
// preserving all current query parameters and adding the new cursor.
func buildNextPageURL(input *GetLogsInput, cursor string) string {
	v := url.Values{}
	v.Set("limit", strconv.Itoa(input.Limit))
	v.Set("sort", input.Sort)
	v.Set("cursor", cursor)

	setIfNonEmpty := func(key, val string) {
		if val != "" {
			v.Set(key, val)
		}
	}
	setIfNonEmpty("level", input.Level)
	setIfNonEmpty("service", input.Service)
	setIfNonEmpty("component", input.Component)
	setIfNonEmpty("cluster", input.Cluster)
	setIfNonEmpty("cloud", input.Cloud)
	setIfNonEmpty("environment", input.Environment)
	setIfNonEmpty("namespace", input.Namespace)
	setIfNonEmpty("message", input.Message)
	setIfNonEmpty("ci_job_instance", input.CIJobInstance)
	setIfNonEmpty("ci_job_type", input.CIJobType)
	setIfNonEmpty("build_repository", input.BuildRepository)
	setIfNonEmpty("build_image", input.BuildImage)
	setIfNonEmpty("build_branch", input.BuildBranch)

	return fmt.Sprintf("/logs/%s?%s", url.PathEscape(input.CorrelationID), v.Encode())
}
