package handler

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// AutomationTestResultsHandler holds dependencies for automation test results routes.
type AutomationTestResultsHandler struct {
	reader domain.AutomationTestResultsReader
}

// NewAutomationTestResultsHandler creates a handler backed by the given reader.
func NewAutomationTestResultsHandler(reader domain.AutomationTestResultsReader) *AutomationTestResultsHandler {
	return &AutomationTestResultsHandler{reader: reader}
}

// GetAutomationTestResultsInput captures path and query parameters.
type GetAutomationTestResultsInput struct {
	CorrelationID string `path:"correlationID" doc:"Slip correlation ID (UUID)"`
	Environment   string `                     doc:"Filter by EnvironmentName (exact)"                                                       query:"environment"`
	Stack         string `                     doc:"Filter by StackName (exact)"                                                             query:"stack"`
	Stage         string `                     doc:"Filter by Stage / test category, e.g. FeatureCoreApi, PreprodCoreApi, FeatureUI (exact)" query:"stage"`
	Attempt       uint32 `                     doc:"Exact Attempt; overrides latest_only when > 0"                                           query:"attempt"`
	LatestOnly    bool   `                     doc:"When true (default), return only the highest Attempt per (Environment, Stack, Stage)"    query:"latest_only" default:"true"`
}

// GetAutomationTestResultsOutput wraps the test-run query response.
type GetAutomationTestResultsOutput struct {
	Body struct {
		Runs  []domain.AutomationTestRunResult `json:"runs"`
		Count int                              `json:"count" doc:"Number of runs returned"`
	}
}

// RegisterAutomationTestResultsRoutes registers the automation test results route on the API.
func RegisterAutomationTestResultsRoutes(api huma.API, h *AutomationTestResultsHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-automation-test-results",
		Method:      http.MethodGet,
		Path:        "/automation-test-results/{correlationID}",
		Summary:     "Get automation test-suite run results by correlation ID",
		Description: "Returns rows from autotest_results.RunResults for the given slip correlation ID, " +
			"optionally filtered by environment, stack, stage, or attempt. Defaults to the latest " +
			"attempt per (Environment, Stack, Stage) tuple.",
		Security: apiKeySecurity,
	}, h.getAutomationTestResults)
}

func (h *AutomationTestResultsHandler) getAutomationTestResults(
	ctx context.Context,
	input *GetAutomationTestResultsInput,
) (*GetAutomationTestResultsOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getAutomationTestResults",
		trace.WithAttributes(
			attribute.String("test.correlation_id", input.CorrelationID),
			attribute.String("test.environment", input.Environment),
			attribute.String("test.stack", input.Stack),
			attribute.String("test.stage", input.Stage),
			attribute.Int("test.attempt", int(input.Attempt)),
			attribute.Bool("test.latest_only", input.LatestOnly),
		),
	)
	defer span.End()

	correlationID, err := uuid.Parse(input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, "invalid correlationID")
	}

	query := &domain.AutomationTestResultsQuery{
		CorrelationID: correlationID,
		Environment:   input.Environment,
		Stack:         input.Stack,
		Stage:         input.Stage,
		Attempt:       input.Attempt,
		LatestOnly:    input.LatestOnly,
	}

	result, err := h.reader.QueryAutomationTestResults(ctx, query)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	span.SetAttributes(attribute.Int("test.result_count", result.Count))
	span.SetStatus(codes.Ok, "")

	out := &GetAutomationTestResultsOutput{}
	out.Body.Runs = result.Runs
	if out.Body.Runs == nil {
		out.Body.Runs = []domain.AutomationTestRunResult{}
	}
	out.Body.Count = result.Count
	return out, nil
}
