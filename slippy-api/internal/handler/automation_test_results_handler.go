package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// AutomationTestResultsHandler holds dependencies for automation-test-results
// routes. The optional testsReader powers the per-test drill-down endpoints;
// when nil, those routes are not registered.
type AutomationTestResultsHandler struct {
	reader      domain.AutomationTestResultsReader
	testsReader domain.AutomationTestsReader
}

// NewAutomationTestResultsHandler creates a handler backed by the given readers.
// testsReader may be nil; when nil, the /tests drill-down routes are not registered.
func NewAutomationTestResultsHandler(
	reader domain.AutomationTestResultsReader,
	testsReader domain.AutomationTestsReader,
) *AutomationTestResultsHandler {
	return &AutomationTestResultsHandler{reader: reader, testsReader: testsReader}
}

// GetAutomationTestResultsInput captures path and query parameters for the
// run-summary endpoint.
type GetAutomationTestResultsInput struct {
	CorrelationID string `path:"correlationID" doc:"Slip correlation ID (UUID)"`
	Environment   string `                     doc:"Filter by EnvironmentName (case-insensitive)"                                                         query:"environment"`
	Stack         string `                     doc:"Filter by StackName (case-insensitive)"                                                               query:"stack"`
	Stage         string `                     doc:"Filter by Stage / test category, e.g. FeatureCoreApi, PreprodCoreApi, FeatureUI (case-insensitive)"   query:"stage"`
	Attempt       uint32 `                     doc:"Specific Attempt to fetch. When omitted, returns the latest attempt per (Environment, Stack, Stage)." query:"attempt"`
}

// GetAutomationTestResultsOutput wraps the run-summary response.
type GetAutomationTestResultsOutput struct {
	Body struct {
		Runs  []domain.AutomationTestRunResult `json:"runs"`
		Count int                              `json:"count" doc:"Number of runs returned"`
	}
}

// GetTestsByCorrelationIDInput captures path and query parameters for the
// per-test drill-down nested under a slip's correlation ID.
type GetTestsByCorrelationIDInput struct {
	CorrelationID string `path:"correlationID" doc:"Slip correlation ID (UUID)"`
	Environment   string `                     doc:"Filter by EnvironmentName (case-insensitive)"                                                                                         query:"environment"`
	Stack         string `                     doc:"Filter by StackName (case-insensitive)"                                                                                               query:"stack"`
	Stage         string `                     doc:"Filter by Stage / test category (case-insensitive)"                                                                                   query:"stage"`
	Attempt       uint32 `                     doc:"Filter by exact Attempt"                                                                                                              query:"attempt"`
	Status        string `                     doc:"ResultStatus filter (case-insensitive ILIKE). Defaults to 'Failed'; pass '*' or 'all' to disable the filter and return every status." query:"status"`
	Limit         int    `                     doc:"Page size"                                                                                                                            query:"limit"       default:"100" minimum:"1" maximum:"1000"`
	Cursor        string `                     doc:"Pagination cursor from a previous response"                                                                                           query:"cursor"`
}

// GetTestsOutput wraps a paginated list of individual test results.
type GetTestsOutput struct {
	Body struct {
		Tests    []domain.AutomationTestResult `json:"tests"`
		NextPage string                        `json:"next_page,omitempty" doc:"URL path for the next page of results"`
		Count    int                           `json:"count" doc:"Number of tests in this page"`
	}
}

// GetTestByCorrelationIDInput captures path parameters for the single-test
// drill-down. (CorrelationID, TestId) uniquely scopes a TestResultsCor row,
// so no extra filters are accepted here.
type GetTestByCorrelationIDInput struct {
	CorrelationID string `path:"correlationID" doc:"Slip correlation ID (UUID)"`
	TestID        string `path:"testId"        doc:"Test row ID (UUID) from a /tests response"`
}

// GetTestOutput wraps a single test result (full row including stack_trace).
type GetTestOutput struct {
	Body domain.AutomationTestResult
}

// resolveStatus applies the per-test status filter rules:
//
//	""       → "Failed" (default)
//	"*"|"all"→ ""        (no filter, all statuses)
//	other    → the supplied value, passed through to ILIKE
func resolveStatus(s string) string {
	switch strings.ToLower(s) {
	case "":
		return "Failed"
	case "*", "all":
		return ""
	default:
		return s
	}
}

// RegisterAutomationTestResultsRoutes registers the automation-test-results
// routes on the given API.
func RegisterAutomationTestResultsRoutes(api huma.API, h *AutomationTestResultsHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-automation-test-results",
		Method:      http.MethodGet,
		Path:        "/automation-test-results/by-correlation/{correlationID}",
		Summary:     "Get automation test-suite run results by correlation ID",
		Description: "Returns rows from autotest_results.RunResults for the given slip correlation ID, " +
			"optionally filtered by environment, stack, stage, or attempt. Defaults to the latest " +
			"attempt per (Environment, Stack, Stage) tuple.",
		Security: apiKeySecurity,
	}, h.getAutomationTestResults)

	if h.testsReader != nil {
		huma.Register(api, huma.Operation{
			OperationID: "get-automation-test-results-tests",
			Method:      http.MethodGet,
			Path:        "/automation-test-results/by-correlation/{correlationID}/tests",
			Summary:     "List individual test results for a slip's runs",
			Description: "Returns rows from autotest_results.TestResultsCor whose CorrelationId matches " +
				"the given slip correlation ID. Bounded to a 14-day lookback for partition pruning. " +
				"Filterable by environment, stack, stage, attempt, and status. Defaults to " +
				"ResultStatus='Failed'; pass `status=*` or `status=all` to widen.",
			Security: apiKeySecurity,
		}, h.getTestsByCorrelationID)

		huma.Register(api, huma.Operation{
			OperationID: "get-automation-test-result-by-id-correlation",
			Method:      http.MethodGet,
			Path:        "/automation-test-results/by-correlation/{correlationID}/tests/{testId}",
			Summary:     "Get a single test result (with stack trace) by TestId",
			Description: "Returns the single TestResultsCor row matching the given (CorrelationId, " +
				"TestId) pair. Bounded to a 14-day lookback. 404 when no matching test exists.",
			Security: apiKeySecurity,
		}, h.getTestByCorrelationID)
	}
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
		),
	)
	defer span.End()

	correlationID, err := uuid.Parse(input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, "invalid correlationID")
	}

	result, err := h.reader.QueryAutomationTestResults(ctx, &domain.AutomationTestResultsQuery{
		CorrelationID: correlationID,
		Environment:   input.Environment,
		Stack:         input.Stack,
		Stage:         input.Stage,
		Attempt:       input.Attempt,
	})
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

func (h *AutomationTestResultsHandler) getTestsByCorrelationID(
	ctx context.Context,
	input *GetTestsByCorrelationIDInput,
) (*GetTestsOutput, error) {
	status := resolveStatus(input.Status)
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getTestsByCorrelationID",
		trace.WithAttributes(
			attribute.String("test.correlation_id", input.CorrelationID),
			attribute.String("test.environment", input.Environment),
			attribute.String("test.stack", input.Stack),
			attribute.String("test.stage", input.Stage),
			attribute.Int("test.attempt", int(input.Attempt)),
			attribute.String("test.status", status),
			attribute.Int("test.limit", input.Limit),
		),
	)
	defer span.End()

	correlationID, err := uuid.Parse(input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, "invalid correlationID")
	}

	result, err := h.testsReader.QueryTestsByCorrelation(ctx, &domain.AutomationTestsByCorrelationQuery{
		CorrelationID: correlationID,
		Environment:   input.Environment,
		Stack:         input.Stack,
		Stage:         input.Stage,
		Attempt:       uint8(input.Attempt),
		Status:        status,
		Limit:         input.Limit,
		Cursor:        input.Cursor,
	})
	if err != nil {
		recordHandlerError(span, err)
		if errors.Is(err, domain.ErrInvalidTestsCursor) {
			return nil, huma.NewError(http.StatusBadRequest, "invalid cursor parameter")
		}
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	span.SetAttributes(attribute.Int("test.result_count", result.Count))
	span.SetStatus(codes.Ok, "")

	out := &GetTestsOutput{}
	out.Body.Tests = result.Tests
	if out.Body.Tests == nil {
		out.Body.Tests = []domain.AutomationTestResult{}
	}
	out.Body.Count = result.Count
	if result.NextCursor != "" {
		out.Body.NextPage = buildTestsByCorrelationIDNextPageURL(input, result.NextCursor)
	}
	return out, nil
}

func (h *AutomationTestResultsHandler) getTestByCorrelationID(
	ctx context.Context,
	input *GetTestByCorrelationIDInput,
) (*GetTestOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getTestByCorrelationID",
		trace.WithAttributes(
			attribute.String("test.correlation_id", input.CorrelationID),
			attribute.String("test.test_id", input.TestID),
		),
	)
	defer span.End()

	correlationID, err := uuid.Parse(input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, "invalid correlationID")
	}
	testID, err := uuid.Parse(input.TestID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, "invalid testId")
	}

	res, err := h.testsReader.LoadTestByCorrelation(ctx, &domain.LoadTestByCorrelationQuery{
		CorrelationID: correlationID,
		TestID:        testID,
	})
	if err != nil {
		recordHandlerError(span, err)
		if errors.Is(err, domain.ErrTestNotFound) {
			return nil, huma.NewError(http.StatusNotFound, "test not found")
		}
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	span.SetStatus(codes.Ok, "")
	return &GetTestOutput{Body: *res}, nil
}

// buildTestsByCorrelationIDNextPageURL constructs the URL path for the next
// page of tests, preserving filters and adding the new cursor.
func buildTestsByCorrelationIDNextPageURL(input *GetTestsByCorrelationIDInput, cursor string) string {
	v := url.Values{}
	v.Set("limit", strconv.Itoa(input.Limit))
	v.Set("cursor", cursor)
	setIfNonEmpty(v, "environment", input.Environment)
	setIfNonEmpty(v, "stack", input.Stack)
	setIfNonEmpty(v, "stage", input.Stage)
	if input.Attempt > 0 {
		v.Set("attempt", strconv.FormatUint(uint64(input.Attempt), 10))
	}
	// Preserve the explicit status only — when it's empty the next page will
	// re-default to Failed via resolveStatus, which is what we want.
	if input.Status != "" {
		v.Set("status", input.Status)
	}
	return fmt.Sprintf("/v1/automation-test-results/by-correlation/%s/tests?%s",
		url.PathEscape(input.CorrelationID), v.Encode())
}

func setIfNonEmpty(v url.Values, key, val string) {
	if val != "" {
		v.Set(key, val)
	}
}
