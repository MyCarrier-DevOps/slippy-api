package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// AutomationTestResultsHandler holds dependencies for automation test results routes.
// The optional testsReader powers the per-test drill-down endpoints; when nil,
// those routes are still registered but always return 503 (caller would need
// to wire the dependency).
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

// GetAutomationTestResultsInput captures path and query parameters.
type GetAutomationTestResultsInput struct {
	CorrelationID string `path:"correlationID" doc:"Slip correlation ID (UUID)"`
	Environment   string `                     doc:"Filter by EnvironmentName (exact)"                                                                    query:"environment"`
	Stack         string `                     doc:"Filter by StackName (exact)"                                                                          query:"stack"`
	Stage         string `                     doc:"Filter by Stage / test category, e.g. FeatureCoreApi, PreprodCoreApi, FeatureUI (exact)"              query:"stage"`
	Attempt       uint32 `                     doc:"Specific Attempt to fetch. When omitted, returns the latest attempt per (Environment, Stack, Stage)." query:"attempt"`
}

// GetAutomationTestResultsByReleaseInput captures path and query parameters
// for the release-id substring lookup. ReleaseID must be at least 7
// characters long (the length of a short Git SHA).
type GetAutomationTestResultsByReleaseInput struct {
	ReleaseID   string `path:"releaseId" doc:"Release ID substring; matched as ILIKE %x% against ReleaseId. Minimum 7 chars (short SHA length)."`
	Environment string `                 doc:"Filter by EnvironmentName (exact)"                                                                               query:"environment"`
	Stack       string `                 doc:"Filter by StackName (exact)"                                                                                     query:"stack"`
	Stage       string `                 doc:"Filter by Stage / test category, e.g. FeatureCoreApi, PreprodCoreApi, FeatureUI (exact)"                         query:"stage"`
	Attempt     uint32 `                 doc:"Specific Attempt to fetch. When omitted, returns the latest attempt per (ReleaseId, Environment, Stack, Stage)." query:"attempt"`
}

// GetAutomationTestResultsOutput wraps the test-run query response.
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
	Environment   string `                     doc:"Filter parent runs by EnvironmentName (case-insensitive)"                                                                             query:"environment"`
	Stack         string `                     doc:"Filter parent runs by StackName (case-insensitive)"                                                                                   query:"stack"`
	Stage         string `                     doc:"Filter parent runs by Stage / test category (case-insensitive)"                                                                       query:"stage"`
	Attempt       uint32 `                     doc:"Specific Attempt to fetch. When omitted, resolves to the latest attempt per (Environment, Stack, Stage)."                             query:"attempt"`
	Status        string `                     doc:"ResultStatus filter (case-insensitive ILIKE). Defaults to 'Failed'; pass '*' or 'all' to disable the filter and return every status." query:"status"`
	Limit         int    `                     doc:"Page size"                                                                                                                            query:"limit"       default:"100" minimum:"1" maximum:"1000"`
	Cursor        string `                     doc:"Pagination cursor from a previous response"                                                                                           query:"cursor"`
}

// GetTestsByReleaseInput captures path and query parameters for the per-test
// drill-down nested under a release-id substring search.
type GetTestsByReleaseInput struct {
	ReleaseID   string `path:"releaseId" doc:"Release ID substring (min 7 chars). Resolves matching runs first, then drills into their tests."`
	Environment string `                 doc:"Filter parent runs by EnvironmentName (case-insensitive)"                                                                             query:"environment"`
	Stack       string `                 doc:"Filter parent runs by StackName (case-insensitive)"                                                                                   query:"stack"`
	Stage       string `                 doc:"Filter parent runs by Stage / test category (case-insensitive)"                                                                       query:"stage"`
	Attempt     uint32 `                 doc:"Specific Attempt to fetch. When omitted, resolves to the latest attempt per (ReleaseId, Environment, Stack, Stage)."                  query:"attempt"`
	Status      string `                 doc:"ResultStatus filter (case-insensitive ILIKE). Defaults to 'Failed'; pass '*' or 'all' to disable the filter and return every status." query:"status"`
	Limit       int    `                 doc:"Page size"                                                                                                                            query:"limit"       default:"100" minimum:"1" maximum:"1000"`
	Cursor      string `                 doc:"Pagination cursor from a previous response"                                                                                           query:"cursor"`
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

// GetTestsOutput wraps a paginated list of individual test results.
type GetTestsOutput struct {
	Body struct {
		Tests    []domain.AutomationTestResult `json:"tests"`
		NextPage string                        `json:"next_page,omitempty" doc:"URL path for the next page of results"`
		Count    int                           `json:"count" doc:"Number of tests in this page"`
	}
}

// GetTestByCorrelationIDInput captures path and query parameters for the
// single-test lookup nested under a slip's correlation ID. The optional
// environment/stack/stage/attempt filters narrow which parent runs are used
// to derive the StartTime window; they don't have to match the run that
// produced the test (the lookup is by TestId, which is globally unique).
type GetTestByCorrelationIDInput struct {
	CorrelationID string `path:"correlationID" doc:"Slip correlation ID (UUID)"`
	TestID        string `path:"testId"        doc:"Test row ID (UUID) from a /tests response"`
	Environment   string `                     doc:"Filter parent runs by EnvironmentName (case-insensitive)"                                                 query:"environment"`
	Stack         string `                     doc:"Filter parent runs by StackName (case-insensitive)"                                                       query:"stack"`
	Stage         string `                     doc:"Filter parent runs by Stage / test category (case-insensitive)"                                           query:"stage"`
	Attempt       uint32 `                     doc:"Specific Attempt to fetch. When omitted, resolves to the latest attempt per (Environment, Stack, Stage)." query:"attempt"`
}

// GetTestByReleaseInput is the by-release counterpart.
type GetTestByReleaseInput struct {
	ReleaseID   string `path:"releaseId" doc:"Release ID substring (min 7 chars)"`
	TestID      string `path:"testId"    doc:"Test row ID (UUID) from a /tests response"`
	Environment string `                 doc:"Filter parent runs by EnvironmentName (case-insensitive)"                                                            query:"environment"`
	Stack       string `                 doc:"Filter parent runs by StackName (case-insensitive)"                                                                  query:"stack"`
	Stage       string `                 doc:"Filter parent runs by Stage / test category (case-insensitive)"                                                      query:"stage"`
	Attempt     uint32 `                 doc:"Specific Attempt to fetch. When omitted, resolves to the latest attempt per (ReleaseId, Environment, Stack, Stage)." query:"attempt"`
}

// GetTestOutput wraps a single test result (full row including stack_trace).
type GetTestOutput struct {
	Body domain.AutomationTestResult
}

// minReleaseIDLength is the minimum accepted length for the release-id path
// parameter. It matches the length of a short (7-char) Git SHA.
const minReleaseIDLength = 7

// finishTimeBuffer is added to RunResults.FinishTime when bounding the
// TestResults StartTime predicate, to absorb clock skew between the run
// summary write and the per-test row writes.
const finishTimeBuffer = 5 * time.Minute

// RegisterAutomationTestResultsRoutes registers the automation test results routes on the API.
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

	huma.Register(api, huma.Operation{
		OperationID: "get-automation-test-results-by-release",
		Method:      http.MethodGet,
		Path:        "/automation-test-results/by-release/{releaseId}",
		Summary:     "Get automation test-suite run results by release ID substring",
		Description: "Returns rows from autotest_results.RunResults whose ReleaseId contains the given " +
			"substring (matched as ILIKE %x%). Useful for searching by short SHA or partial release " +
			"identifier (e.g. `26.04`). Defaults to the latest attempt per (ReleaseId, Environment, " +
			"Stack, Stage) tuple.",
		Security: apiKeySecurity,
	}, h.getAutomationTestResultsByRelease)

	if h.testsReader != nil {
		huma.Register(api, huma.Operation{
			OperationID: "get-automation-test-results-tests",
			Method:      http.MethodGet,
			Path:        "/automation-test-results/by-correlation/{correlationID}/tests",
			Summary:     "Drill into individual test results for a slip's runs",
			Description: "Resolves the matching RunResults rows for the correlation ID (with the same " +
				"environment/stack/stage/attempt filters as the parent endpoint), then returns the " +
				"individual TestResults rows within each run's StartTime window. Defaults to " +
				"ResultStatus='Failed'; pass `status=` empty to widen.",
			Security: apiKeySecurity,
		}, h.getTestsByCorrelationID)

		huma.Register(api, huma.Operation{
			OperationID: "get-automation-test-results-by-release-tests",
			Method:      http.MethodGet,
			Path:        "/automation-test-results/by-release/{releaseId}/tests",
			Summary:     "Drill into individual test results by release ID substring",
			Description: "Resolves the matching RunResults rows for the release-ID substring (with the " +
				"same environment/stack/stage/attempt filters as the parent endpoint), then returns " +
				"the individual TestResults rows within each run's StartTime window. Defaults to " +
				"ResultStatus='Failed'; pass `status=` empty to widen.",
			Security: apiKeySecurity,
		}, h.getTestsByRelease)

		huma.Register(api, huma.Operation{
			OperationID: "get-automation-test-result-by-id-correlation",
			Method:      http.MethodGet,
			Path:        "/automation-test-results/by-correlation/{correlationID}/tests/{testId}",
			Summary:     "Get a single test result (with stack trace) by TestId",
			Description: "Resolves the matching RunResults rows for the correlation ID (using the same " +
				"environment/stack/stage/attempt filters), derives a StartTime window for partition " +
				"pruning, then returns the single TestResults row matching the given TestId. " +
				"Returns 404 when no matching test exists in scope.",
			Security: apiKeySecurity,
		}, h.getTestByCorrelationID)

		huma.Register(api, huma.Operation{
			OperationID: "get-automation-test-result-by-id-release",
			Method:      http.MethodGet,
			Path:        "/automation-test-results/by-release/{releaseId}/tests/{testId}",
			Summary:     "Get a single test result (with stack trace) by TestId via release ID",
			Description: "Resolves the matching RunResults rows for the release-ID substring, derives " +
				"a StartTime window for partition pruning, then returns the single TestResults row " +
				"matching the given TestId. Returns 404 when no matching test exists in scope.",
			Security: apiKeySecurity,
		}, h.getTestByRelease)
	}
}

func (h *AutomationTestResultsHandler) getAutomationTestResultsByRelease(
	ctx context.Context,
	input *GetAutomationTestResultsByReleaseInput,
) (*GetAutomationTestResultsOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getAutomationTestResultsByRelease",
		trace.WithAttributes(
			attribute.String("test.release_id", input.ReleaseID),
			attribute.String("test.environment", input.Environment),
			attribute.String("test.stack", input.Stack),
			attribute.String("test.stage", input.Stage),
			attribute.Int("test.attempt", int(input.Attempt)),
		),
	)
	defer span.End()

	if len(input.ReleaseID) < minReleaseIDLength {
		err := fmt.Errorf("releaseId must be at least %d characters", minReleaseIDLength)
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, err.Error())
	}

	query := &domain.AutomationTestResultsByReleaseQuery{
		ReleaseIDSubstring: input.ReleaseID,
		Environment:        input.Environment,
		Stack:              input.Stack,
		Stage:              input.Stage,
		Attempt:            input.Attempt,
	}

	result, err := h.reader.QueryAutomationTestResultsByRelease(ctx, query)
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

// runsToKeysAndWindow converts a slice of resolved RunResults rows into the
// shape QueryTests expects: a slice of join keys plus the [min, max + buffer]
// time window for partition pruning.
func runsToKeysAndWindow(
	runs []domain.AutomationTestRunResult,
) (keys []domain.ResolvedRunKey, minStart, maxFinish time.Time) {
	keys = make([]domain.ResolvedRunKey, 0, len(runs))
	for i, r := range runs {
		keys = append(keys, domain.ResolvedRunKey{
			ReleaseID:       r.ReleaseID,
			Attempt:         uint8(r.Attempt),
			Stage:           r.Stage,
			EnvironmentName: r.EnvironmentName,
			StackName:       r.StackName,
		})
		if i == 0 || r.StartTime.Before(minStart) {
			minStart = r.StartTime
		}
		if i == 0 || r.FinishTime.After(maxFinish) {
			maxFinish = r.FinishTime
		}
	}
	return keys, minStart, maxFinish.Add(finishTimeBuffer)
}

// runTestsQuery is the shared back half of getTestsByCorrelationID and
// getTestsByRelease. It runs a resolved set of runs through QueryTests and
// formats the response.
func (h *AutomationTestResultsHandler) runTestsQuery(
	ctx context.Context,
	span trace.Span,
	runs []domain.AutomationTestRunResult,
	status string,
	limit int,
	cursor string,
) (*domain.AutomationTestsResult, error) {
	if len(runs) == 0 {
		return &domain.AutomationTestsResult{Tests: nil, Count: 0}, nil
	}
	keys, minStart, maxFinish := runsToKeysAndWindow(runs)
	span.SetAttributes(
		attribute.Int("test.runs_count", len(keys)),
		attribute.String("test.min_start", minStart.Format(time.RFC3339Nano)),
		attribute.String("test.max_finish", maxFinish.Format(time.RFC3339Nano)),
	)
	return h.testsReader.QueryTests(ctx, &domain.AutomationTestsQuery{
		Runs:      keys,
		MinStart:  minStart,
		MaxFinish: maxFinish,
		Status:    status,
		Limit:     limit,
		Cursor:    cursor,
	})
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

	runsResult, err := h.reader.QueryAutomationTestResults(ctx, &domain.AutomationTestResultsQuery{
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

	testsResult, err := h.runTestsQuery(ctx, span, runsResult.Runs, status, input.Limit, input.Cursor)
	if err != nil {
		recordHandlerError(span, err)
		if errors.Is(err, domain.ErrInvalidTestsCursor) {
			return nil, huma.NewError(http.StatusBadRequest, "invalid cursor parameter")
		}
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	span.SetAttributes(attribute.Int("test.result_count", testsResult.Count))
	span.SetStatus(codes.Ok, "")

	out := &GetTestsOutput{}
	out.Body.Tests = testsResult.Tests
	if out.Body.Tests == nil {
		out.Body.Tests = []domain.AutomationTestResult{}
	}
	out.Body.Count = testsResult.Count
	if testsResult.NextCursor != "" {
		out.Body.NextPage = buildTestsByCorrelationIDNextPageURL(input, testsResult.NextCursor)
	}
	return out, nil
}

func (h *AutomationTestResultsHandler) getTestsByRelease(
	ctx context.Context,
	input *GetTestsByReleaseInput,
) (*GetTestsOutput, error) {
	status := resolveStatus(input.Status)
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getTestsByRelease",
		trace.WithAttributes(
			attribute.String("test.release_id", input.ReleaseID),
			attribute.String("test.environment", input.Environment),
			attribute.String("test.stack", input.Stack),
			attribute.String("test.stage", input.Stage),
			attribute.Int("test.attempt", int(input.Attempt)),
			attribute.String("test.status", status),
			attribute.Int("test.limit", input.Limit),
		),
	)
	defer span.End()

	if len(input.ReleaseID) < minReleaseIDLength {
		err := fmt.Errorf("releaseId must be at least %d characters", minReleaseIDLength)
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, err.Error())
	}

	runsResult, err := h.reader.QueryAutomationTestResultsByRelease(
		ctx,
		&domain.AutomationTestResultsByReleaseQuery{
			ReleaseIDSubstring: input.ReleaseID,
			Environment:        input.Environment,
			Stack:              input.Stack,
			Stage:              input.Stage,
			Attempt:            input.Attempt,
		},
	)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	testsResult, err := h.runTestsQuery(ctx, span, runsResult.Runs, status, input.Limit, input.Cursor)
	if err != nil {
		recordHandlerError(span, err)
		if errors.Is(err, domain.ErrInvalidTestsCursor) {
			return nil, huma.NewError(http.StatusBadRequest, "invalid cursor parameter")
		}
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	span.SetAttributes(attribute.Int("test.result_count", testsResult.Count))
	span.SetStatus(codes.Ok, "")

	out := &GetTestsOutput{}
	out.Body.Tests = testsResult.Tests
	if out.Body.Tests == nil {
		out.Body.Tests = []domain.AutomationTestResult{}
	}
	out.Body.Count = testsResult.Count
	if testsResult.NextCursor != "" {
		out.Body.NextPage = buildTestsByReleaseNextPageURL(input, testsResult.NextCursor)
	}
	return out, nil
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
	// Always include status so an explicit override survives pagination.
	if input.Status != "" {
		v.Set("status", input.Status)
	}
	return fmt.Sprintf(
		"/v1/automation-test-results/by-correlation/%s/tests?%s",
		url.PathEscape(input.CorrelationID),
		v.Encode(),
	)
}

// buildTestsByReleaseNextPageURL is the by-release counterpart.
func buildTestsByReleaseNextPageURL(input *GetTestsByReleaseInput, cursor string) string {
	v := url.Values{}
	v.Set("limit", strconv.Itoa(input.Limit))
	v.Set("cursor", cursor)
	setIfNonEmpty(v, "environment", input.Environment)
	setIfNonEmpty(v, "stack", input.Stack)
	setIfNonEmpty(v, "stage", input.Stage)
	if input.Attempt > 0 {
		v.Set("attempt", strconv.FormatUint(uint64(input.Attempt), 10))
	}
	if input.Status != "" {
		v.Set("status", input.Status)
	}
	return fmt.Sprintf(
		"/v1/automation-test-results/by-release/%s/tests?%s",
		url.PathEscape(input.ReleaseID),
		v.Encode(),
	)
}

func setIfNonEmpty(v url.Values, key, val string) {
	if val != "" {
		v.Set(key, val)
	}
}

// loadTestByID is the shared back half of getTestByCorrelationID and
// getTestByRelease. It expects already-resolved runs and the parsed TestId,
// and translates domain.ErrTestNotFound into a 404.
func (h *AutomationTestResultsHandler) loadTestByID(
	ctx context.Context,
	span trace.Span,
	runs []domain.AutomationTestRunResult,
	testID uuid.UUID,
) (*domain.AutomationTestResult, error) {
	if len(runs) == 0 {
		return nil, huma.NewError(http.StatusNotFound, "test not found")
	}
	keys, minStart, maxFinish := runsToKeysAndWindow(runs)
	span.SetAttributes(
		attribute.Int("test.runs_count", len(keys)),
		attribute.String("test.test_id", testID.String()),
	)
	res, err := h.testsReader.LoadTestByID(ctx, &domain.LoadTestByIDQuery{
		Runs:      keys,
		MinStart:  minStart,
		MaxFinish: maxFinish,
		TestID:    testID,
	})
	if err != nil {
		recordHandlerError(span, err)
		if errors.Is(err, domain.ErrTestNotFound) {
			return nil, huma.NewError(http.StatusNotFound, "test not found")
		}
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}
	return res, nil
}

func (h *AutomationTestResultsHandler) getTestByCorrelationID(
	ctx context.Context,
	input *GetTestByCorrelationIDInput,
) (*GetTestOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getTestByCorrelationID",
		trace.WithAttributes(
			attribute.String("test.correlation_id", input.CorrelationID),
			attribute.String("test.test_id", input.TestID),
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
	testID, err := uuid.Parse(input.TestID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, "invalid testId")
	}

	runsResult, err := h.reader.QueryAutomationTestResults(ctx, &domain.AutomationTestResultsQuery{
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

	res, err := h.loadTestByID(ctx, span, runsResult.Runs, testID)
	if err != nil {
		return nil, err
	}
	span.SetStatus(codes.Ok, "")
	return &GetTestOutput{Body: *res}, nil
}

func (h *AutomationTestResultsHandler) getTestByRelease(
	ctx context.Context,
	input *GetTestByReleaseInput,
) (*GetTestOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getTestByRelease",
		trace.WithAttributes(
			attribute.String("test.release_id", input.ReleaseID),
			attribute.String("test.test_id", input.TestID),
			attribute.String("test.environment", input.Environment),
			attribute.String("test.stack", input.Stack),
			attribute.String("test.stage", input.Stage),
			attribute.Int("test.attempt", int(input.Attempt)),
		),
	)
	defer span.End()

	if len(input.ReleaseID) < minReleaseIDLength {
		err := fmt.Errorf("releaseId must be at least %d characters", minReleaseIDLength)
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, err.Error())
	}
	testID, err := uuid.Parse(input.TestID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusBadRequest, "invalid testId")
	}

	runsResult, err := h.reader.QueryAutomationTestResultsByRelease(
		ctx,
		&domain.AutomationTestResultsByReleaseQuery{
			ReleaseIDSubstring: input.ReleaseID,
			Environment:        input.Environment,
			Stack:              input.Stack,
			Stage:              input.Stage,
			Attempt:            input.Attempt,
		},
	)
	if err != nil {
		recordHandlerError(span, err)
		return nil, huma.NewError(http.StatusInternalServerError, "internal error")
	}

	res, err := h.loadTestByID(ctx, span, runsResult.Runs, testID)
	if err != nil {
		return nil, err
	}
	span.SetStatus(codes.Ok, "")
	return &GetTestOutput{Body: *res}, nil
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

	query := &domain.AutomationTestResultsQuery{
		CorrelationID: correlationID,
		Environment:   input.Environment,
		Stack:         input.Stack,
		Stage:         input.Stage,
		Attempt:       input.Attempt,
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
