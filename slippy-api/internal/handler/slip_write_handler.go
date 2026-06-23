package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// SlipWriteHandler holds dependencies for write route handlers.
type SlipWriteHandler struct {
	writer      domain.SlipWriter
	invalidator domain.Invalidator // nil-safe; skipped when nil
}

// NewSlipWriteHandler creates a handler backed by the given writer.
// invalidator is optional — pass nil when caching is not enabled.
func NewSlipWriteHandler(writer domain.SlipWriter, invalidator domain.Invalidator) *SlipWriteHandler {
	return &SlipWriteHandler{writer: writer, invalidator: invalidator}
}

// invalidate evicts the cached slip entry after a successful write.
// It is a no-op when no invalidator is configured.
func (h *SlipWriteHandler) invalidate(ctx context.Context, correlationID string) {
	if h.invalidator != nil {
		h.invalidator.InvalidateByCorrelationID(ctx, correlationID)
	}
}

// writeApiKeySecurity marks an operation as requiring write API key authentication.
var writeApiKeySecurity = []map[string][]string{{"writeApiKey": {}}}

// --- Input / Output types ------------------------------------------------

// ComponentDefinitionInput is a JSON-friendly DTO for component definitions.
// The upstream slippy.ComponentDefinition has no JSON tags, so we need this
// local type for proper request deserialization.
type ComponentDefinitionInput struct {
	Name           string `json:"name"                      doc:"Component identifier"`
	DockerfilePath string `json:"dockerfile_path,omitempty" doc:"Path to Dockerfile"`
}

// CreateSlipInput captures the request body for creating a routing slip.
type CreateSlipInput struct {
	Body struct {
		CorrelationID string                     `json:"correlation_id" doc:"Unique slip identifier (from Kafka event)"`
		Repository    string                     `json:"repository" doc:"Full repository name (owner/repo)"`
		Branch        string                     `json:"branch" doc:"Git branch name"`
		CommitSHA     string                     `json:"commit_sha" doc:"Full git commit SHA"`
		CommitMessage string                     `json:"commit_message,omitempty" doc:"Commit message (enables squash merge PR-based ancestry)"`
		Components    []ComponentDefinitionInput `json:"components,omitempty" doc:"Components to track in aggregate steps"`
	}
}

// CreateSlipOutput wraps the response for slip creation.
// Warnings are converted from []error to []string for JSON serialization.
type CreateSlipOutput struct {
	Body struct {
		Slip             *domain.Slip `json:"slip"`
		Warnings         []string     `json:"warnings,omitempty"`
		AncestryResolved bool         `json:"ancestry_resolved"`
	}
}

// StepBody is the optional request body for step start/complete endpoints.
type StepBody struct {
	ComponentName string `json:"component_name,omitempty" doc:"Component name (required for aggregate steps, empty for pipeline steps)"`
}

// StepInput captures path params and optional body for step start/complete.
// Body is a pointer so that the request body is optional in the OpenAPI spec —
// pipeline-level steps don't need a body at all.
type StepInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	StepName      string `path:"stepName"      doc:"Pipeline step name"`
	Body          *StepBody
}

// componentName returns the component name from the optional body, or empty string if no body.
func (s *StepInput) componentName() string {
	if s.Body == nil {
		return ""
	}
	return s.Body.ComponentName
}

// FailStepInput captures path params and body for step failure.
type FailStepInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	StepName      string `path:"stepName"      doc:"Pipeline step name"`
	Body          struct {
		ComponentName string `json:"component_name,omitempty" doc:"Component name (required for aggregate steps, empty for pipeline steps)"`
		Reason        string `json:"reason" doc:"Failure reason"`
	}
}

// SkipStepInput captures path params and body for step skip.
type SkipStepInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	StepName      string `path:"stepName"      doc:"Pipeline step name"`
	Body          *struct {
		ComponentName string `json:"component_name,omitempty" doc:"Component name (required for aggregate steps, empty for pipeline steps)"`
		Reason        string `json:"reason,omitempty" doc:"Skip reason"`
	}
}

// componentName returns the component name from the optional body, or empty string if no body.
func (s *SkipStepInput) componentName() string {
	if s.Body == nil {
		return ""
	}
	return s.Body.ComponentName
}

// reason returns the skip reason from the optional body, or empty string if no body.
func (s *SkipStepInput) reason() string {
	if s.Body == nil {
		return ""
	}
	return s.Body.Reason
}

// PromoteSlipInput captures path params and body for promoting a slip.
type PromoteSlipInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	Body          struct {
		PromotedTo string `json:"promoted_to" doc:"Correlation ID of the new slip on the target branch"`
	}
}

// AbandonSlipInput captures path params and body for abandoning a slip.
type AbandonSlipInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	Body          struct {
		SupersededBy string `json:"superseded_by" doc:"Correlation ID of the newer slip that supersedes this one"`
	}
}

// SetImageTagInput captures path params and body for setting an image tag.
type SetImageTagInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	ComponentName string `path:"componentName" doc:"Component name"`
	Body          struct {
		ImageTag string `json:"image_tag" doc:"Container image tag (e.g. 26.09.aef1234)"`
	}
}

// --- Route Registration --------------------------------------------------

// RegisterWriteRoutes registers all write-related routes on the given huma API.
func RegisterWriteRoutes(api huma.API, h *SlipWriteHandler) {
	huma.Register(api, huma.Operation{
		OperationID:   "create-slip",
		Method:        http.MethodPost,
		Path:          "/slips",
		Summary:       "Create a new routing slip for a push event",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusCreated,
		Tags:          []string{"v1"},
	}, h.createSlip)

	huma.Register(api, huma.Operation{
		OperationID:   "start-step",
		Method:        http.MethodPost,
		Path:          "/slips/{correlationID}/steps/{stepName}/start",
		Summary:       "Mark a pipeline step as running",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.startStep)

	huma.Register(api, huma.Operation{
		OperationID:   "complete-step",
		Method:        http.MethodPost,
		Path:          "/slips/{correlationID}/steps/{stepName}/complete",
		Summary:       "Mark a pipeline step as completed",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.completeStep)

	huma.Register(api, huma.Operation{
		OperationID:   "fail-step",
		Method:        http.MethodPost,
		Path:          "/slips/{correlationID}/steps/{stepName}/fail",
		Summary:       "Mark a pipeline step as failed",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.failStep)

	huma.Register(api, huma.Operation{
		OperationID:   "skip-step",
		Method:        http.MethodPost,
		Path:          "/slips/{correlationID}/steps/{stepName}/skip",
		Summary:       "Mark a pipeline step as skipped",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.skipStep)

	huma.Register(api, huma.Operation{
		OperationID:   "set-image-tag",
		Method:        http.MethodPut,
		Path:          "/slips/{correlationID}/components/{componentName}/image-tag",
		Summary:       "Record the built container image tag for a component",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.setImageTag)

	huma.Register(api, huma.Operation{
		OperationID:   "promote-slip",
		Method:        http.MethodPost,
		Path:          "/slips/{correlationID}/promote",
		Summary:       "Mark a routing slip as promoted to another branch",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.promoteSlip)

	huma.Register(api, huma.Operation{
		OperationID:   "abandon-slip",
		Method:        http.MethodPost,
		Path:          "/slips/{correlationID}/abandon",
		Summary:       "Mark a routing slip as abandoned, superseded by a newer push",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.abandonSlip)
}

// --- Handlers ------------------------------------------------------------

func (h *SlipWriteHandler) createSlip(ctx context.Context, input *CreateSlipInput) (*CreateSlipOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.createSlip",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.Body.CorrelationID),
			attribute.String("slip.repository", input.Body.Repository),
			attribute.String("slip.branch", input.Body.Branch),
			attribute.String("slip.commit_sha", input.Body.CommitSHA),
			attribute.Int("slip.components_count", len(input.Body.Components)),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "slip: create",
		"correlation_id", input.Body.CorrelationID,
		"repository", input.Body.Repository,
		"branch", input.Body.Branch,
		"commit_sha", input.Body.CommitSHA,
		"components_count", len(input.Body.Components))

	components := make([]domain.ComponentDefinition, len(input.Body.Components))
	for i, c := range input.Body.Components {
		components[i] = domain.ComponentDefinition{
			Name:           c.Name,
			DockerfilePath: c.DockerfilePath,
		}
	}

	result, err := h.writer.CreateSlipForPush(ctx, domain.PushOptions{
		CorrelationID: input.Body.CorrelationID,
		Repository:    input.Body.Repository,
		Branch:        input.Body.Branch,
		CommitSHA:     input.Body.CommitSHA,
		CommitMessage: input.Body.CommitMessage,
		Components:    components,
	})
	if err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "slip: create failed",
			"correlation_id", input.Body.CorrelationID,
			"repository", input.Body.Repository, "error", err)
		return nil, mapWriteError(err)
	}

	span.SetAttributes(
		attribute.Bool("slip.ancestry_resolved", result.AncestryResolved),
		attribute.Int("slip.warnings_count", len(result.Warnings)),
	)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "slip: created",
		"correlation_id", input.Body.CorrelationID,
		"repository", input.Body.Repository,
		"ancestry_resolved", result.AncestryResolved,
		"warnings_count", len(result.Warnings))

	h.invalidate(ctx, input.Body.CorrelationID)
	out := &CreateSlipOutput{}
	out.Body.Slip = result.Slip
	out.Body.AncestryResolved = result.AncestryResolved
	for _, w := range result.Warnings {
		out.Body.Warnings = append(out.Body.Warnings, w.Error())
	}
	return out, nil
}

//nolint:dupl // startStep and completeStep share intentional parallel structure; the operations are semantically distinct.
func (h *SlipWriteHandler) startStep(ctx context.Context, input *StepInput) (*struct{}, error) {
	componentName := input.componentName()
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.startStep",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.step_name", input.StepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "step: start",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", componentName)

	if err := h.writer.StartStep(ctx, input.CorrelationID, input.StepName, componentName); err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "step: start failed",
			"correlation_id", input.CorrelationID,
			"step", input.StepName, "component", componentName, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "step: started",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", componentName)
	return &struct{}{}, nil
}

//nolint:dupl // completeStep and startStep share intentional parallel structure; the operations are semantically distinct.
func (h *SlipWriteHandler) completeStep(ctx context.Context, input *StepInput) (*struct{}, error) {
	componentName := input.componentName()
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.completeStep",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.step_name", input.StepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "step: complete",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", componentName)

	if err := h.writer.CompleteStep(ctx, input.CorrelationID, input.StepName, componentName); err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "step: complete failed",
			"correlation_id", input.CorrelationID,
			"step", input.StepName, "component", componentName, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "step: completed",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", componentName)
	return &struct{}{}, nil
}

func (h *SlipWriteHandler) failStep(ctx context.Context, input *FailStepInput) (*struct{}, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.failStep",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.step_name", input.StepName),
			attribute.String("slip.component_name", input.Body.ComponentName),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "step: fail",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", input.Body.ComponentName,
		"reason", input.Body.Reason)

	if err := h.writer.FailStep(
		ctx,
		input.CorrelationID,
		input.StepName,
		input.Body.ComponentName,
		input.Body.Reason,
	); err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "step: fail failed",
			"correlation_id", input.CorrelationID,
			"step", input.StepName, "component", input.Body.ComponentName, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "step: failed (recorded)",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", input.Body.ComponentName)
	return &struct{}{}, nil
}

func (h *SlipWriteHandler) skipStep(ctx context.Context, input *SkipStepInput) (*struct{}, error) {
	componentName := input.componentName()
	reason := input.reason()
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.skipStep",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.step_name", input.StepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "step: skip",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", componentName, "reason", reason)

	if err := h.writer.SkipStep(ctx, input.CorrelationID, input.StepName, componentName, reason); err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "step: skip failed",
			"correlation_id", input.CorrelationID,
			"step", input.StepName, "component", componentName, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "step: skipped",
		"correlation_id", input.CorrelationID,
		"step", input.StepName, "component", componentName)
	return &struct{}{}, nil
}

func (h *SlipWriteHandler) setImageTag(ctx context.Context, input *SetImageTagInput) (*struct{}, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.setImageTag",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.component_name", input.ComponentName),
			attribute.String("slip.image_tag", input.Body.ImageTag),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "image_tag: set",
		"correlation_id", input.CorrelationID,
		"component", input.ComponentName, "image_tag", input.Body.ImageTag)

	if err := h.writer.SetComponentImageTag(
		ctx,
		input.CorrelationID,
		input.ComponentName,
		input.Body.ImageTag,
	); err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "image_tag: set failed",
			"correlation_id", input.CorrelationID,
			"component", input.ComponentName, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "image_tag: set ok",
		"correlation_id", input.CorrelationID,
		"component", input.ComponentName, "image_tag", input.Body.ImageTag)
	return &struct{}{}, nil
}

//nolint:dupl // promoteSlip and abandonSlip share intentional parallel structure; the operations are semantically distinct.
func (h *SlipWriteHandler) promoteSlip(ctx context.Context, input *PromoteSlipInput) (*struct{}, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.promoteSlip",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.promoted_to", input.Body.PromotedTo),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "slip: promote",
		"correlation_id", input.CorrelationID, "promoted_to", input.Body.PromotedTo)

	if err := h.writer.PromoteSlip(ctx, input.CorrelationID, input.Body.PromotedTo); err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "slip: promote failed",
			"correlation_id", input.CorrelationID,
			"promoted_to", input.Body.PromotedTo, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "slip: promoted",
		"correlation_id", input.CorrelationID, "promoted_to", input.Body.PromotedTo)
	return &struct{}{}, nil
}

//nolint:dupl // abandonSlip and promoteSlip share intentional parallel structure; the operations are semantically distinct.
func (h *SlipWriteHandler) abandonSlip(ctx context.Context, input *AbandonSlipInput) (*struct{}, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.abandonSlip",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.superseded_by", input.Body.SupersededBy),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "slip: abandon",
		"correlation_id", input.CorrelationID, "superseded_by", input.Body.SupersededBy)

	if err := h.writer.AbandonSlip(ctx, input.CorrelationID, input.Body.SupersededBy); err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "slip: abandon failed",
			"correlation_id", input.CorrelationID,
			"superseded_by", input.Body.SupersededBy, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "slip: abandoned",
		"correlation_id", input.CorrelationID, "superseded_by", input.Body.SupersededBy)
	return &struct{}{}, nil
}

// --- Error Mapping -------------------------------------------------------

// mapWriteError converts domain/store errors to huma status errors for write ops.
//
// Sentinel ordering: ErrTerminalAlreadyExists and ErrCorrIDWriteInProgress are
// checked BEFORE the default errors.As(*slippy.StepError) branch (plan v3
// §C.1) so the two I5-specific 409 mappings take precedence — both sentinels
// are wrapped in a *slippy.StepError by the library and a naive As-only
// branch would mis-map them to 422 Unprocessable Entity.
func mapWriteError(err error) error {
	switch {
	case errors.Is(err, slippy.ErrSlipNotFound):
		return huma.NewError(http.StatusNotFound, "slip not found")
	case errors.Is(err, slippy.ErrInvalidCorrelationID):
		return huma.NewError(http.StatusBadRequest, "invalid correlation ID")
	case errors.Is(err, domain.ErrInvalidCorrelationID):
		return huma.NewError(http.StatusBadRequest, "invalid correlation_id format")
	case errors.Is(err, slippy.ErrInvalidRepository):
		return huma.NewError(http.StatusBadRequest, "invalid repository")
	case errors.Is(err, slippy.ErrInvalidConfiguration):
		return huma.NewError(http.StatusBadRequest, "invalid configuration")
	case errors.Is(err, domain.ErrCreationInProgress):
		return huma.NewError(
			http.StatusConflict,
			"slip creation already in progress for this commit; duplicate suppressed",
		)
	case errors.Is(err, slippy.ErrTerminalAlreadyExists):
		// Option 1 INSERT-time gate refused the transition (plan v3 §B.2, §C.1).
		// 409 signals an idempotent retry by the caller is the right action —
		// the prior terminal status is the durable truth and a same-or-newer
		// terminal write would have been allowed by the §D matrix.
		return huma.NewError(
			http.StatusConflict,
			"step already in terminal state; transition rejected (I5 invariant)",
		)
	case errors.Is(err, domain.ErrCorrIDWriteInProgress):
		// Per-correlationID lock miss (plan v3 §M.1, §C.1). 409 is the chosen
		// status code (plan v3 §M.7 option (a)) — Slippy CLI PR 3 will add
		// bounded retry-with-jitter on 409 so legitimate contention does not
		// surface as workflow failure. Until PR 3 lands, SLIPPY_I5_LOCK_ENABLED
		// MUST stay false (plan v3 §G.1).
		return huma.NewError(
			http.StatusConflict,
			"write to slip in progress; retry with backoff",
		)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Infrastructure-level cancellation (client disconnect, LB idle-timeout,
		// upstream ClickHouse cancel). Return 504 so callers know to retry.
		// Without this branch the error would unwrap into the *StepError /
		// *SlipError default case below and surface as a misleading 422.
		return huma.NewError(http.StatusGatewayTimeout, "upstream timeout")
	default:
		if strings.Contains(err.Error(), "invalid push options") {
			return huma.NewError(http.StatusBadRequest, err.Error())
		}
		var stepErr *slippy.StepError
		if errors.As(err, &stepErr) {
			return huma.NewError(http.StatusUnprocessableEntity, stepErr.Error())
		}
		var slipErr *slippy.SlipError
		if errors.As(err, &slipErr) {
			return huma.NewError(http.StatusUnprocessableEntity, slipErr.Error())
		}
		return huma.NewError(http.StatusInternalServerError, "internal error")
	}
}
