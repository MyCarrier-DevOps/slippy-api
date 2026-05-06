package handler

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// StepPrerequisitesInput captures path parameters for the prerequisites endpoint.
type StepPrerequisitesInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	StepName      string `path:"stepName"      doc:"Pipeline step name"`
}

// FailedPrereq carries the step name and the failure reason for a failed prerequisite.
type FailedPrereq struct {
	StepName string `json:"step_name"        doc:"Failed prerequisite step name"`
	Reason   string `json:"reason,omitempty" doc:"Failure reason from step state"`
}

// StepPrerequisitesResponseBody is the response for the step prerequisites endpoint.
type StepPrerequisitesResponseBody struct {
	Satisfied      bool                         `json:"satisfied"                 doc:"True when all prerequisites have reached a terminal success state"`
	Pending        []string                     `json:"pending,omitempty"         doc:"Prerequisite step names not yet in a terminal state"`
	Failed         []FailedPrereq               `json:"failed,omitempty"          doc:"Prerequisite step names that failed, with reasons"`
	PrereqStatuses map[string]domain.StepStatus `json:"prereq_statuses,omitempty" doc:"Status of each prerequisite step"`
}

// GetStepPrerequisitesOutput wraps the prerequisites response.
type GetStepPrerequisitesOutput struct {
	Body *StepPrerequisitesResponseBody
}

// StepPrerequisitesHandler evaluates prerequisite state for a given pipeline step.
type StepPrerequisitesHandler struct {
	reader domain.SlipReader
	cfg    *slippy.PipelineConfig
}

// NewStepPrerequisitesHandler creates a handler backed by the given reader and pipeline config.
func NewStepPrerequisitesHandler(
	reader domain.SlipReader,
	cfg *slippy.PipelineConfig,
) *StepPrerequisitesHandler {
	return &StepPrerequisitesHandler{reader: reader, cfg: cfg}
}

// RegisterStepPrerequisitesRoutes registers the step prerequisites endpoint.
func RegisterStepPrerequisitesRoutes(api huma.API, h *StepPrerequisitesHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-step-prerequisites",
		Method:      http.MethodGet,
		Path:        "/slips/{correlationID}/step-prerequisites/{stepName}",
		Summary:     "Check whether all prerequisites for a pipeline step are satisfied",
		Security:    apiKeySecurity,
		Tags:        []string{"v1"},
	}, h.getStepPrerequisites)
}

func (h *StepPrerequisitesHandler) getStepPrerequisites(
	ctx context.Context,
	input *StepPrerequisitesInput,
) (*GetStepPrerequisitesOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getStepPrerequisites",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.step_name", input.StepName),
		),
	)
	defer span.End()

	// Validate the step name against the pipeline config.
	if h.cfg == nil {
		span.SetStatus(codes.Error, "pipeline config not loaded")
		return nil, huma.NewError(http.StatusInternalServerError, "pipeline config not available")
	}

	stepCfg := h.cfg.GetStep(input.StepName)
	if stepCfg == nil {
		span.SetStatus(codes.Error, "unknown step")
		return nil, huma.NewError(http.StatusBadRequest, "unknown step: "+input.StepName)
	}

	// Load the slip.
	slip, err := h.reader.Load(ctx, input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, mapError(err)
	}

	// Evaluate each prerequisite. Aggregate steps reference their own slip.Steps key
	// (the aggregate step itself), so no special expansion is needed here — the
	// slip.Steps["builds"] entry represents the aggregate outcome.
	var (
		pending        []string
		failedPrereqs  []FailedPrereq
		prereqStatuses = make(map[string]domain.StepStatus, len(stepCfg.Prerequisites))
	)

	for _, prereq := range stepCfg.Prerequisites {
		step, ok := slip.Steps[prereq]
		if !ok {
			// Step not yet present in the slip — treat as pending.
			prereqStatuses[prereq] = slippy.StepStatusPending
			pending = append(pending, prereq)
			continue
		}
		prereqStatuses[prereq] = step.Status
		switch {
		case step.Status.IsSuccess():
			// satisfied — nothing to do
		case step.Status.IsFailure():
			failedPrereqs = append(failedPrereqs, FailedPrereq{
				StepName: prereq,
				Reason:   step.Error,
			})
		default:
			// running, pending, held, etc.
			pending = append(pending, prereq)
		}
	}

	satisfied := len(pending) == 0 && len(failedPrereqs) == 0

	span.SetStatus(codes.Ok, "")
	span.SetAttributes(
		attribute.Bool("prereq.satisfied", satisfied),
		attribute.Int("prereq.pending_count", len(pending)),
		attribute.Int("prereq.failed_count", len(failedPrereqs)),
	)

	return &GetStepPrerequisitesOutput{
		Body: &StepPrerequisitesResponseBody{
			Satisfied:      satisfied,
			Pending:        pending,
			Failed:         failedPrereqs,
			PrereqStatuses: prereqStatuses,
		},
	}, nil
}
