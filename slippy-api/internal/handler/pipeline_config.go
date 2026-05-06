package handler

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// PipelineConfigStep represents a single step in the pipeline configuration response.
type PipelineConfigStep struct {
	Name          string   `json:"name"                    doc:"Step name"`
	Prerequisites []string `json:"prerequisites,omitempty" doc:"Step names that must complete before this step"`
	IsAggregate   bool     `json:"is_aggregate,omitempty"  doc:"True if this step aggregates component-level data"`
	AggregateOf   string   `json:"aggregate_of,omitempty"  doc:"Component-level step name this step aggregates"`
	IsGate        bool     `json:"is_gate,omitempty"       doc:"True if this step is a pipeline gate"`
}

// PipelineConfigResponseBody is the response shape for the pipeline config endpoint.
type PipelineConfigResponseBody struct {
	Version string               `json:"version" doc:"Pipeline config schema version"`
	Name    string               `json:"name"    doc:"Pipeline name"`
	Steps   []PipelineConfigStep `json:"steps"   doc:"Ordered list of pipeline steps"`
}

// GetPipelineConfigOutput wraps the pipeline config response.
type GetPipelineConfigOutput struct {
	Body *PipelineConfigResponseBody
}

// PipelineConfigHandler holds the loaded pipeline config.
type PipelineConfigHandler struct {
	cfg *slippy.PipelineConfig
}

// NewPipelineConfigHandler creates a handler that serves the given pipeline config.
func NewPipelineConfigHandler(cfg *slippy.PipelineConfig) *PipelineConfigHandler {
	return &PipelineConfigHandler{cfg: cfg}
}

// RegisterPipelineConfigRoutes registers the pipeline-config endpoint on the given huma API.
func RegisterPipelineConfigRoutes(api huma.API, h *PipelineConfigHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-pipeline-config",
		Method:      http.MethodGet,
		Path:        "/pipeline-config",
		Summary:     "Get the server-side pipeline configuration",
		Security:    apiKeySecurity,
		Tags:        []string{"v1"},
	}, h.getPipelineConfig)
}

func (h *PipelineConfigHandler) getPipelineConfig(
	ctx context.Context,
	_ *struct{},
) (*GetPipelineConfigOutput, error) {
	_, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getPipelineConfig")
	defer span.End()

	if h.cfg == nil {
		span.SetStatus(codes.Error, "pipeline config not loaded")
		return nil, huma.NewError(http.StatusInternalServerError, "pipeline config not available")
	}

	steps := make([]PipelineConfigStep, len(h.cfg.Steps))
	for i, s := range h.cfg.Steps {
		steps[i] = PipelineConfigStep{
			Name:          s.Name,
			Prerequisites: s.Prerequisites,
			IsAggregate:   s.Aggregates != "",
			AggregateOf:   s.Aggregates,
			IsGate:        s.IsGate,
		}
	}

	span.SetStatus(codes.Ok, "")
	return &GetPipelineConfigOutput{
		Body: &PipelineConfigResponseBody{
			Version: h.cfg.Version,
			Name:    h.cfg.Name,
			Steps:   steps,
		},
	}, nil
}
