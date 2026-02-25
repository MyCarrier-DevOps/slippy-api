package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// ImageTagHandler holds dependencies for image tag route handlers.
type ImageTagHandler struct {
	imageTagReader domain.ImageTagReader
}

// NewImageTagHandler creates a handler backed by the given ImageTagReader.
func NewImageTagHandler(imageTagReader domain.ImageTagReader) *ImageTagHandler {
	return &ImageTagHandler{imageTagReader: imageTagReader}
}

// --- Input / Output types ------------------------------------------------

// GetImageTagsInput captures the path parameter for resolving image tags.
type GetImageTagsInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
}

// GetImageTagsOutput wraps the resolved image tags response.
type GetImageTagsOutput struct {
	Body *domain.ImageTagResult
}

// --- Route Registration --------------------------------------------------

// RegisterImageTagRoutes registers image tag routes on the given huma API.
func RegisterImageTagRoutes(api huma.API, h *ImageTagHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-image-tags",
		Method:      http.MethodGet,
		Path:        "/slips/{correlationID}/image-tags",
		Summary:     "Resolve per-component image tags for a routing slip",
		Description: "Looks up the slip by correlation ID, determines build_scope from " +
			"ci.repoproperties, and returns per-component image tags. " +
			"For build_scope=all, all components share the slip-computed tag (YY.WW.SHA7). " +
			"For build_scope=modified, each component carries its actual tag from ci.buildinfo.",
		Security: apiKeySecurity,
	}, h.getImageTags)
}

// --- Handler -------------------------------------------------------------

func (h *ImageTagHandler) getImageTags(ctx context.Context, input *GetImageTagsInput) (*GetImageTagsOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getImageTags",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
		),
	)
	defer span.End()

	result, err := h.imageTagReader.ResolveImageTags(ctx, input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, mapImageTagError(err)
	}

	span.SetAttributes(
		attribute.String("image_tag.build_scope", result.BuildScope),
		attribute.String("image_tag.slip_tag", result.SlipTag),
		attribute.Int("image_tag.component_count", len(result.Tags)),
	)
	span.SetStatus(codes.Ok, "")
	return &GetImageTagsOutput{Body: result}, nil
}

// mapImageTagError converts image-tag-related errors to huma status errors.
func mapImageTagError(err error) error {
	switch {
	case errors.Is(err, slippy.ErrSlipNotFound):
		return huma.NewError(http.StatusNotFound, "slip not found")
	case errors.Is(err, slippy.ErrInvalidCorrelationID):
		return huma.NewError(http.StatusBadRequest, "invalid correlation ID")
	default:
		if err.Error() == "no ci.buildinfo rows found for correlation ID" {
			return huma.NewError(http.StatusNotFound, "no build info found for correlation ID")
		}
		return huma.NewError(http.StatusInternalServerError, "internal error")
	}
}
