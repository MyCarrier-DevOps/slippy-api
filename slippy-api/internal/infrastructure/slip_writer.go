package infrastructure

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// writerTracerName is the instrumentation scope for write operations.
const writerTracerName = "slippy-api/writer"

// SlipWriterAdapter adapts the upstream slippy.Client to the domain.SlipWriter
// interface. It wraps the high-level business client (not the raw store) so that
// operations like CreateSlipForPush include ancestry resolution and step updates
// include atomic history appends.
type SlipWriterAdapter struct {
	client *slippy.Client
}

// NewSlipWriterAdapter wraps a slippy.Client as a SlipWriter.
func NewSlipWriterAdapter(client *slippy.Client) *SlipWriterAdapter {
	return &SlipWriterAdapter{client: client}
}

// Compile-time interface compliance check.
var _ domain.SlipWriter = (*SlipWriterAdapter)(nil)

func (a *SlipWriterAdapter) CreateSlipForPush(
	ctx context.Context,
	opts domain.PushOptions,
) (*domain.CreateSlipResult, error) {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.CreateSlipForPush",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", opts.CorrelationID),
			attribute.String("slip.repository", opts.Repository),
			attribute.String("slip.commit_sha", opts.CommitSHA),
		),
	)
	defer span.End()

	result, err := a.client.CreateSlipForPush(ctx, opts)
	if err != nil {
		recordWriterError(span, err)
		return nil, err
	}
	return result, nil
}

func (a *SlipWriterAdapter) StartStep(ctx context.Context, correlationID, stepName, componentName string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.StartStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	if err := a.client.StartStep(ctx, correlationID, stepName, componentName); err != nil {
		recordWriterError(span, err)
		return err
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(ctx, correlationID); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) CompleteStep(ctx context.Context, correlationID, stepName, componentName string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.CompleteStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	// Pipeline-level terminal events route directly: steps.go:101 guard fires
	// checkPipelineCompletion automatically, saving a redundant Load.
	// Component events MUST go through RunPostExecution to drive aggregate recomputation.
	if componentName != "" {
		if _, err := a.client.RunPostExecution(ctx, slippy.PostExecutionOptions{
			CorrelationID:     correlationID,
			StepName:          stepName,
			ComponentName:     componentName,
			WorkflowSucceeded: true,
		}); err != nil {
			recordWriterError(span, err)
			return err
		}
	} else {
		if err := a.client.CompleteStep(ctx, correlationID, stepName, componentName); err != nil {
			recordWriterError(span, err)
			return err
		}
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(ctx, correlationID); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) FailStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.FailStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	// Pipeline-level terminal events route directly: steps.go:101 guard fires
	// checkPipelineCompletion automatically, saving a redundant Load.
	// Component events MUST go through RunPostExecution to drive aggregate recomputation.
	if componentName != "" {
		if _, err := a.client.RunPostExecution(ctx, slippy.PostExecutionOptions{
			CorrelationID:     correlationID,
			StepName:          stepName,
			ComponentName:     componentName,
			WorkflowSucceeded: false,
			FailureMessage:    reason,
		}); err != nil {
			recordWriterError(span, err)
			return err
		}
	} else {
		if err := a.client.FailStep(ctx, correlationID, stepName, componentName, reason); err != nil {
			recordWriterError(span, err)
			return err
		}
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(ctx, correlationID); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) SkipStep(ctx context.Context, correlationID, stepName, componentName, reason string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.SkipStep",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.step_name", stepName),
			attribute.String("slip.component_name", componentName),
		),
	)
	defer span.End()

	if err := a.client.SkipStep(ctx, correlationID, stepName, componentName, reason); err != nil {
		recordWriterError(span, err)
		return err
	}
	if a.isPipelineStep(stepName, componentName) {
		if err := a.hydrateAndPersist(ctx, correlationID); err != nil {
			span.AddEvent("hydration failed", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}
	return nil
}

func (a *SlipWriterAdapter) SetComponentImageTag(
	ctx context.Context,
	correlationID, componentName, imageTag string,
) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.SetComponentImageTag",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.component_name", componentName),
			attribute.String("slip.image_tag", imageTag),
		),
	)
	defer span.End()

	if err := a.client.SetComponentImageTag(ctx, correlationID, componentName, imageTag); err != nil {
		recordWriterError(span, err)
		return err
	}
	return nil
}

// isPipelineStep reports whether a step transition should trigger a post-write
// hydration. Returns true only when componentName is empty (not a component-level
// event) and the step is not an aggregate step (aggregate steps invoke
// updateAggregateStatusFromComponentStatesWithHistory internally, which already
// calls Load + Update, so we must not duplicate that work).
func (a *SlipWriterAdapter) isPipelineStep(stepName, componentName string) bool {
	if componentName != "" {
		return false
	}
	if pipelineCfg := a.client.PipelineConfig(); pipelineCfg != nil {
		return !pipelineCfg.IsAggregateStep(stepName)
	}
	return true
}

// hydrateAndPersist reads the current slip state from ClickHouse (which invokes
// hydrateSlip to recompute all *_status columns from slip_component_states), then
// persists the hydrated row back via Update. This flushes correct *_status values
// for pipeline steps, whose AppendHistory path copies columns verbatim and would
// otherwise leave *_status stale until the next aggregate write.
//
// Update is called directly on the store rather than through Client because Client
// does not expose an Update method. This intentionally bypasses any future
// Client-level hooks around Update; revisit if Client gains such hooks.
//
// Errors are returned to the caller but treated as non-fatal — the step event is
// already durably recorded in slip_component_states.
func (a *SlipWriterAdapter) hydrateAndPersist(ctx context.Context, correlationID string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.hydrateAndPersist",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
		),
	)
	defer span.End()

	slip, err := a.client.Load(ctx, correlationID)
	if err != nil {
		recordWriterError(span, err)
		return fmt.Errorf("hydrateAndPersist: load failed: %w", err)
	}

	if err := a.client.Store().Update(ctx, slip); err != nil {
		recordWriterError(span, err)
		return fmt.Errorf("hydrateAndPersist: update failed: %w", err)
	}

	return nil
}

// recordWriterError records an error on a span, distinguishing client errors
// from server/infrastructure errors.
func recordWriterError(span trace.Span, err error) {
	span.RecordError(err)
	switch {
	case isClientError(err):
		span.SetStatus(codes.Unset, err.Error())
	default:
		span.SetStatus(codes.Error, fmt.Sprintf("write operation failed: %v", err))
	}
}
