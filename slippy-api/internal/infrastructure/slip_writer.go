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

	if err := a.client.CompleteStep(ctx, correlationID, stepName, componentName); err != nil {
		recordWriterError(span, err)
		return err
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

	if err := a.client.FailStep(ctx, correlationID, stepName, componentName, reason); err != nil {
		recordWriterError(span, err)
		return err
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
