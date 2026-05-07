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

// forceOverwriteKey is the unexported context key for bypassing the terminal guard.
type forceOverwriteKey struct{}

// WithForceOverwrite returns a context that instructs SlipWriterAdapter to skip
// the terminal-overwrite guard on CompleteStep / FailStep. Use sparingly — the
// primary consumer is an explicit operator escape hatch via HTTP header.
func WithForceOverwrite(ctx context.Context) context.Context {
	return context.WithValue(ctx, forceOverwriteKey{}, true)
}

// isForceOverwrite reports whether the context carries the force-overwrite flag.
func isForceOverwrite(ctx context.Context) bool {
	v, ok := ctx.Value(forceOverwriteKey{}).(bool)
	return ok && v
}

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

	// Terminal-overwrite guard: prevent silently clobbering an already-terminal step.
	if !isForceOverwrite(ctx) {
		idempotent, err := a.checkTerminalGuard(ctx, correlationID, stepName, componentName, slippy.StepStatusCompleted)
		if err != nil {
			recordWriterError(span, err)
			return err
		}
		if idempotent {
			return nil // same status already recorded — safe no-op
		}
	}

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

	// Terminal-overwrite guard: prevent silently clobbering an already-terminal step.
	if !isForceOverwrite(ctx) {
		idempotent, err := a.checkTerminalGuard(ctx, correlationID, stepName, componentName, slippy.StepStatusFailed)
		if err != nil {
			recordWriterError(span, err)
			return err
		}
		if idempotent {
			return nil // same status already recorded — safe no-op
		}
	}

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

func (a *SlipWriterAdapter) PromoteSlip(ctx context.Context, correlationID, promotedTo string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.PromoteSlip",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.promoted_to", promotedTo),
		),
	)
	defer span.End()

	if err := a.client.PromoteSlip(ctx, correlationID, promotedTo); err != nil {
		recordWriterError(span, err)
		return err
	}
	return nil
}

func (a *SlipWriterAdapter) AbandonSlip(ctx context.Context, correlationID, supersededBy string) error {
	ctx, span := otel.Tracer(writerTracerName).Start(ctx, "writer.AbandonSlip",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("slip.correlation_id", correlationID),
			attribute.String("slip.superseded_by", supersededBy),
		),
	)
	defer span.End()

	if err := a.client.AbandonSlip(ctx, correlationID, supersededBy); err != nil {
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

// checkTerminalGuard loads the current slip and checks whether (stepName,
// componentName) is already in a terminal state.
//
// Returns:
//   - (true, nil)  — current status equals requestedStatus; caller should no-op (idempotent).
//   - (false, err) — current status is terminal but differs; returns *domain.StepAlreadyTerminalError.
//   - (false, nil) — not yet terminal; caller should proceed with the write.
//
// If the slip cannot be loaded (e.g. not found, store error), the error is
// returned directly so the outer operation fails with a meaningful error.
// Absence of the step in the slip (pending/unknown) is treated as non-terminal.
func (a *SlipWriterAdapter) checkTerminalGuard(
	ctx context.Context,
	correlationID, stepName, componentName string,
	requestedStatus slippy.StepStatus,
) (idempotent bool, err error) {
	slip, err := a.client.Load(ctx, correlationID)
	if err != nil {
		return false, fmt.Errorf("checkTerminalGuard: load failed: %w", err)
	}

	currentStatus := a.resolveCurrentStatus(slip, stepName, componentName)
	if !currentStatus.IsTerminal() {
		return false, nil
	}
	if currentStatus == requestedStatus {
		return true, nil // idempotent: same terminal status already written
	}
	return false, &domain.StepAlreadyTerminalError{
		StepName:        stepName,
		ComponentName:   componentName,
		CurrentStatus:   currentStatus,
		RequestedStatus: requestedStatus,
	}
}

// resolveCurrentStatus returns the current StepStatus for the (stepName,
// componentName) pair from a loaded slip.
//
// For pipeline-level steps (componentName == ""), it looks up slip.Steps[stepName].
// For component-level steps (componentName != ""), it scans slip.Aggregates[stepName]
// for a matching component entry.
//
// Returns the empty StepStatus ("") when the step has no recorded state, which
// IsTerminal() returns false for, so the guard will not fire.
func (a *SlipWriterAdapter) resolveCurrentStatus(
	slip *slippy.Slip,
	stepName, componentName string,
) slippy.StepStatus {
	if componentName == "" {
		if step, ok := slip.Steps[stepName]; ok {
			return step.Status
		}
		return ""
	}
	// Component-level step: search aggregates.
	for _, compData := range slip.Aggregates[stepName] {
		if compData.Component == componentName {
			return compData.Status
		}
	}
	return ""
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
