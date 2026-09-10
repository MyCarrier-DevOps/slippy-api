package handler

import (
	"context"
	"errors"
	"fmt"
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
	Name           string `json:"name"                      maxLength:"256"  doc:"Component identifier"`
	DockerfilePath string `json:"dockerfile_path,omitempty" maxLength:"1024" doc:"Path to Dockerfile"`
}

// CreateSlipInput captures the request body for creating a routing slip.
type CreateSlipInput struct {
	Body struct {
		// Deliberately untagged: validateCorrelationIDFormat below already owns emptiness,
		// length and the character set for this field, and returns 400 with a precise
		// message. Duplicating the rule as schema tags would only move an existing 400 to
		// a 422 and split the rule across two places. The *references* to a correlation ID
		// — promoted_to, superseded_by — do carry tags, because nothing validated them.
		CorrelationID string `json:"correlation_id" doc:"Unique slip identifier (from Kafka event)"`
		Repository    string `json:"repository" maxLength:"256" doc:"Full repository name (owner/repo)"`
		Branch        string `json:"branch" maxLength:"512" doc:"Git branch name"`
		// No pattern on commit_sha: the resolver deliberately accepts short refs as well
		// as full SHAs (isFullCommitSHA), so a strict 40-hex pattern would reject callers
		// the service supports. 64 leaves room for SHA-256 object names.
		CommitSHA string `json:"commit_sha" maxLength:"64" doc:"Git commit SHA"`
		// Generous: commit messages drive squash-merge PR ancestry, so truncating them
		// would degrade slip resolution. 16 KiB bounds the field without touching real use.
		CommitMessage string                     `json:"commit_message,omitempty" maxLength:"16384" doc:"Commit message (enables squash merge PR-based ancestry)"`
		Components    []ComponentDefinitionInput `json:"components,omitempty" maxItems:"100" doc:"Components to track in aggregate steps"`
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
	ComponentName string `json:"component_name,omitempty" maxLength:"256" doc:"Component name (required for aggregate steps, empty for pipeline steps)"`
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
		ComponentName string `json:"component_name,omitempty" maxLength:"256" doc:"Component name (required for aggregate steps, empty for pipeline steps)"`
		// Reasons are appended to the slip's state_history jsonb, which is rewritten whole
		// on every append — so an unbounded reason is unbounded, quadratic growth in the
		// document the platform treats as the authority on step completion.
		Reason string `json:"reason" maxLength:"4096" doc:"Failure reason"`
	}
}

// SkipStepInput captures path params and body for step skip.
type SkipStepInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	StepName      string `path:"stepName"      doc:"Pipeline step name"`
	Body          *struct {
		ComponentName string `json:"component_name,omitempty" maxLength:"256" doc:"Component name (required for aggregate steps, empty for pipeline steps)"`
		Reason        string `json:"reason,omitempty" maxLength:"4096" doc:"Skip reason"`
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
		PromotedTo string `json:"promoted_to" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$" doc:"Correlation ID of the new slip on the target branch"`
	}
}

// AbandonSlipInput captures path params and body for abandoning a slip.
type AbandonSlipInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	Body          struct {
		SupersededBy string `json:"superseded_by" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$" doc:"Correlation ID of the newer slip that supersedes this one"`
	}
}

// ClaimSlipInput captures path params and body for claiming an adopted slip.
type ClaimSlipInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	Body          struct {
		// Required: an unattributed claim is not much use to the operator reading
		// the history later, which is the whole point of the marker.
		ClaimedBy string `json:"claimed_by" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9._:/-]+$" doc:"Adopter that is taking over this slip (e.g. \"rerunner\"); recorded as the history entry's actor"`
		Reason    string `json:"reason,omitempty" maxLength:"512" doc:"Optional scope of the adopted work (e.g. \"retrigger builds and unit tests\")"`
	}
}

// SetImageTagInput captures path params and body for setting an image tag.
type SetImageTagInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	ComponentName string `path:"componentName" doc:"Component name"`
	Body          struct {
		ImageTag string `json:"image_tag" maxLength:"256" doc:"Container image tag (e.g. 26.09.aef1234)"`
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

	huma.Register(api, huma.Operation{
		OperationID: "claim-slip",
		Method:      http.MethodPost,
		Path:        "/slips/{correlationID}/claim",
		Summary:     "Claim an adopted routing slip before dispatching work against it",
		Description: "Records that the caller has work in flight against a slip it did not create: " +
			"appends an adoption marker to the slip's state history and sets its status to in_progress. " +
			"Adopters MUST call this before dispatching any workflow. An ended slip stays eligible to be " +
			"replaced by a same-commit push, which deletes the row out from under in-flight work; a claimed " +
			"slip is live, so such a push deduplicates onto it instead. A non-2xx response means the claim " +
			"is not confirmed and the caller must dispatch nothing; it does not mean the slip is unclaimed, " +
			"because the writes are not cancelled by a client timeout. Claiming again is the recovery: a " +
			"repeat claim on a slip that is already in_progress is a no-op, never a conflict. Claiming a " +
			"failed slip also removes it from consumer-side stranded-slip protection that keys on failed, so " +
			"the adopter is exposed to a concurrent force-push or branch delete for the life of its run.",
		Security:      writeApiKeySecurity,
		DefaultStatus: http.StatusNoContent,
		Tags:          []string{"v1"},
	}, h.claimSlip)
}

// --- Validation ----------------------------------------------------------

// correlationIDMaxLen is the maximum permitted length for a correlation ID.
const correlationIDMaxLen = 128

// validateCorrelationIDFormat returns a 400 huma error when s is empty,
// exceeds correlationIDMaxLen characters, or contains any character outside
// the allowed set [A-Za-z0-9._:-]. The check is intentionally NOT a UUID
// parse — it enforces the bounded allow-list requested in code review (PR
// #42). Empty rejection is explicit here rather than relying on downstream
// library behavior (defense-in-depth).
func validateCorrelationIDFormat(s string) error {
	if s == "" {
		return huma.NewError(http.StatusBadRequest, "correlation_id must not be empty")
	}
	if len(s) > correlationIDMaxLen {
		return huma.NewError(http.StatusBadRequest,
			fmt.Sprintf("correlation_id exceeds maximum length of %d characters", correlationIDMaxLen))
	}
	for _, r := range s {
		if (r < 'A' || r > 'Z') &&
			(r < 'a' || r > 'z') &&
			(r < '0' || r > '9') &&
			r != '.' && r != '_' && r != ':' && r != '-' {
			return huma.NewError(http.StatusBadRequest,
				fmt.Sprintf("correlation_id contains invalid character %q; allowed: [A-Za-z0-9._:-]", r))
		}
	}
	return nil
}

// --- Handlers ------------------------------------------------------------

func (h *SlipWriteHandler) createSlip(ctx context.Context, input *CreateSlipInput) (*CreateSlipOutput, error) {
	if err := validateCorrelationIDFormat(input.Body.CorrelationID); err != nil {
		return nil, err
	}
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
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
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
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
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
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
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
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
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
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
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
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
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
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
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

func (h *SlipWriteHandler) claimSlip(ctx context.Context, input *ClaimSlipInput) (*struct{}, error) {
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.claimSlip",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("slip.claimed_by", input.Body.ClaimedBy),
		),
	)
	defer span.End()

	slog.InfoContext(ctx, "slip: claim",
		"correlation_id", input.CorrelationID,
		"claimed_by", input.Body.ClaimedBy, "reason", input.Body.Reason)

	if err := h.writer.ClaimSlip(ctx, input.CorrelationID, input.Body.ClaimedBy, input.Body.Reason); err != nil {
		recordHandlerError(span, err)
		// Logged at Error, not Warn, even for the expected not-found: the caller is
		// about to abandon a dispatch it was asked to perform, and the operator who
		// triggered the rerun needs the correlation ID to find out why nothing ran.
		slog.ErrorContext(ctx, "slip: claim failed",
			"correlation_id", input.CorrelationID,
			"claimed_by", input.Body.ClaimedBy, "error", err)
		return nil, mapWriteError(err)
	}
	h.invalidate(ctx, input.CorrelationID)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "slip: claimed",
		"correlation_id", input.CorrelationID, "claimed_by", input.Body.ClaimedBy)
	return &struct{}{}, nil
}

// --- Error Mapping -------------------------------------------------------

// mapWriteError converts domain/store errors to huma status errors for write ops.
func mapWriteError(err error) error {
	switch {
	case errors.Is(err, slippy.ErrSlipNotFound):
		// Deliberately more than "slip not found". On a WRITE the overwhelmingly
		// likely cause is not a typo in the correlation ID but that the row was
		// replaced: a same-commit push repaves an ended slip by deleting it and
		// inserting a fresh one under a new correlation ID, so a straggling write
		// from the superseded run arrives here with nowhere to land. Nothing
		// distinguishes the two cases at this layer — the row is simply gone, and
		// DEVOPS-277 declined to leave a tombstone behind — so the message names
		// the likely cause and the recovery instead of guessing (DEVOPS-285).
		return huma.NewError(
			http.StatusNotFound,
			"no routing slip with this correlation ID; it was either never created or has been "+
				"replaced by a newer run for the same commit. Re-read the slip for this "+
				"repository and commit to get the current correlation ID before retrying",
		)
	case errors.Is(err, slippy.ErrInvalidCorrelationID):
		return huma.NewError(http.StatusBadRequest, "invalid correlation ID")
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
		return huma.NewError(
			http.StatusConflict,
			"step already in terminal state; transition rejected (I5 freshness gate)",
		)
	case errors.Is(err, domain.ErrWriteContended):
		// Per-slip write-lock contention that outlasted the DB lock_timeout and the
		// in-adapter retries (translated from the driver error at the adapter boundary).
		// The transaction rolled back with nothing applied, so it is safe to retry —
		// surface it as retryable rather than a generic 500.
		return huma.NewError(
			http.StatusServiceUnavailable,
			"write contended on the per-slip lock and did not complete; safe to retry",
		)
	case errors.Is(err, domain.ErrStatementTimeout):
		// Server-side statement_timeout fired — a slow query, not a client cancel.
		return huma.NewError(http.StatusGatewayTimeout, "database statement timeout")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// A context deadline/cancellation that reaches mapWriteError means the
		// AUTHORITATIVE insert (UpdateStepWithHistory / slip_component_states) did NOT
		// land — the write-op timeout fired before the row was durable.
		//
		// This branch is only reachable for the start/skip and pipeline-level
		// complete/fail paths (componentName == ""). For those paths, UpdateStepWithStatus
		// calls store.UpdateStepWithHistory directly; a timeout there propagates as a
		// *StepError wrapping context.DeadlineExceeded, which errors.Is unwraps here.
		// checkPipelineCompletion errors for pipeline-level steps are already swallowed
		// in steps.go (logged as Warn, not returned), so they never reach this point.
		//
		// For component-level complete/fail (componentName != ""), the adapter routes
		// through goLib's RunPostExecution, which calls store.UpdateStepWithHistory first
		// (durable), then checkPipelineCompletion on the same write-op-bounded context.
		// A timeout inside checkPipelineCompletion is wrapped by goLib as ErrSlipNotFound
		// (→ 404) or ErrSlipStatusUpdateFailed (→ 500) before it returns, so a raw
		// context.DeadlineExceeded is NOT produced for that sub-path. In other words,
		// for component events: the component row is durable when this branch fires, but
		// it will not be reached — the 404/500 cases above win instead. The residual gap
		// (slip-level completion may not advance if the post-insert completion-check times
		// out) is tracked as a follow-up: derive slip-completion on Load so step/aggregate
		// status self-heals without a separate slip-status write.
		//
		// Returning 504 tells the CLI to re-POST, preventing silent data loss on the
		// insert-failure paths. Without this branch the error would unwrap into
		// *StepError/*SlipError and surface as a misleading 422.
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
