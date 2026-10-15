package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// maxRollbackBodyBytes bounds the record-rollback body. The largest legitimate body is about
// 9 KiB (a 1000-character message and outcome, up to 4 bytes per character, plus short fields),
// so 16 KiB leaves headroom while refusing anything that is not a rollback record before it is
// parsed.
const maxRollbackBodyBytes = 16 * 1024

// RollbackHandler serves the rollback record routes (DEVOPS-320): gitops-rollback records one
// row per rollback attempt, and reporting reads them back.
type RollbackHandler struct {
	store domain.RollbackStore
}

// NewRollbackHandler creates a handler backed by the given store.
func NewRollbackHandler(store domain.RollbackStore) *RollbackHandler {
	return &RollbackHandler{store: store}
}

// --- Input / Output types ------------------------------------------------

// RecordRollbackInput is one rollback attempt. Every body field is required on the wire so the
// generated client carries plain values; the descriptive ones may be empty. The maxLength
// values mirror the library's limits in goLibMyCarrier/slippy/rollback.go.
type RecordRollbackInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
	Body          struct {
		Workflow     string `json:"workflow"      minLength:"1" maxLength:"253" doc:"Argo Workflow of the attempt; a repeat for the same slip and workflow is not recorded twice, and replaces the outcome only of a failed record"`
		Environment  string `json:"environment"   maxLength:"16"   doc:"dev, preprod or prod"`
		Reason       string `json:"reason"        maxLength:"64"   doc:"alert_gate, canary_abort, timeout, manual or unknown; may be empty"`
		Source       string `json:"source"        maxLength:"64"   doc:"Publisher of the prod.rollback event; may be empty"`
		RolloutPhase string `json:"rollout_phase" maxLength:"64"   doc:"dark, canary, promoted or rolling; may be empty"`
		PodHash      string `json:"pod_hash"      maxLength:"64"   doc:"Aborted ReplicaSet pod-template hash; may be empty"`
		GitOpsRepo   string `json:"gitops_repo"   maxLength:"128"  doc:"GitOps repository the revert targeted"`
		GitOpsCommit string `json:"gitops_commit" maxLength:"64"   doc:"Release commit that was reverted"`
		RevertCommit string `json:"revert_commit" maxLength:"64"   doc:"Revert commit; empty when none was made"`
		Status       string `json:"status"        maxLength:"16"   doc:"success, noop, skipped, failed or dry_run"`
		Forced       bool   `json:"forced"                         doc:"Whether --force conflict resolution was used"`
		Message      string `json:"message"       maxLength:"1000" doc:"Why the rollback started: the publisher's text"`
		Outcome      string `json:"outcome"       maxLength:"1000" doc:"What the attempt did, or why it did not land"`
	}
}

// RecordRollbackOutput reports whether this call wrote the record.
type RecordRollbackOutput struct {
	Body struct {
		Inserted bool `json:"inserted" doc:"True when this call wrote a new record; false when one for this slip and workflow already existed"`
	}
}

// ListRollbacksInput names the slip whose rollback attempts to list.
type ListRollbacksInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
}

// ListRollbacksOutput carries a slip's rollback attempts in recorded order.
type ListRollbacksOutput struct {
	Body struct {
		Rollbacks []domain.RollbackRecord `json:"rollbacks" doc:"Rollback attempts in the order they were recorded"`
	}
}

// --- Route Registration --------------------------------------------------

// RegisterRollbackRoutes registers the rollback record routes. main.go registers them on the
// v1-only group.
func RegisterRollbackRoutes(api huma.API, h *RollbackHandler) {
	huma.Register(api, huma.Operation{
		OperationID:  "record-rollback",
		Method:       http.MethodPost,
		Path:         "/slips/{correlationID}/rollbacks",
		Summary:      "Record a rollback attempt for a routing slip",
		Security:     writeApiKeySecurity,
		MaxBodyBytes: maxRollbackBodyBytes,
		Tags:         []string{"v1"},
	}, h.recordRollback)

	huma.Register(api, huma.Operation{
		OperationID: "list-rollbacks",
		Method:      http.MethodGet,
		Path:        "/slips/{correlationID}/rollbacks",
		Summary:     "List the rollback attempts recorded for a routing slip",
		Security:    apiKeySecurity,
		Tags:        []string{"v1"},
	}, h.listRollbacks)
}

// --- Handlers ------------------------------------------------------------

func (h *RollbackHandler) recordRollback(
	ctx context.Context,
	input *RecordRollbackInput,
) (*RecordRollbackOutput, error) {
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.recordRollback",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
			attribute.String("rollback.workflow", input.Body.Workflow),
			attribute.String("rollback.status", input.Body.Status),
		),
	)
	defer span.End()

	inserted, err := h.store.RecordRollback(ctx, domain.RollbackRecord{
		CorrelationID: input.CorrelationID,
		Workflow:      input.Body.Workflow,
		Environment:   input.Body.Environment,
		Reason:        input.Body.Reason,
		Source:        input.Body.Source,
		RolloutPhase:  input.Body.RolloutPhase,
		PodHash:       input.Body.PodHash,
		GitOpsRepo:    input.Body.GitOpsRepo,
		GitOpsCommit:  input.Body.GitOpsCommit,
		RevertCommit:  input.Body.RevertCommit,
		Status:        domain.RollbackStatus(input.Body.Status),
		Forced:        input.Body.Forced,
		Message:       input.Body.Message,
		Outcome:       input.Body.Outcome,
	})
	if err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "rollback: record failed",
			"correlation_id", input.CorrelationID, "workflow", input.Body.Workflow, "error", err)
		return nil, mapRollbackError(err)
	}

	span.SetAttributes(attribute.Bool("rollback.inserted", inserted))
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "rollback: recorded",
		"correlation_id", input.CorrelationID, "workflow", input.Body.Workflow,
		"status", input.Body.Status, "inserted", inserted)
	out := &RecordRollbackOutput{}
	out.Body.Inserted = inserted
	return out, nil
}

func (h *RollbackHandler) listRollbacks(
	ctx context.Context,
	input *ListRollbacksInput,
) (*ListRollbacksOutput, error) {
	if err := validateCorrelationIDFormat(input.CorrelationID); err != nil {
		return nil, err
	}
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.listRollbacks",
		trace.WithAttributes(attribute.String("slip.correlation_id", input.CorrelationID)),
	)
	defer span.End()

	records, err := h.store.ListRollbacks(ctx, input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "rollback: list failed", "correlation_id", input.CorrelationID, "error", err)
		return nil, mapRollbackError(err)
	}
	if records == nil {
		records = []domain.RollbackRecord{} // JSON [] rather than null
	}

	span.SetAttributes(attribute.Int("rollback.count", len(records)))
	span.SetStatus(codes.Ok, "")
	out := &ListRollbacksOutput{}
	out.Body.Rollbacks = records
	return out, nil
}

// mapRollbackError converts rollback store errors to huma status errors. Anything unmapped is
// a 500 whose detail stays in the logs.
func mapRollbackError(err error) error {
	switch {
	case errors.Is(err, slippy.ErrSlipNotFound):
		return huma.NewError(http.StatusNotFound,
			"no routing slip with this correlation ID; it was never created or has been replaced by a newer run")
	case errors.Is(err, slippy.ErrInvalidRollback):
		return huma.NewError(http.StatusUnprocessableEntity, "invalid rollback record", err)
	default:
		return huma.NewError(http.StatusInternalServerError, "internal error")
	}
}
