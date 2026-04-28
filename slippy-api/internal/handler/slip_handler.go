package handler

import (
	"context"
	"errors"
	"log"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// handlerTracerName is the instrumentation scope for handler operations.
const handlerTracerName = "slippy-api/handler"

// SlipHandler holds dependencies for slip route handlers.
type SlipHandler struct {
	reader domain.SlipReader
}

// NewSlipHandler creates a handler backed by the given reader.
func NewSlipHandler(reader domain.SlipReader) *SlipHandler {
	return &SlipHandler{reader: reader}
}

// --- Input / Output types ------------------------------------------------

// GetSlipInput captures the path parameter for loading a slip by correlation ID.
type GetSlipInput struct {
	CorrelationID string `path:"correlationID" doc:"Routing slip correlation ID"`
}

// GetSlipOutput wraps a single slip response.
type GetSlipOutput struct {
	Body *domain.Slip
}

// GetSlipByCommitInput captures owner/repo/commitSHA path segments.
// The repository is reconstructed as "owner/repo" in the handler.
type GetSlipByCommitInput struct {
	Owner     string `path:"owner"     doc:"Repository owner (e.g. 'org')"`
	Repo      string `path:"repo"      doc:"Repository name (e.g. 'my-service')"`
	CommitSHA string `path:"commitSHA" doc:"Git commit SHA"`
}

// FindByCommitsInput is the request body for commit-based lookups.
type FindByCommitsInput struct {
	Body struct {
		Repository string   `json:"repository" doc:"Full repository name (owner/repo)"`
		Commits    []string `json:"commits" doc:"List of commit SHAs to search"`
	}
}

// FindByCommitsOutput returns the matched slip and the commit that matched.
type FindByCommitsOutput struct {
	Body struct {
		Slip          *domain.Slip `json:"slip"`
		MatchedCommit string       `json:"matched_commit"`
	}
}

// FindAllByCommitsItem represents a single slip matched to a commit.
type FindAllByCommitsItem struct {
	Slip          *domain.Slip `json:"slip"`
	MatchedCommit string       `json:"matched_commit"`
}

// FindAllByCommitsOutput returns all matched slips.
type FindAllByCommitsOutput struct {
	Body []FindAllByCommitsItem
}

// --- Route Registration --------------------------------------------------

// apiKeySecurity marks an operation as requiring API key authentication.
var apiKeySecurity = []map[string][]string{{"apiKey": {}}}

// RegisterRoutes registers all slip-related routes on the given huma API.
func RegisterRoutes(api huma.API, h *SlipHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-slip",
		Method:      http.MethodGet,
		Path:        "/slips/{correlationID}",
		Summary:     "Get a routing slip by correlation ID",
		Security:    apiKeySecurity,
	}, h.getSlip)

	huma.Register(api, huma.Operation{
		OperationID: "get-slip-by-commit",
		Method:      http.MethodGet,
		Path:        "/slips/by-commit/{owner}/{repo}/{commitSHA}",
		Summary:     "Get a routing slip by repository and commit SHA",
		Security:    apiKeySecurity,
	}, h.getSlipByCommit)

	huma.Register(api, huma.Operation{
		OperationID: "find-by-commits",
		Method:      http.MethodPost,
		Path:        "/slips/find-by-commits",
		Summary:     "Find the first matching routing slip for a list of commits",
		Security:    apiKeySecurity,
	}, h.findByCommits)

	huma.Register(api, huma.Operation{
		OperationID: "find-all-by-commits",
		Method:      http.MethodPost,
		Path:        "/slips/find-all-by-commits",
		Summary:     "Find all matching routing slips for a list of commits",
		Security:    apiKeySecurity,
	}, h.findAllByCommits)
}

// --- Handlers ------------------------------------------------------------

func (h *SlipHandler) getSlip(ctx context.Context, input *GetSlipInput) (*GetSlipOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getSlip",
		trace.WithAttributes(
			attribute.String("slip.correlation_id", input.CorrelationID),
		),
	)
	defer span.End()

	slip, err := h.reader.Load(ctx, input.CorrelationID)
	if err != nil {
		recordHandlerError(span, err)
		return nil, mapError(err)
	}
	span.SetStatus(codes.Ok, "")
	return &GetSlipOutput{Body: slip}, nil
}

func (h *SlipHandler) getSlipByCommit(ctx context.Context, input *GetSlipByCommitInput) (*GetSlipOutput, error) {
	repository := input.Owner + "/" + input.Repo
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getSlipByCommit",
		trace.WithAttributes(
			attribute.String("slip.repository", repository),
			attribute.String("slip.commit_sha", input.CommitSHA),
		),
	)
	defer span.End()

	slip, err := h.reader.LoadByCommit(ctx, repository, input.CommitSHA)
	if err != nil {
		recordHandlerError(span, err)
		return nil, mapError(err)
	}
	span.SetStatus(codes.Ok, "")
	return &GetSlipOutput{Body: slip}, nil
}

func (h *SlipHandler) findByCommits(ctx context.Context, input *FindByCommitsInput) (*FindByCommitsOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.findByCommits",
		trace.WithAttributes(
			attribute.String("slip.repository", input.Body.Repository),
			attribute.Int("slip.commits_count", len(input.Body.Commits)),
		),
	)
	defer span.End()

	slip, commit, err := h.reader.FindByCommits(ctx, input.Body.Repository, input.Body.Commits)
	if err != nil {
		recordHandlerError(span, err)
		return nil, mapError(err)
	}
	span.SetAttributes(attribute.String("slip.matched_commit", commit))
	span.SetStatus(codes.Ok, "")
	out := &FindByCommitsOutput{}
	out.Body.Slip = slip
	out.Body.MatchedCommit = commit
	return out, nil
}

func (h *SlipHandler) findAllByCommits(
	ctx context.Context,
	input *FindByCommitsInput,
) (*FindAllByCommitsOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.findAllByCommits",
		trace.WithAttributes(
			attribute.String("slip.repository", input.Body.Repository),
			attribute.Int("slip.commits_count", len(input.Body.Commits)),
		),
	)
	defer span.End()

	results, err := h.reader.FindAllByCommits(ctx, input.Body.Repository, input.Body.Commits)
	if err != nil {
		recordHandlerError(span, err)
		return nil, mapError(err)
	}
	span.SetAttributes(attribute.Int("slip.results_count", len(results)))
	span.SetStatus(codes.Ok, "")
	items := make([]FindAllByCommitsItem, len(results))
	for i, r := range results {
		items[i] = FindAllByCommitsItem{
			Slip:          r.Slip,
			MatchedCommit: r.MatchedCommit,
		}
	}
	return &FindAllByCommitsOutput{Body: items}, nil
}

// --- Error Mapping -------------------------------------------------------

// recordHandlerError records an error on the handler span.
//
// TEMPORARY: also logs the error to stdout for live debugging. Remove the
// log.Printf line once the handler errors are no longer being investigated.
func recordHandlerError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	log.Printf("handler error: %v", err)
}

// mapError converts domain/store errors to huma status errors.
func mapError(err error) error {
	switch {
	case errors.Is(err, slippy.ErrSlipNotFound):
		return huma.NewError(http.StatusNotFound, "slip not found")
	case errors.Is(err, slippy.ErrInvalidCorrelationID):
		return huma.NewError(http.StatusBadRequest, "invalid correlation ID")
	case errors.Is(err, slippy.ErrInvalidRepository):
		return huma.NewError(http.StatusBadRequest, "invalid repository")
	default:
		return huma.NewError(http.StatusInternalServerError, "internal error")
	}
}
