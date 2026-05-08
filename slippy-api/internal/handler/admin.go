package handler

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// AdminHandler serves diagnostic endpoints that require direct ClickHouse access.
type AdminHandler struct {
	session  ch.ClickhouseSessionInterface
	database string
	pipeline *slippy.PipelineConfig
}

// NewAdminHandler creates an AdminHandler backed by the given session.
func NewAdminHandler(
	session ch.ClickhouseSessionInterface,
	database string,
	pipeline *slippy.PipelineConfig,
) *AdminHandler {
	return &AdminHandler{session: session, database: database, pipeline: pipeline}
}

// SchemaVersionOutput is the response body for GET /v1/admin/schema-version.
type SchemaVersionOutput struct {
	Body struct {
		Current int `json:"current" doc:"Current ClickHouse schema version"`
		Target  int `json:"target"  doc:"Target schema version derived from the pipeline config"`
	}
}

// RegisterAdminRoutes registers admin diagnostic endpoints on the given API group.
func RegisterAdminRoutes(api huma.API, h *AdminHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-schema-version",
		Method:      http.MethodGet,
		Path:        "/admin/schema-version",
		Summary:     "Get current and target ClickHouse schema versions",
		Tags:        []string{"v1"},
	}, h.getSchemaVersion)
}

func (h *AdminHandler) getSchemaVersion(ctx context.Context, _ *struct{}) (*SchemaVersionOutput, error) {
	current, err := slippy.GetCurrentSchemaVersion(ctx, h.session.Conn(), h.database)
	if err != nil {
		return nil, huma.NewError(http.StatusInternalServerError, "failed to read schema version")
	}
	target := slippy.GetDynamicMigrationVersion(h.pipeline)
	out := &SchemaVersionOutput{}
	out.Body.Current = current
	out.Body.Target = target
	return out, nil
}
