package handler

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

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
		Current int `json:"current" doc:"Current version of the LEGACY ClickHouse slip schema. Slip data now lives in Postgres, whose schema is owned by the slippy-migrator Job and is NOT reported here."`
		Target  int `json:"target"  doc:"Target schema version derived from the pipeline config"`
	}
}

// RegisterAdminRoutes registers admin diagnostic endpoints on the given API group.
func RegisterAdminRoutes(api huma.API, h *AdminHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-schema-version",
		Method:      http.MethodGet,
		Path:        "/admin/schema-version",
		Summary:     "Get the legacy ClickHouse slip schema version (diagnostic; PG schema is owned by the migrator Job)",
		Tags:        []string{"v1"},
	}, h.getSchemaVersion)
}

func (h *AdminHandler) getSchemaVersion(ctx context.Context, _ *struct{}) (*SchemaVersionOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getSchemaVersion",
		trace.WithAttributes(attribute.String("slip.database", h.database)),
	)
	defer span.End()

	slog.InfoContext(ctx, "admin: reading schema version", "database", h.database)

	// NOTE: this reports the LEGACY ClickHouse slip schema version. Post-Postgres-cutover
	// the operational slip schema lives in Postgres and is owned by the slippy-migrator Job;
	// this endpoint intentionally does not track it (repointing at the PG schema version is a
	// separate change). The returned number reflects the now-frozen ClickHouse slip schema.
	current, err := slippy.GetCurrentSchemaVersion(ctx, h.session.Conn(), h.database)
	if err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "admin: failed to read schema version",
			"database", h.database, "error", err)
		return nil, huma.NewError(http.StatusInternalServerError, "failed to read schema version")
	}
	target := slippy.GetDynamicMigrationVersion(h.pipeline)
	span.SetAttributes(
		attribute.Int("schema.current", current),
		attribute.Int("schema.target", target),
	)
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "admin: schema version retrieved",
		"database", h.database, "current", current, "target", target)

	out := &SchemaVersionOutput{}
	out.Body.Current = current
	out.Body.Target = target
	return out, nil
}
