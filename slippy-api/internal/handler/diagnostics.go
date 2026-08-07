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

// DiagnosticsHandler serves read-only diagnostic endpoints that need direct
// ClickHouse access. These report on the service's own datastores; they perform no
// administrative action, which is why they live under /diagnostics rather than a
// path namespace that reads as privileged (DEVOPS-217).
type DiagnosticsHandler struct {
	session  ch.ClickhouseSessionInterface
	database string
}

// NewDiagnosticsHandler creates a DiagnosticsHandler backed by the given session.
func NewDiagnosticsHandler(
	session ch.ClickhouseSessionInterface,
	database string,
) *DiagnosticsHandler {
	return &DiagnosticsHandler{session: session, database: database}
}

// ClickHouseSchemaVersionOutput is the response body for
// GET /v1/diagnostics/clickhouse-schema-version.
type ClickHouseSchemaVersionOutput struct {
	Body struct {
		Current int `json:"current" doc:"Version of the LEGACY ClickHouse slip schema (now frozen). The operational slip schema lives in Postgres, owned by the slippy-migrator Job, and is not reported by this endpoint."`
	}
}

// RegisterDiagnosticsRoutes registers the diagnostic endpoints on the given API
// group. They require the read key like every other read operation — the probe
// costs a caller nothing to authenticate and needs no exception to the
// fail-closed auth default.
func RegisterDiagnosticsRoutes(api huma.API, h *DiagnosticsHandler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-clickhouse-schema-version",
		Method:      http.MethodGet,
		Path:        "/diagnostics/clickhouse-schema-version",
		Summary:     "Get the legacy ClickHouse slip schema version (diagnostic; PG schema is owned by the migrator Job)",
		Tags:        []string{"v1"},
		Security:    apiKeySecurity,
	}, h.getClickHouseSchemaVersion)
}

func (h *DiagnosticsHandler) getClickHouseSchemaVersion(
	ctx context.Context,
	_ *struct{},
) (*ClickHouseSchemaVersionOutput, error) {
	ctx, span := otel.Tracer(handlerTracerName).Start(ctx, "handler.getClickHouseSchemaVersion",
		trace.WithAttributes(attribute.String("slip.database", h.database)),
	)
	defer span.End()

	slog.InfoContext(ctx, "diagnostics: reading clickhouse schema version", "database", h.database)

	// NOTE: this reports the LEGACY ClickHouse slip schema version. Post-Postgres-cutover
	// the operational slip schema lives in Postgres and is owned by the slippy-migrator Job;
	// this endpoint intentionally does not track it (repointing at the PG schema version is a
	// separate change). The returned number reflects the now-frozen ClickHouse slip schema.
	current, err := slippy.GetCurrentSchemaVersion(ctx, h.session.Conn(), h.database)
	if err != nil {
		recordHandlerError(span, err)
		slog.ErrorContext(ctx, "diagnostics: failed to read clickhouse schema version",
			"database", h.database, "error", err)
		return nil, huma.NewError(http.StatusInternalServerError, "failed to read schema version")
	}
	span.SetAttributes(attribute.Int("schema.current", current))
	span.SetStatus(codes.Ok, "")
	slog.InfoContext(ctx, "diagnostics: clickhouse schema version retrieved",
		"database", h.database, "current", current)

	out := &ClickHouseSchemaVersionOutput{}
	out.Body.Current = current
	return out, nil
}
