package handler

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

// HealthOutput represents the health check response.
type HealthOutput struct {
	Body struct {
		Status string `json:"status" example:"ok" doc:"Service health status"`
	}
}

// RegisterHealthRoutes registers the health check endpoint (no auth required).
func RegisterHealthRoutes(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "health-check",
		Method:      http.MethodGet,
		Path:        "/health",
		Summary:     "Health check endpoint",
	}, func(_ context.Context, _ *struct{}) (*HealthOutput, error) {
		out := &HealthOutput{}
		out.Body.Status = "ok"
		return out, nil
	})
}
