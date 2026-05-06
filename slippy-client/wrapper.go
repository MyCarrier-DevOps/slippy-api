package slippyclient

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// wrapperVersion is embedded in the default User-Agent header.
const wrapperVersion = "1.0.0"

// wrapperConfig holds all configuration for a WrappedClient.
type wrapperConfig struct {
	token       string
	logger      *slog.Logger
	tp          trace.TracerProvider
	serviceName string
	userAgent   string
	httpClient  HttpRequestDoer
}

// WrapperOption configures a WrappedClient.
type WrapperOption func(*wrapperConfig)

// WithBearerToken sets the Authorization: Bearer token sent on every request.
func WithBearerToken(token string) WrapperOption {
	return func(c *wrapperConfig) { c.token = token }
}

// WithLogger sets the structured logger used for request/response logging.
// Accepts *slog.Logger; pass nil to use the default logger.
func WithLogger(l *slog.Logger) WrapperOption {
	return func(c *wrapperConfig) { c.logger = l }
}

// WithTracerProvider sets the OTel TracerProvider used for client spans.
// When not set, the global provider is used.
func WithTracerProvider(tp trace.TracerProvider) WrapperOption {
	return func(c *wrapperConfig) { c.tp = tp }
}

// WithServiceName sets the service name embedded in trace span attributes and
// the default User-Agent string.
func WithServiceName(name string) WrapperOption {
	return func(c *wrapperConfig) { c.serviceName = name }
}

// WithUserAgent overrides the User-Agent header sent on every request.
func WithUserAgent(ua string) WrapperOption {
	return func(c *wrapperConfig) { c.userAgent = ua }
}

// WithCustomHTTPClient replaces the underlying HTTP client (useful for testing).
// This corresponds to the plan's WithHTTPClient WrapperOption.
func WithCustomHTTPClient(c HttpRequestDoer) WrapperOption {
	return func(cfg *wrapperConfig) { cfg.httpClient = c }
}

// WrappedClient is a hand-written facade that embeds the oapi-codegen generated
// ClientWithResponses and adds cross-cutting concerns:
//
//   - Bearer token authentication (Authorization header)
//   - OTel client span per request with W3C traceparent/tracestate injection
//   - Structured request/response logging (slog)
//   - Configurable User-Agent header
//
// Use NewWrappedClient to construct; all generated methods are accessible
// directly through the embedded *ClientWithResponses.
type WrappedClient struct {
	*ClientWithResponses
	logger *slog.Logger
	tracer trace.Tracer
}

// NewWrappedClient constructs a WrappedClient targeting the given server URL.
// At least WithBearerToken should be provided for authenticated endpoints.
func NewWrappedClient(server string, opts ...WrapperOption) (*WrappedClient, error) {
	cfg := &wrapperConfig{
		serviceName: "slippy-client",
	}
	for _, o := range opts {
		o(cfg)
	}

	// Resolve User-Agent.
	ua := cfg.userAgent
	if ua == "" {
		ua = fmt.Sprintf("%s/slippy-client/%s", cfg.serviceName, wrapperVersion)
	}

	// Resolve TracerProvider and derive a Tracer.
	tp := cfg.tp
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	tracer := tp.Tracer("slippy-client")

	// Resolve logger.
	logger := cfg.logger
	if logger == nil {
		logger = slog.Default()
	}

	// Build the RequestEditorFn chain: auth → tracing → logging → user-agent.
	editors := []RequestEditorFn{
		buildAuthEditor(cfg.token),
		buildTracingEditor(tracer),
		buildLoggingEditor(logger),
		buildUserAgentEditor(ua),
	}

	// Build client options.
	var clientOpts []ClientOption
	for _, e := range editors {
		clientOpts = append(clientOpts, WithRequestEditorFn(e))
	}
	if cfg.httpClient != nil {
		clientOpts = append(clientOpts, WithHTTPClient(cfg.httpClient))
	}

	base, err := NewClientWithResponses(server, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("slippy-client: %w", err)
	}

	return &WrappedClient{
		ClientWithResponses: base,
		logger:              logger,
		tracer:              tracer,
	}, nil
}

// --- RequestEditorFn builders ---

// buildAuthEditor returns a RequestEditorFn that injects the Bearer token.
func buildAuthEditor(token string) RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		return nil
	}
}

// buildTracingEditor returns a RequestEditorFn that starts an OTel client span
// and injects W3C traceparent/tracestate headers via the global propagator.
func buildTracingEditor(tracer trace.Tracer) RequestEditorFn {
	return func(ctx context.Context, req *http.Request) error {
		_, span := tracer.Start(ctx, req.Method+" "+req.URL.Path,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("http.method", req.Method),
				attribute.String("http.url", req.URL.String()),
			),
		)
		defer span.End()

		// Inject W3C Trace Context (traceparent, tracestate) into outgoing request
		// headers using the global propagator — configured by the host application
		// via goLibMyCarrier/otel or direct OTel SDK setup.
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
		return nil
	}
}

// buildLoggingEditor returns a RequestEditorFn that emits a structured log entry.
func buildLoggingEditor(logger *slog.Logger) RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		logger.Info("slippy-client request",
			"method", req.Method,
			"path", req.URL.Path,
			"correlation_id", extractCorrelationID(req.URL.Path),
			"timestamp_utc", time.Now().UTC().Format(time.RFC3339),
		)
		return nil
	}
}

// buildUserAgentEditor returns a RequestEditorFn that sets the User-Agent header.
func buildUserAgentEditor(ua string) RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("User-Agent", ua)
		return nil
	}
}

// extractCorrelationID parses the correlation ID from a slips URL path like
// /slips/{id}/... → returns the ID segment, or empty string if not found.
func extractCorrelationID(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		if p == "slips" && i+1 < len(parts) {
			next := parts[i+1]
			if next != "by-commit" && next != "find-by-commits" && next != "find-all-by-commits" {
				return next
			}
			return ""
		}
	}
	return ""
}
