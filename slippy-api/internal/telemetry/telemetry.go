// Package telemetry initialises OpenTelemetry tracing and metrics for slippy-api.
//
// All configuration is driven by standard OTel environment variables:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT   – collector address (default http://localhost:4317)
//	OTEL_EXPORTER_OTLP_PROTOCOL   – "grpc" (default) or "http/protobuf"
//	OTEL_SERVICE_NAME              – service identifier (default "slippy-api")
//	OTEL_SDK_DISABLED              – set "true" to disable the SDK entirely
//	OTEL_RESOURCE_ATTRIBUTES       – comma-separated key=value resource attributes
//
// When the SDK is disabled or the endpoint is unreachable the application
// falls back to the no-op provider — telemetry is never a hard dependency.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	// Protocol constants matching the OTEL_EXPORTER_OTLP_PROTOCOL env var.
	protocolGRPC = "grpc"
	protocolHTTP = "http/protobuf"

	// Default service name when OTEL_SERVICE_NAME is not set.
	defaultServiceName = "slippy-api"
)

// Shutdown aggregates the shutdown functions for all registered providers.
// Callers should defer Shutdown(ctx) after Init.
type Shutdown func(ctx context.Context) error

// Init bootstraps OpenTelemetry with an OTLP exporter driven entirely by
// standard environment variables. It returns a Shutdown function that flushes
// and releases all provider resources.
//
// When OTEL_SDK_DISABLED=true or no endpoint is configured the function
// returns a no-op shutdown — the global providers remain as default no-ops.
func Init(ctx context.Context) (Shutdown, error) {
	// Respect the standard SDK kill-switch.
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		log.Println("otel: SDK disabled via OTEL_SDK_DISABLED")
		return noopShutdown, nil
	}

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		log.Println("otel: OTEL_EXPORTER_OTLP_ENDPOINT not set, telemetry disabled")
		return noopShutdown, nil
	}

	protocol := strings.ToLower(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"))
	if protocol == "" {
		protocol = protocolGRPC // gRPC is more efficient for high-volume telemetry
	}

	// Build a resource describing this service instance.
	res, err := buildResource(ctx)
	if err != nil {
		return noopShutdown, fmt.Errorf("otel resource: %w", err)
	}

	// Strip scheme and detect TLS for the raw endpoint address.
	addr, secure := parseEndpoint(endpoint)

	// --- Trace provider ---
	traceExp, err := newTraceExporter(ctx, protocol, addr, secure)
	if err != nil {
		return noopShutdown, fmt.Errorf("otel trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)

	// --- Metric provider ---
	metricExp, err := newMetricExporter(ctx, protocol, addr, secure)
	if err != nil {
		// Trace is already initialised — best-effort: shut it down and return error.
		_ = tp.Shutdown(ctx)
		return noopShutdown, fmt.Errorf("otel metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)

	// Register global providers and propagator.
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("otel: initialised (%s → %s)", protocol, endpoint)

	shutdown := func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}
	return shutdown, nil
}

// noopShutdown is returned when telemetry is not initialised.
func noopShutdown(_ context.Context) error { return nil }

// buildResource creates an OTel resource with the service name and any
// extra Kubernetes attributes injected by the Helm chart.
func buildResource(ctx context.Context) (*resource.Resource, error) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
	}

	// Append Kubernetes pod attributes injected by the Helm chart.
	k8sEnvMap := map[string]string{
		"OTEL_RESOURCE_ATTRIBUTES_NODE_NAME":     "k8s.node.name",
		"OTEL_RESOURCE_ATTRIBUTES_POD_NAME":      "k8s.pod.name",
		"OTEL_RESOURCE_ATTRIBUTES_POD_NAMESPACE": "k8s.namespace.name",
		"OTEL_RESOURCE_ATTRIBUTES_POD_UID":       "k8s.pod.uid",
	}
	for env, key := range k8sEnvMap {
		if v := os.Getenv(env); v != "" {
			attrs = append(attrs, attribute.String(key, v))
		}
	}

	return resource.NewWithAttributes("", attrs...), nil
}

// parseEndpoint strips the scheme from the endpoint and returns the bare
// host:port and whether TLS should be used.
func parseEndpoint(endpoint string) (addr string, secure bool) {
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		return strings.TrimPrefix(endpoint, "https://"), true
	case strings.HasPrefix(endpoint, "http://"):
		return strings.TrimPrefix(endpoint, "http://"), false
	default:
		return endpoint, false
	}
}

// newTraceExporter creates an OTLP span exporter for the requested protocol.
func newTraceExporter(ctx context.Context, protocol, addr string, secure bool) (sdktrace.SpanExporter, error) {
	switch protocol {
	case protocolGRPC:
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(addr)}
		if !secure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, opts...)
	case protocolHTTP:
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(addr)}
		if !secure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		return otlptracehttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unsupported OTEL_EXPORTER_OTLP_PROTOCOL %q", protocol)
	}
}

// newMetricExporter creates an OTLP metric exporter for the requested protocol.
func newMetricExporter(ctx context.Context, protocol, addr string, secure bool) (sdkmetric.Exporter, error) {
	switch protocol {
	case protocolGRPC:
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(addr)}
		if !secure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		return otlpmetricgrpc.New(ctx, opts...)
	case protocolHTTP:
		opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(addr)}
		if !secure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		return otlpmetrichttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unsupported OTEL_EXPORTER_OTLP_PROTOCOL %q", protocol)
	}
}
