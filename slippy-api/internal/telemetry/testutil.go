// Package telemetry provides OpenTelemetry initialisation for slippy-api.
//
// This file contains test utilities for capturing and asserting on spans.
// It is compiled into the production binary but has zero cost at runtime
// because no production code calls these functions.
package telemetry

import (
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// SetupTestTracing installs a TracerProvider backed by an in-memory SpanRecorder
// and returns the recorder for span assertions. The caller should defer
// restoring the original provider.
//
// Usage:
//
//	recorder, cleanup := telemetry.SetupTestTracing()
//	defer cleanup()
//	... exercise code ...
//	spans := recorder.Ended()
func SetupTestTracing() (recorder *tracetest.SpanRecorder, cleanup func()) {
	recorder = tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)

	cleanup = func() {
		otel.SetTracerProvider(prev)
	}
	return recorder, cleanup
}
