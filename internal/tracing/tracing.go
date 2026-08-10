// Package tracing provides OpenTelemetry tracing setup utilities for Icarus
package tracing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.uber.org/zap"
)

// setupOnce guards against building more than one TracerProvider/exporter per process. Runner
// (pkg/runner) calls SetupTracing once per registered plugin-type runner — without this guard,
// each call built a brand-new OTLP exporter and clobbered the global TracerProvider
// (otel.SetTracerProvider), leaking every prior exporter's connection/goroutines and making
// which runner's Resource attributes end up describing the shared trace stream an accident of
// registration order. Only the first call's config takes effect; later calls return the same
// shutdown function.
var (
	setupOnce     sync.Once
	setupShutdown func(context.Context) error
	setupErr      error
)

// TracingConfig holds configuration for tracing setup
type TracingConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	OTLPEndpoint   string // e.g., "127.0.0.1:4318" for Jaeger (host:port only, path added by exporter)
	SampleRatio    float64
}

// DefaultConfig returns a default tracing configuration
func DefaultConfig(serviceName string) TracingConfig {
	return TracingConfig{
		ServiceName:    serviceName,
		ServiceVersion: "1.0.0",
		Environment:    "development",
		OTLPEndpoint:   "127.0.0.1:4318", // OTLP HTTP endpoint (host:port only, path added by exporter)
		SampleRatio:    1.0,              // Sample all traces in development
	}
}

// JaegerConfig returns a configuration optimized for Jaeger
func JaegerConfig(serviceName string) TracingConfig {
	return TracingConfig{
		ServiceName:    serviceName,
		ServiceVersion: "1.0.0",
		Environment:    "development",
		OTLPEndpoint:   "127.0.0.1:4318", // OTLP HTTP endpoint for Jaeger (host:port only, path added by exporter)
		SampleRatio:    1.0,
	}
}

// SetupTracing initializes OpenTelemetry tracing with OTLP exporter.
// Returns a shutdown function that should be called when the application exits.
// Idempotent per process: only the first call actually builds an exporter/provider; later calls
// (e.g. one per registered runner) return the same shutdown function without side effects.
func SetupTracing(ctx context.Context, config TracingConfig, logger *zap.Logger) (func(context.Context) error, error) {
	setupOnce.Do(func() {
		setupShutdown, setupErr = setupTracingOnce(ctx, config, logger)
	})
	return setupShutdown, setupErr
}

func setupTracingOnce(ctx context.Context, config TracingConfig, logger *zap.Logger) (func(context.Context) error, error) {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn("OpenTelemetry error", zap.Error(err))
	}))

	logger.Info("Setting up tracing",
		zap.String("service_name", config.ServiceName),
		zap.String("otlp_endpoint", config.OTLPEndpoint),
		zap.String("environment", config.Environment))

	// Create OTLP HTTP exporter
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(config.OTLPEndpoint),
		otlptracehttp.WithInsecure(), // Use insecure connection for local development
	)
	if err != nil {
		logger.Error("Failed to create OTLP exporter", zap.Error(err))
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service information
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(config.ServiceName),
			semconv.ServiceVersion(config.ServiceVersion),
			semconv.DeploymentEnvironment(config.Environment),
		),
	)
	if err != nil {
		logger.Error("Failed to create resource", zap.Error(err))
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider. ParentBased wraps the ratio sampler: a bare TraceIDRatioBased
	// re-decides on every hop, so a trace another service already sampled would lose this
	// process's spans; ParentBased keeps the ratio as a root-span-only decision.
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(config.SampleRatio))),
	)

	// Set global trace provider
	otel.SetTracerProvider(tp)

	// Set global propagator (W3C TraceContext + Baggage)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	logger.Info("Tracing setup completed successfully")

	// Return shutdown function
	return tp.Shutdown, nil
}

// SetupJaegerTracing is a convenience function to setup tracing with Jaeger-specific defaults
func SetupJaegerTracing(ctx context.Context, serviceName string, logger *zap.Logger) (func(context.Context) error, error) {
	config := JaegerConfig(serviceName)
	return SetupTracing(ctx, config, logger)
}

// ShutdownTracing gracefully shuts down the tracing provider
func ShutdownTracing(shutdown func(context.Context) error, logger *zap.Logger) error {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	logger.Info("Shutting down tracing")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := shutdown(ctx)
	if err != nil {
		logger.Error("Failed to shutdown tracing", zap.Error(err))
	} else {
		logger.Info("Tracing shutdown completed successfully")
	}
	return err
}
