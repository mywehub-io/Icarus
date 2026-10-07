package runner

import (
	"context"

	"go.uber.org/zap"

	internaltracing "github.com/wehubfusion/Icarus/internal/tracing"
)

// TracingConfig is the public tracing configuration used by Runner clients.
// It mirrors the internal tracing configuration but keeps the implementation private.
type TracingConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	OTLPEndpoint   string
	SampleRatio    float64
}

// DefaultTracingConfig returns a development-friendly tracing configuration.
func DefaultTracingConfig(serviceName string) TracingConfig {
	cfg := internaltracing.DefaultConfig(serviceName)
	return fromInternalConfig(cfg)
}

// JaegerTracingConfig returns a tracing configuration tailored for Jaeger.
func JaegerTracingConfig(serviceName string) TracingConfig {
	cfg := internaltracing.JaegerConfig(serviceName)
	return fromInternalConfig(cfg)
}

func (c TracingConfig) toInternalConfig() internaltracing.TracingConfig {
	return internaltracing.TracingConfig{
		ServiceName:    c.ServiceName,
		ServiceVersion: c.ServiceVersion,
		Environment:    c.Environment,
		OTLPEndpoint:   c.OTLPEndpoint,
		SampleRatio:    c.SampleRatio,
	}
}

func fromInternalConfig(cfg internaltracing.TracingConfig) TracingConfig {
	return TracingConfig{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		Environment:    cfg.Environment,
		OTLPEndpoint:   cfg.OTLPEndpoint,
		SampleRatio:    cfg.SampleRatio,
	}
}

// SetupTracing sets up the process's OTLP exporter and global tracer provider once. A service that
// runs many runners calls this in main and passes a nil TracingConfig to NewRunner: each runner
// given a config builds its own exporter and replaces the global provider, so a process with 16
// runners held 16 exporters and used only the last. Call the returned function at shutdown.
func SetupTracing(ctx context.Context, cfg TracingConfig, logger *zap.Logger) (func(context.Context) error, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	return internaltracing.SetupTracing(ctx, cfg.toInternalConfig(), logger)
}
