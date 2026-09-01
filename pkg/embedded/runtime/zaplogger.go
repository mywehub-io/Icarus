package runtime

import "go.uber.org/zap"

// zapLogger adapts a *zap.Logger to the Logger interface this package logs through.
type zapLogger struct{ l *zap.Logger }

// NewZapLogger returns a Logger backed by zap, for a host that already logs with zap.
//
// Without this, a host has no way to give the embedded runtime a logger it already owns, so
// ProcessorConfig.Logger stays nil and every decision the runtime makes — which nodes iterate, at
// what depth, which mappings resolved, which nodes were skipped and why — goes to NoOpLogger and is
// invisible. That is not a cosmetic gap: an embedded node silently failing to run left no trace in
// any log, and the only way to see it was to notice a node missing from the run's node list.
//
// A nil logger yields a Logger that discards, so a caller never has to guard.
func NewZapLogger(l *zap.Logger) Logger {
	if l == nil {
		return &NoOpLogger{}
	}
	return &zapLogger{l: l}
}

func zapFields(fields []Field) []zap.Field {
	if len(fields) == 0 {
		return nil
	}
	out := make([]zap.Field, len(fields))
	for i, f := range fields {
		out[i] = zap.Any(f.Key, f.Value)
	}
	return out
}

func (z *zapLogger) Debug(msg string, fields ...Field) { z.l.Debug(msg, zapFields(fields)...) }
func (z *zapLogger) Info(msg string, fields ...Field)  { z.l.Info(msg, zapFields(fields)...) }
func (z *zapLogger) Warn(msg string, fields ...Field)  { z.l.Warn(msg, zapFields(fields)...) }
func (z *zapLogger) Error(msg string, fields ...Field) { z.l.Error(msg, zapFields(fields)...) }

// Compile-time proof the adapter satisfies the interface the runtime logs through.
var _ Logger = (*zapLogger)(nil)
