package storage

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// tracer follows the platform split: unprefixed span names inside Icarus,
// "{service}.{area}.{op}" inside services.
var tracer = otel.Tracer("icarus/storage")

// Span names emitted by this package. docs/services/*/observability.md previously
// documented blob.put and blob.get as existing; they did not. These are those spans.
const (
	spanBlobPut        = "blob.put"
	spanBlobGet        = "blob.get"
	spanBlobGetRange   = "blob.get_range"
	spanBlobProperties = "blob.properties"
	spanBlobDelete     = "blob.delete"
)

// blobOp instruments one storage operation with a span and a structured log line
// carrying duration, size and outcome.
//
// Before this, the download path logged nothing on success or failure
// (DownloadResult and DownloadFromURL both returned silently), which left no
// before-number for the quantity this work exists to reduce. Bytes moved per
// operation is the measurement that matters; everything else here is context for it.
type blobOp struct {
	span         trace.Span
	logger       *zap.Logger
	name         string
	start        time.Time
	successLevel logLevel
	fields       []zap.Field
}

// logLevel selects the level used for a successful operation. Ranged reads are
// deliberately Debug: a single resolve issues many, and at Info they would drown the
// coarse events that matter operationally. Failures are always Error.
type logLevel int

const (
	logInfo logLevel = iota
	logDebug
)

func (a *AzureBlobClient) startOp(ctx context.Context, name string, level logLevel, attrs ...attribute.KeyValue) (context.Context, *blobOp) {
	ctx, span := tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	fields := make([]zap.Field, 0, len(attrs)+3)
	for _, kv := range attrs {
		fields = append(fields, zap.String(string(kv.Key), kv.Value.Emit()))
	}
	return ctx, &blobOp{
		span:         span,
		logger:       a.logger,
		name:         name,
		start:        time.Now(),
		successLevel: level,
		fields:       fields,
	}
}

// bytes records the payload size on both the span and the log line. Call it before
// finish; it is separate because the size is often only known once the call returns.
func (o *blobOp) bytes(n int) {
	o.span.SetAttributes(attribute.Int("blob.size_bytes", n))
	o.fields = append(o.fields, zap.Int("size_bytes", n))
}

func (o *blobOp) attr(kv ...attribute.KeyValue) {
	o.span.SetAttributes(kv...)
	for _, k := range kv {
		o.fields = append(o.fields, zap.String(string(k.Key), k.Value.Emit()))
	}
}

// finish closes the span and emits the log line. err nil means success.
func (o *blobOp) finish(err error) {
	elapsed := time.Since(o.start)
	o.span.SetAttributes(attribute.Int64("blob.duration_ms", elapsed.Milliseconds()))
	o.fields = append(o.fields,
		zap.Duration("duration", elapsed),
		zap.String("operation", o.name),
	)

	if err != nil {
		o.span.RecordError(err)
		o.span.SetStatus(codes.Error, err.Error())
		o.span.End()
		if o.logger != nil {
			o.logger.Error("blob operation failed", append(o.fields, zap.Error(err))...)
		}
		return
	}

	o.span.SetStatus(codes.Ok, "")
	o.span.End()
	if o.logger == nil {
		return
	}
	if o.successLevel == logDebug {
		o.logger.Debug("blob operation", o.fields...)
		return
	}
	o.logger.Info("blob operation", o.fields...)
}
