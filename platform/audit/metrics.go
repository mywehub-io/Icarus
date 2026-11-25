package audit

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// Metric instrument names.
const (
	MetricPublished  = "audit.events.published"
	MetricDropped    = "audit.events.dropped"
	MetricRetries    = "audit.publish.retries"
	MetricQueueDepth = "audit.queue.depth"
)

const meterName = "github.com/wehubfusion/Icarus/platform/audit"

type instruments struct {
	pub   metric.Int64Counter
	drop  metric.Int64Counter
	retry metric.Int64Counter
	attrs metric.MeasurementOption
}

func newInstruments(mp metric.MeterProvider, service string, depth func() int64, logger *zap.Logger) *instruments {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)
	set := attribute.NewSet(attribute.String("service", service))
	in := &instruments{attrs: metric.WithAttributeSet(set)}
	fallback := noop.NewMeterProvider().Meter(meterName)

	var err error
	if in.pub, err = meter.Int64Counter(MetricPublished,
		metric.WithDescription("Audit events the stream acknowledged.")); err != nil {
		logger.Warn("audit metric unavailable", zap.String("metric", MetricPublished), zap.Error(err))
		in.pub, _ = fallback.Int64Counter(MetricPublished)
	}
	if in.drop, err = meter.Int64Counter(MetricDropped,
		metric.WithDescription("Audit events given up on: over the size cap, queue full, or unflushed at close.")); err != nil {
		logger.Warn("audit metric unavailable", zap.String("metric", MetricDropped), zap.Error(err))
		in.drop, _ = fallback.Int64Counter(MetricDropped)
	}
	if in.retry, err = meter.Int64Counter(MetricRetries,
		metric.WithDescription("Publish attempts for audit events whose earlier attempt failed.")); err != nil {
		logger.Warn("audit metric unavailable", zap.String("metric", MetricRetries), zap.Error(err))
		in.retry, _ = fallback.Int64Counter(MetricRetries)
	}
	if _, err = meter.Int64ObservableGauge(MetricQueueDepth,
		metric.WithDescription("Audit events waiting in the emitter's queue."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(depth(), metric.WithAttributeSet(set))
			return nil
		})); err != nil {
		logger.Warn("audit metric unavailable", zap.String("metric", MetricQueueDepth), zap.Error(err))
	}
	return in
}

func (in *instruments) published(ctx context.Context) { in.pub.Add(ctx, 1, in.attrs) }

func (in *instruments) dropped(ctx context.Context, n int64) { in.drop.Add(ctx, n, in.attrs) }

func (in *instruments) retried(ctx context.Context) { in.retry.Add(ctx, 1, in.attrs) }
