package message

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// Size caps for the JetStream streams the workflow path uses. No stream had one, so a backlog
// grew until the NATS disk filled and every write failed (UAT incident 08/10/2026, C2). The
// streams keep messages by limits, not until they are acknowledged, so a full stream drops its
// oldest messages (DiscardOld) rather than refusing new ones, which would stop all work. A
// message dropped that way had not been consumed only when a consumer is far behind; watch
// consumer lag to see it.
const (
	// DefaultWorkStreamMaxBytes caps a unit, results or trigger stream, whose messages are small
	// references.
	DefaultWorkStreamMaxBytes int64 = 1 << 30
	// DefaultDLQStreamMaxBytes caps a dead letter stream.
	DefaultDLQStreamMaxBytes int64 = 512 << 20
)

// WithSizeCap returns cfg with MaxBytes set to maxBytes and the oldest messages discarded above it.
func WithSizeCap(cfg jetstream.StreamConfig, maxBytes int64) jetstream.StreamConfig {
	cfg.MaxBytes = maxBytes
	cfg.Discard = jetstream.DiscardOld
	return cfg
}

// StreamUpdater is the part of jetstream.JetStream EnsureSizeCap needs.
type StreamUpdater interface {
	UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
}

// EnsureSizeCap brings an existing stream to maxBytes with DiscardOld, changing nothing else. It
// updates only when the cap differs, so it is cheap to call at start. Lowering the cap below the
// stream's current size drops its oldest messages.
func EnsureSizeCap(ctx context.Context, js StreamUpdater, stream jetstream.Stream, maxBytes int64, logger *zap.Logger) error {
	if js == nil || stream == nil || maxBytes <= 0 {
		return nil
	}
	info := stream.CachedInfo()
	if info == nil {
		var err error
		if info, err = stream.Info(ctx); err != nil {
			return fmt.Errorf("stream info: %w", err)
		}
	}
	cfg := info.Config
	if cfg.MaxBytes == maxBytes && cfg.Discard == jetstream.DiscardOld {
		return nil
	}
	previous := cfg.MaxBytes
	if _, err := js.UpdateStream(ctx, WithSizeCap(cfg, maxBytes)); err != nil {
		return fmt.Errorf("set size cap on stream %q: %w", cfg.Name, err)
	}
	if logger != nil {
		logger.Info("Updated JetStream stream size cap",
			zap.String("stream", cfg.Name),
			zap.Int64("from", previous),
			zap.Int64("to", maxBytes))
	}
	return nil
}
