package message

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// Size caps for the JetStream streams the workflow path uses. No stream had one, so a backlog
// grew until the NATS disk filled and every write failed (UAT incident 08/10/2026, C2). The
// streams keep messages by limits, not until they are acknowledged, so a full stream drops its
// oldest messages (DiscardOld) rather than refusing new ones, which would stop all work. A
// message dropped that way had not been consumed only when a consumer is far behind; watch
// consumer lag to see it.
//
// A cap is not free. JetStream reserves each stream's MaxBytes against the server's store limit
// when the stream is created and again every time the server starts, and a stream that no longer
// fits is not loaded at all. At 1 GiB per work stream the local estate reserved 98.6% of its
// store, and each NATS restart dropped whole streams with every message in them ("Error
// recreating stream ...: insufficient storage resources available", 10047): XLSX_OPERATIONS and
// WORKFLOW_TRIGGERS_DLQ in the chaos baseline of 10/10/2026. The caps are therefore sized to the
// traffic (a work stream holds small references; the busiest local one held 15 MiB), and an
// operator can change them per environment:
//
//	ICARUS_WORK_STREAM_MAX_BYTES  default 256 MiB
//	ICARUS_DLQ_STREAM_MAX_BYTES   default  64 MiB
//
// Every stream of every environment sharing a NATS cluster reserves its cap there, so the total
// must stay well inside the server's max_file_store.
var (
	// DefaultWorkStreamMaxBytes caps a unit, results or trigger stream, whose messages are small
	// references.
	DefaultWorkStreamMaxBytes = envBytes("ICARUS_WORK_STREAM_MAX_BYTES", 256<<20)
	// DefaultDLQStreamMaxBytes caps a dead letter stream.
	DefaultDLQStreamMaxBytes = envBytes("ICARUS_DLQ_STREAM_MAX_BYTES", 64<<20)
)

// StreamReplicas is how many replicas each stream or bucket gets (NATS_STREAM_REPLICAS, the same
// variable every Olympus service reads): 1 on a single NATS server, 3 on a three-node cluster,
// where an R1 stream is unavailable whenever its one node restarts or is drained.
func StreamReplicas() int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("NATS_STREAM_REPLICAS"))); err == nil && n > 0 {
		return n
	}
	return 1
}

// envBytes reads a positive byte count from the environment, or returns def.
func envBytes(name string, def int64) int64 {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

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

// StreamKind says which policy EnsureStreamPolicy applies.
type StreamKind int

const (
	// WorkStream is a unit, results or trigger stream: every message is work a consumer must do.
	WorkStream StreamKind = iota
	// DLQStream is a dead-letter stream, read by a person.
	DLQStream
)

// WorkStreamConfig applies the work-stream policy to a stream being created. Messages are kept
// until a consumer acknowledges them (WorkQueue), so the cap bounds only real backlog rather than
// work already done, and a full stream refuses new messages (DiscardNew): the publisher gets an
// error it can act on (Hermes answers 503, Zeus retries the dispatch) instead of the oldest
// unconsumed message being dropped without a trace (workplan infra-fault-tolerance, D3).
//
// Retention cannot change in place (NATS 2.10: "can not change retention policy to/from
// workqueue"), so a stream created before this keeps limits retention until it is migrated
// (scripts/nats/migrate-work-streams.sh); EnsureStreamPolicy gives such a stream the policy that
// is safe for it meanwhile.
func WorkStreamConfig(cfg jetstream.StreamConfig) jetstream.StreamConfig {
	cfg.Retention = jetstream.WorkQueuePolicy
	cfg.Discard = jetstream.DiscardNew
	cfg.MaxBytes = DefaultWorkStreamMaxBytes
	cfg.Replicas = StreamReplicas()
	return cfg
}

// DLQStreamConfig applies the dead-letter policy to a stream being created: messages are kept by
// limits (nobody consumes them), and a full stream refuses new ones, so a dead-letter write fails
// visibly and the original message is kept instead of being acknowledged into nothing.
func DLQStreamConfig(cfg jetstream.StreamConfig) jetstream.StreamConfig {
	cfg.Retention = jetstream.LimitsPolicy
	cfg.Discard = jetstream.DiscardNew
	cfg.MaxBytes = DefaultDLQStreamMaxBytes
	cfg.Replicas = StreamReplicas()
	return cfg
}

// policyFor is the cap and discard policy an existing stream of this kind and retention needs.
func policyFor(kind StreamKind, retention jetstream.RetentionPolicy) (int64, jetstream.DiscardPolicy) {
	if kind == DLQStream {
		return DefaultDLQStreamMaxBytes, jetstream.DiscardNew
	}
	if retention == jetstream.WorkQueuePolicy {
		return DefaultWorkStreamMaxBytes, jetstream.DiscardNew
	}
	// A work stream still on limits retention: its acknowledged messages count towards the cap,
	// so refusing new messages when full would stop all work. Drop the oldest until it is
	// migrated.
	return DefaultWorkStreamMaxBytes, jetstream.DiscardOld
}

// EnsureStreamPolicy brings an existing stream's cap and discard policy to what its kind and
// retention need, changing nothing else. It updates only when they differ, so it is cheap to call
// at start. Lowering the cap below the stream's current size drops its oldest messages.
func EnsureStreamPolicy(ctx context.Context, js StreamUpdater, stream jetstream.Stream, kind StreamKind, logger *zap.Logger) error {
	if js == nil || stream == nil {
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
	maxBytes, discard := policyFor(kind, cfg.Retention)
	if cfg.MaxBytes == maxBytes && cfg.Discard == discard {
		return nil
	}
	previous, previousDiscard := cfg.MaxBytes, cfg.Discard
	cfg.MaxBytes = maxBytes
	cfg.Discard = discard
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("set policy on stream %q: %w", cfg.Name, err)
	}
	if logger != nil {
		logger.Info("Updated JetStream stream cap and discard policy",
			zap.String("stream", cfg.Name),
			zap.String("retention", cfg.Retention.String()),
			zap.Int64("max_bytes_from", previous),
			zap.Int64("max_bytes_to", maxBytes),
			zap.String("discard_from", previousDiscard.String()),
			zap.String("discard_to", discard.String()))
	}
	return nil
}

// ExecutionHeartbeatsBucket is the KV bucket the runner writes each unit's liveness heartbeat and
// claim to, and Zeus's liveness sweeper reads.
const ExecutionHeartbeatsBucket = "EXECUTION_HEARTBEATS"

// ExecutionHeartbeatTTL is how long a heartbeat entry survives with no refresh: three missed
// writes, so a single missed write does not read as a dead pod.
const ExecutionHeartbeatTTL = 90 * time.Second

// HeartbeatBucketConfig is the configuration EXECUTION_HEARTBEATS is created with. Whichever side
// finds the bucket missing first creates it (the runner on a claim, Zeus's sweeper on a sweep),
// so both take it from here.
func HeartbeatBucketConfig() jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket:   ExecutionHeartbeatsBucket,
		TTL:      ExecutionHeartbeatTTL,
		Replicas: StreamReplicas(),
	}
}

// TriggerStreamDuplicates is WORKFLOW_TRIGGERS' deduplication window. Hermes publishes each
// trigger with its run_id as Nats-Msg-Id, and republishes one whose outcome was unknown under the
// same id for most of this window; the server recognises the repeat only inside it. Hermes and
// Zeus both create the stream, so both take it from here: a stream Zeus created had the server's
// 2-minute default, and a republish after that ran the workflow twice. 30 minutes because a
// trust's interface engine retries over many minutes and queues and forwards after an outage.
const TriggerStreamDuplicates = 30 * time.Minute
