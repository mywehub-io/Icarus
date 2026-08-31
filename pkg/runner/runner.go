// Package runner provides a concurrent message processing framework using NATS JetStream.
// It allows processing messages from a stream with configurable batch sizes and worker-pool-based concurrency,
// with built-in success and error reporting capabilities.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	icarusnats "github.com/wehubfusion/Icarus/internal/nats"
	internaltracing "github.com/wehubfusion/Icarus/internal/tracing"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Processor defines the interface for message processing implementations.
//
// Goroutine safety: the runner invokes Process concurrently from multiple
// worker goroutines. Implementations MUST be safe for concurrent use. Shared
// state (e.g. gRPC clients, connection pools, caches) must use locks or be
// immutable after construction.
//
// Return semantics:
//   - nil error → processing succeeded; the runner calls ReportSuccess and
//     ACKs the NATS message.
//   - non-nil error → processing failed; the runner calls ReportError, which
//     NAKs the NATS message for redelivery or routes to the DLQ when
//     MaxDeliver is exhausted. It also invokes ProcessFailureObserver.
//
// Context: the ctx passed to Process is derived from the runner's main context
// with an additional processTimeout deadline. When processTimeout fires,
// ctx.Err() == context.DeadlineExceeded. Check ctx.Err() before long
// operations to avoid unnecessary work after cancellation.
type Processor interface {
	Process(ctx context.Context, msg *message.Message) (message message.Message, err error)
}

// ProcessFailureObserver is called after ReportError successfully publishes a
// failed result. It is invoked for every processing error regardless of
// whether the error is transient or permanent.
//
// Typical use: emit a node.ended Argus observation event with HasError=true
// (see pkg/runner/argus/observer.go for the standard implementation).
//
// Idempotency: the observer MAY be called more than once for the same message
// when JetStream redelivers the message after an AckWait timeout. Implementations
// MUST be idempotent or tolerate duplicate invocations (e.g. use the message
// dedup key as the Argus event ID).
//
// Return errors only for logging; the runner does not retry the observer and
// does not change the NATS ack outcome based on the observer's return value.
type ProcessFailureObserver func(ctx context.Context, msg *message.Message, processErr error) error

// Metadata keys the runner sets on msg.Metadata when processErr carries EmbeddedFailureDetail,
// so a registered ProcessFailureObserver (e.g. pkg/runner/argus's) can tell a structured
// embedded-node failure apart from a plugin failing before its embedded subflow ever ran.
const (
	MetaEmbedFailedNodeID = "embed_failed_node_id"
	MetaEmbedRootCause    = "embed_root_cause"
)

// EmbeddedFailureDetail is implemented by an error that names which embedded node inside an
// execution unit actually failed. A plugin's embedded-node runtime (e.g. Icarus's own
// pkg/embedded/runtime, wrapped by a plugin's error type) already emits its own node.ended for
// every embedded node it processed before returning such an error; the runner reads this
// interface via errors.As so a ProcessFailureObserver can skip re-emitting (and so overwriting)
// those nodes' real statuses, rather than blanket-marking the whole unit failed.
type EmbeddedFailureDetail interface {
	EmbeddedFailureDetail() (failedNodeID, rootCause string)
}

// RunnerOption configures a Runner at construction time.
// Options are applied after the Runner struct is initialised and after stream/consumer
// existence is verified, so option closures may safely reference the resolved config.
type RunnerOption func(*Runner)

// WithProcessFailureObserver registers a ProcessFailureObserver that is called after
// every successful ReportError. Without this option, processing failures are logged but
// no Argus node.ended event is emitted, which causes nodes to remain in the "running"
// state in Athena and may cause Hermes trigger-sync to hang indefinitely waiting for
// the node-ended manifest entry.
//
// Use pkg/runner/argus.NewProcessFailureObserver for the standard Argus implementation.
func WithProcessFailureObserver(obs ProcessFailureObserver) RunnerOption {
	return func(r *Runner) {
		r.processFailureObserver = obs
	}
}

// WithConsumerFilterSubject sets the JetStream consumer FilterSubject (tenant/default routing).
func WithConsumerFilterSubject(filterSubject string) RunnerOption {
	return func(r *Runner) {
		r.consumerFilterSubject = filterSubject
	}
}

// Runner manages concurrent message processing from a NATS JetStream consumer.
// It pulls messages in batches and dispatches them through an internal worker
// pool, with automatic success and error reporting to the RESULTS stream.
//
// Goroutine safety: Runner itself is not safe for concurrent Start/Stop calls.
// Call Run once per Runner instance. Run is safe to call from a single
// goroutine; all internal concurrency is managed by the runner's worker pool.
//
// Cancellation: cancel the context passed to Run to initiate graceful shutdown.
// Run drains the job channel and waits for all in-flight Process calls to
// complete before returning. In-flight messages are allowed to finish; new
// pulls stop immediately on context cancellation. If a process context
// deadline (processTimeout) fires during shutdown, the worker logs a warning
// and reports the error, then the drain continues.
//
// Worker pool size: resolved at construction from Config.WorkerCount,
// ICARUS_RUNNER_WORKERS env, ICARUS_RUNNER_WORKER_MULTIPLIER × GOMAXPROCS,
// or GOMAXPROCS as the fallback (in that order). Queue depth defaults to
// 4 × WorkerCount (capped at 1000).
type Runner struct {
	client                 *client.Client
	processor              Processor
	stream                 string
	consumer               string
	consumerFilterSubject  string
	batchSize              int
	logger                 *zap.Logger
	processTimeout         time.Duration
	tracer                 trace.Tracer
	tracingShutdown        func(context.Context) error
	config                 Config
	jobChan                chan *message.Message
	processFailureObserver ProcessFailureObserver
	// heartbeatKV is the EXECUTION_HEARTBEATS bucket used for the liveness heartbeat and the
	// claim-per-execution-unit idempotency check (see startAckHeartbeat and claimExecutionUnit).
	// nil when the bucket could not be created/reached at startup; both mechanisms degrade to
	// no-ops (heartbeat) or fail-open (claim) when nil rather than blocking processing.
	heartbeatKV jetstream.KeyValue
}

// Config controls runner worker pool behavior.
type Config struct {
	// WorkerCount is the number of concurrent worker goroutines processing messages.
	// If 0 or less, it will be resolved from env or CPU count.
	WorkerCount int

	// QueueSize controls the buffered job queue feeding workers.
	// If 0 or less, it defaults to 4×WorkerCount (min WorkerCount, max 1000).
	QueueSize int
}

// DefaultConfig provides baseline values resolved at runtime.
func DefaultConfig() Config {
	return Config{
		WorkerCount: 0,
		QueueSize:   0,
	}
}

func (c Config) withDefaults() Config {
	workers := resolveWorkerCount(c.WorkerCount)
	queue := c.QueueSize
	if queue <= 0 {
		queue = workers * 4
		if queue < workers {
			queue = workers
		}
		if queue > 1000 {
			queue = 1000
		}
	}
	return Config{
		WorkerCount: workers,
		QueueSize:   queue,
	}
}

func resolveWorkerCount(configured int) int {
	if configured > 0 {
		return configured
	}

	if v := getEnvInt("ICARUS_RUNNER_WORKERS", 0); v > 0 {
		return v
	}
	if mult := getEnvInt("ICARUS_RUNNER_WORKER_MULTIPLIER", 0); mult > 0 {
		workers := runtime.GOMAXPROCS(0) * mult
		if workers > 0 {
			return workers
		}
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}
	return workers
}

func getEnvInt(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}
	return parsed
}

// NewRunner creates a new Runner instance with a connected client and stream/consumer configuration.
// The client must already be connected before creating the runner.
// The processor must implement the Processor interface for message handling.
// batchSize specifies how many messages to pull at once from the stream.
// processTimeout specifies the maximum time allowed for processing a single message.
// logger is the zap logger instance for structured logging.
// tracingConfig is optional - if nil, no tracing will be set up. If provided, tracing will be automatically configured and cleaned up.
// cfg controls worker pool sizing; if nil, DefaultConfig() is used.
// opts apply additional configuration (e.g. WithProcessFailureObserver).
// Returns an error if any of the parameters are invalid.
func NewRunner(client *client.Client, processor Processor, stream, consumer string, batchSize int, processTimeout time.Duration, logger *zap.Logger, tracingConfig *TracingConfig, cfg *Config, opts ...RunnerOption) (*Runner, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if processor == nil {
		return nil, errors.New("processor cannot be nil")
	}
	if stream == "" {
		return nil, errors.New("stream name cannot be empty")
	}
	if consumer == "" {
		return nil, errors.New("consumer name cannot be empty")
	}
	if batchSize <= 0 {
		return nil, errors.New("batchSize must be greater than 0")
	}
	if processTimeout <= 0 {
		return nil, errors.New("processTimeout must be greater than 0")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	config := DefaultConfig()
	if cfg != nil {
		config = *cfg
	}
	config = config.withDefaults()

	runner := &Runner{
		client:         client,
		processor:      processor,
		stream:         stream,
		consumer:       consumer,
		batchSize:      batchSize,
		processTimeout: processTimeout,
		logger:         logger,
		tracer:         otel.Tracer("icarus/runner"),
		config:         config,
		jobChan:        make(chan *message.Message, config.QueueSize),
	}

	for _, opt := range opts {
		opt(runner)
	}

	// Ensure the stream and consumer exist, create them if necessary
	ensureCtx := context.Background()
	if err := client.Messages.EnsureStream(ensureCtx, stream); err != nil {
		return nil, fmt.Errorf("failed to ensure stream '%s' exists: %w", stream, err)
	}

	if err := client.Messages.EnsureConsumer(ensureCtx, stream, consumer, runner.consumerFilterSubject); err != nil {
		return nil, fmt.Errorf("failed to ensure consumer '%s' exists: %w", consumer, err)
	}

	// Ensure the EXECUTION_HEARTBEATS KV bucket exists. Multiple runners (multiple plugins,
	// multiple replicas) race this at startup; treat "already exists" as success, the same way
	// EnsureConsumer above tolerates an existing durable. A missing bucket must not stop the
	// runner from processing messages — heartbeatKV stays nil and both the heartbeat write and
	// the claim check degrade gracefully (see startAckHeartbeat and claimExecutionUnit).
	//
	// client.JetStream() can be a nil jetstream.JetStream (e.g. client.NewClientWithJSContext,
	// a test-only constructor that never sets the underlying js handle); calling a method on a
	// nil interface panics, so guard it the same way a real KV-unreachable error is handled.
	if client.JetStream() == nil {
		logger.Warn("JetStream handle not available; EXECUTION_HEARTBEATS heartbeat and claim disabled for this runner")
	} else if kv, err := client.JetStream().KeyValue(ensureCtx, executionHeartbeatBucket); err == nil {
		runner.heartbeatKV = kv
	} else if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, createErr := client.JetStream().CreateKeyValue(ensureCtx, jetstream.KeyValueConfig{
			Bucket: executionHeartbeatBucket,
			TTL:    executionHeartbeatTTL,
		})
		if createErr == nil {
			runner.heartbeatKV = kv
		} else if errors.Is(createErr, jetstream.ErrBucketExists) {
			if kv, getErr := client.JetStream().KeyValue(ensureCtx, executionHeartbeatBucket); getErr == nil {
				runner.heartbeatKV = kv
			} else {
				logger.Warn("EXECUTION_HEARTBEATS bucket exists but could not be opened; heartbeat and claim disabled for this runner", zap.Error(getErr))
			}
		} else {
			logger.Warn("Failed to create EXECUTION_HEARTBEATS bucket; heartbeat and claim disabled for this runner", zap.Error(createErr))
		}
	} else {
		logger.Warn("Failed to reach EXECUTION_HEARTBEATS bucket; heartbeat and claim disabled for this runner", zap.Error(err))
	}

	// Setup tracing if configuration is provided
	if tracingConfig != nil {
		ctx := context.Background()
		internalCfg := tracingConfig.toInternalConfig()
		shutdown, err := internaltracing.SetupTracing(ctx, internalCfg, logger)
		if err != nil {
			logger.Warn("Failed to setup tracing, continuing without tracing", zap.Error(err))
		} else {
			runner.tracingShutdown = shutdown
			logger.Info("Tracing setup complete",
				zap.String("service", tracingConfig.ServiceName),
				zap.String("endpoint", tracingConfig.OTLPEndpoint))
		}
	}

	return runner, nil
}

// Close gracefully shuts down the runner and cleans up resources including tracing.
// This should be called when the runner is no longer needed.
func (r *Runner) Close() error {
	if r.tracingShutdown != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.tracingShutdown(ctx); err != nil {
			r.logger.Error("Error shutting down tracing", zap.Error(err))
			return err
		}
		r.logger.Info("Tracing shutdown complete")
	}

	return nil
}

// Run starts the message processing pipeline and blocks until shutdown completes.
//
// Startup: Run launches WorkerCount worker goroutines and one consume-supervision
// goroutine. The supervision goroutine starts a JetStream Consume() loop
// (new nats.go/jetstream API) that delivers messages to a callback; the callback
// wraps each message and dispatches it into the internal job queue with a
// blocking send, which provides natural backpressure. Worker pool sizing is
// resolved from Config.WorkerCount (see NewRunner for the resolution order).
//
// Shutdown — cancel the context to stop: cancelling ctx stops the Consume loop
// and all workers. Message delivery stops immediately; any messages already in
// jobChan are drained and processed to completion. Run returns only after every
// in-flight Process call has returned. There is no explicit stop timeout for
// draining workers — each Process call is bounded by processTimeout, so the
// maximum drain time is bounded by processTimeout.
//
// Error handling on consume failures: transient errors (heartbeat misses during
// server blips) are retried by the JetStream library itself. Fatal ErrHandler
// failures and unexpected Consume stops (Closed without ErrHandler, e.g.
// consumer deleted) restart the Consume loop with exponential backoff
// (100 ms → 5 s), reconnecting the client when the transport is down.
//
// Return values:
//   - context.Canceled / context.DeadlineExceeded — normal shutdown path when
//     the caller cancels ctx.
//
// There is no separate Stop method. Stopping is always done by cancelling the
// context passed to Run.
func (r *Runner) Run(ctx context.Context) error {
	var (
		backgroundWG sync.WaitGroup
		processWG    sync.WaitGroup
	)

	// Start workers
	for i := 0; i < r.config.WorkerCount; i++ {
		processWG.Add(1)
		go func(id int) {
			defer processWG.Done()
			r.worker(ctx, id)
		}(i)
	}

	dispatchMessage := func(msg *message.Message) {
		select {
		case <-ctx.Done():
			return
		case r.jobChan <- msg:
		}
	}

	// Consume callback: wrap the JetStream message and dispatch it into the
	// worker pool. The blocking send into jobChan provides backpressure — the
	// JetStream library stops requesting more messages while the callback blocks.
	handleMsg := func(jsMsg jetstream.Msg) {
		msg, err := message.FromJetStreamMsg(jsMsg)
		if err != nil {
			deliverCount := ""
			if md, mdErr := jsMsg.Metadata(); mdErr == nil && md != nil {
				deliverCount = strconv.FormatUint(md.NumDelivered, 10)
			}
			r.logger.Warn("Dropping malformed consumed message; NAK for redelivery",
				zap.String("stream", r.stream),
				zap.String("consumer", r.consumer),
				zap.String("subject", jsMsg.Subject()),
				zap.String("jetstream_deliver_count", deliverCount),
				zap.Error(err))
			_ = jsMsg.Nak()
			return
		}
		if msg.Metadata == nil {
			msg.Metadata = make(map[string]string)
		}
		if _, ok := msg.Metadata[message.MetaIcarusEnqueueUnixMs]; !ok {
			msg.WithMetadata(message.MetaIcarusEnqueueUnixMs, fmt.Sprintf("%d", time.Now().UnixMilli()))
		}
		r.logger.Info("Runner dispatching consumed message to job queue",
			zap.String("stream", r.stream),
			zap.String("consumer", r.consumer),
			zap.Int("job_queue_depth", len(r.jobChan)),
			zap.Int("job_queue_cap", cap(r.jobChan)),
			zap.Int("worker_count", r.config.WorkerCount),
			zap.Int("batch_size_config", r.batchSize))
		dispatchMessage(msg)
	}

	// Start consume supervision goroutine. It keeps a JetStream Consume() loop
	// alive, restarting it (with reconnect when needed) after fatal failures.
	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()

		backoffDelay := 100 * time.Millisecond
		maxBackoff := 5 * time.Second

		// waitBackoff sleeps for the current backoff delay (doubling it up to
		// maxBackoff) unless the context is cancelled first.
		waitBackoff := func() bool {
			select {
			case <-time.After(backoffDelay):
				if backoffDelay < maxBackoff {
					backoffDelay *= 2
				}
				return true
			case <-ctx.Done():
				return false
			}
		}

		for {
			select {
			case <-ctx.Done():
				r.logger.Info("Shutting down message processor...")
				return
			default:
			}

			cons, err := r.client.Messages.GetConsumer(ctx, r.stream, r.consumer)
			if err != nil {
				if ctx.Err() != nil {
					r.logger.Debug("Message consuming stopped due to context cancellation")
					return
				}
				r.logger.Error("Error resolving JetStream consumer", zap.Error(err))
				if icarusnats.IsTransportError(err) || !r.client.IsConnected() {
					if r.tryReconnectNATS() {
						backoffDelay = 100 * time.Millisecond
					}
				}
				if !waitBackoff() {
					return
				}
				continue
			}

			// Buffered channel so the error handler never blocks. Keep the latest
			// error (replace stale) so a later fatal is not dropped after a
			// transient heartbeat miss. Some terminal conditions stop Consume
			// without invoking ErrHandler — those are covered by Closed() below.
			consumeErrCh := make(chan error, 1)
			consumeCtx, err := cons.Consume(handleMsg,
				jetstream.PullMaxMessages(r.batchSize),
				jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, cerr error) {
					select {
					case consumeErrCh <- cerr:
					default:
						select {
						case <-consumeErrCh:
						default:
						}
						select {
						case consumeErrCh <- cerr:
						default:
						}
					}
				}),
			)
			if err != nil {
				r.logger.Error("Error starting JetStream consume", zap.Error(err))
				if icarusnats.IsTransportError(err) || !r.client.IsConnected() {
					if r.tryReconnectNATS() {
						backoffDelay = 100 * time.Millisecond
					}
				}
				if !waitBackoff() {
					return
				}
				continue
			}

			r.logger.Info("JetStream consume started",
				zap.String("stream", r.stream),
				zap.String("consumer", r.consumer),
				zap.Int("batch_size", r.batchSize))
			backoffDelay = 100 * time.Millisecond

			// Block until shutdown, a fatal consume error, or Consume stopping on
			// its own (Closed). Watching Closed matches the old pull-loop habit of
			// always continuing after a failed fetch — nats.go can Stop() on
			// e.g. consumer deleted without calling ConsumeErrHandler.
			restart := false
			for !restart {
				select {
				case <-ctx.Done():
					consumeCtx.Stop()
					// Wait for the consume goroutine (including any in-flight
					// callback) to finish so nothing sends on jobChan after Run
					// closes it. A callback blocked in dispatchMessage unblocks
					// via ctx.Done().
					<-consumeCtx.Closed()
					r.logger.Info("Shutting down message processor...")
					return
				case cerr := <-consumeErrCh:
					// Only true Consume fatals tear down the loop. Do not treat
					// !IsConnected as fatal here: during nats auto-reconnect the
					// library may emit ErrNoHeartbeat while Consume is still alive
					// and will re-issue pulls on CONNECTED. Stopping would fight that.
					if isFatalConsumeError(cerr) {
						r.logger.Error("JetStream consume failure; restarting consume loop",
							zap.String("stream", r.stream),
							zap.String("consumer", r.consumer),
							zap.Error(cerr))
						consumeCtx.Stop()
						<-consumeCtx.Closed()
						if icarusnats.IsTransportError(cerr) || !r.client.IsConnected() {
							r.tryReconnectNATS()
						}
						restart = true
					} else {
						// Transient (e.g. missed heartbeats during a server blip);
						// the JetStream library keeps retrying pulls on its own.
						r.logger.Warn("JetStream consume transient error",
							zap.String("stream", r.stream),
							zap.String("consumer", r.consumer),
							zap.Error(cerr))
					}
				case <-consumeCtx.Closed():
					if ctx.Err() != nil {
						r.logger.Info("Shutting down message processor...")
						return
					}
					r.logger.Error("JetStream consume stopped; restarting consume loop",
						zap.String("stream", r.stream),
						zap.String("consumer", r.consumer))
					if !r.client.IsConnected() {
						r.tryReconnectNATS()
					}
					restart = true
				}
			}

			if !waitBackoff() {
				return
			}
		}
	}()

	// Wait for all goroutines to finish or context cancellation
	done := make(chan struct{})
	go func() {
		defer close(done)
		backgroundWG.Wait()
	}()

	// Wait for completion or context cancellation
	select {
	case <-done:
		close(r.jobChan)
		processWG.Wait()
		r.logger.Info("Runner completed successfully")
		return nil
	case <-ctx.Done():
		backgroundWG.Wait()
		// Stop accepting new messages and drain workers
		close(r.jobChan)
		processWG.Wait()
		r.logger.Info("Runner stopped due to context cancellation")
		return ctx.Err()
	}
}

// worker executes messages from the job channel until context cancellation or channel close.
func (r *Runner) worker(ctx context.Context, id int) {
	r.logger.Debug("runner worker started", zap.Int("worker_id", id))
	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("runner worker stopping due to context cancellation", zap.Int("worker_id", id))
			return
		case msg, ok := <-r.jobChan:
			if !ok {
				r.logger.Debug("runner worker stopping, job channel closed", zap.Int("worker_id", id))
				return
			}
			if err := r.processMessage(ctx, msg); err != nil {
				r.logger.Error("Message processing failed", zap.Error(err))
			}
		}
	}
}

// backgroundWithSpan returns a fresh context.Background()-rooted context (so report/observer
// calls still go through even if the parent ctx is cancelled by a runner shutdown) that carries
// the given span's SpanContext, so a traceparent can still be Inject-ed from it. Trace linkage
// without cancellation coupling.
func backgroundWithSpan(span trace.Span) context.Context {
	return trace.ContextWithSpanContext(context.Background(), span.SpanContext())
}

// processMessage handles the actual message processing logic.
// It returns an error when message processing or result reporting fails.
// ackHeartbeatInterval is how often an in-flight message extends its ack deadline.
//
// EnsureConsumer creates durables without an explicit AckWait, so the NATS server default of
// 30s applies; 10s gives three chances to extend before that expires. It is deliberately not
// derived from consumer config: existing durables are never modified, so a deployed consumer's
// real AckWait cannot be assumed to match anything we would compute locally. A fixed interval
// comfortably under the smallest plausible AckWait is the safe choice.
//
// A var rather than a const so tests can shorten it; nothing outside this package writes it.
var ackHeartbeatInterval = 10 * time.Second

// executionHeartbeatBucket is the NATS KV bucket used for the liveness heartbeat Zeus's sweeper
// polls (see the temporal-removal plan's phase-2) and for the claim-per-execution-unit idempotency
// check in processMessage. New as of this change; nothing else in Icarus reads or writes it.
const executionHeartbeatBucket = "EXECUTION_HEARTBEATS"

// executionHeartbeatTTL is how long a heartbeat entry survives with no refresh. Three missed
// writes (executionHeartbeatEveryNTicks x ackHeartbeatInterval x 3 = 90s) before expiry, so a
// single missed KV write does not read as a dead pod.
const executionHeartbeatTTL = 90 * time.Second

// executionHeartbeatEveryNTicks makes the KV write fire every 3rd ack-extension tick (10s x 3 =
// 30s), per the design doc: one ticker, two cadences, rather than a second time.Ticker.
const executionHeartbeatEveryNTicks = 3

// executionHeartbeat is the JSON payload written to executionHeartbeatBucket.
type executionHeartbeat struct {
	WorkflowID  string `json:"workflow_id"`
	RunID       string `json:"run_id"`
	NodeID      string `json:"node_id"`
	ExecutionID string `json:"execution_id"`
	Attempt     int    `json:"attempt"`
	Pod         string `json:"pod"`
	StartedAt   string `json:"started_at"` // RFC3339, set once per delivery, not per tick
}

// executionHeartbeatKey builds the EXECUTION_HEARTBEATS key for one execution unit. Dots, not
// colons, since KV keys become NATS subject tokens internally and ':' is not a valid one.
func executionHeartbeatKey(workflowID, runID, nodeID string) string {
	return fmt.Sprintf("%s.%s.%s", workflowID, runID, nodeID)
}

// deliverAttempt returns msg's JetStream NumDelivered, or 0 if msg has no JetStream handle or the
// metadata call fails (Core NATS / synthesised messages, or a transient metadata-fetch error).
func deliverAttempt(msg *message.Message) int {
	jsMsg := msg.GetJetStreamMsg()
	if jsMsg == nil {
		return 0
	}
	md, err := jsMsg.Metadata()
	if err != nil || md == nil {
		return 0
	}
	return int(md.NumDelivered)
}

// claimExecutionUnit attempts to claim key in kv via Create (not Put), so a second pod racing the
// same execution unit (a redelivery landing on a different pod while the first pod is still alive
// and heartbeating) fails the claim instead of running the plugin twice. Returns (true, nil) when
// this call won the claim, (false, nil) when jetstream.ErrKeyExists (someone else already holds
// it), and (false, err) for any other error — callers must fail OPEN on that last case: a KV
// outage must not stop all processing platform-wide.
func claimExecutionUnit(ctx context.Context, kv jetstream.KeyValue, key string, hb executionHeartbeat) (bool, error) {
	payload, err := json.Marshal(hb)
	if err != nil {
		return false, err
	}
	if _, err := kv.Create(ctx, key, payload); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// claimOrNak is processMessage's entry-point wrapper around claimExecutionUnit. It returns true
// when processing should proceed (the claim was won, heartbeatKV is nil, or the claim attempt
// failed for a reason other than losing the race — fail-open), and false when the message has
// already been nak'd and processMessage must return without calling Process.
//
// Isolated as its own method (rather than inlined in processMessage) so the fail-open and
// lost-race paths are testable without a real *client.Client — both would otherwise be
// unreachable in a unit test, since a won claim falls through into Process and the real
// ReportSuccess/ReportError machinery.
func (r *Runner) claimOrNak(ctx context.Context, msg *message.Message, workflowID, runID, nodeID, executionID string) bool {
	if r.heartbeatKV == nil {
		return true
	}
	key := executionHeartbeatKey(workflowID, runID, nodeID)
	hb := executionHeartbeat{
		WorkflowID:  workflowID,
		RunID:       runID,
		NodeID:      nodeID,
		ExecutionID: executionID,
		Attempt:     deliverAttempt(msg),
		Pod:         os.Getenv("HOSTNAME"),
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	claimed, claimErr := claimExecutionUnit(ctx, r.heartbeatKV, key, hb)
	if claimErr == nil && !claimed {
		r.logger.Info("Execution unit already claimed by another pod; nak-ing without processing",
			zap.String("stream", r.stream),
			zap.String("consumer", r.consumer),
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("node_id", nodeID))
		if nakErr := msg.Nak(); nakErr != nil {
			r.logger.Warn("Failed to nak message after losing claim race", zap.Error(nakErr))
		}
		return false
	}
	if claimErr != nil {
		// KV unreachable, not ErrKeyExists: fail OPEN. A KV outage must not halt all
		// processing platform-wide — a broken heartbeat/claim mechanism already looks
		// identical to "every pod is dead" from Zeus's side, an accepted degradation.
		r.logger.Warn("Failed to claim execution unit (KV unreachable); processing anyway",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("node_id", nodeID),
			zap.Error(claimErr))
	}
	return true
}

// startAckHeartbeat extends the message's ack deadline every ackHeartbeatInterval until the
// returned stop function is called. Stop is idempotent and waits for the goroutine to exit.
//
// Handlers routinely run far longer than the ack deadline (Elysium's plugin timeouts range
// from 2 to 30 minutes against a 30s AckWait). Without heartbeating, JetStream redelivers
// while the original is still running, and after MaxDeliver attempts the message is exhausted
// mid-flight. With more than one consumer replica the redelivery lands on a *different* pod,
// so the same unit executes twice concurrently.
//
// Failures are logged, not propagated: a missed extension costs a redelivery, whereas failing
// the message would discard work that is still in progress.
func (r *Runner) startAckHeartbeat(ctx context.Context, msg *message.Message, workflowID, runID, nodeID string) func() {
	if msg == nil || msg.GetJetStreamMsg() == nil {
		return func() {} // Core NATS or a synthesised message: nothing to extend.
	}

	done := make(chan struct{})
	stopped := make(chan struct{})

	executionID := ""
	if msg.Payload != nil {
		executionID = msg.Payload.ExecutionID
	}
	pod := os.Getenv("HOSTNAME")
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	key := executionHeartbeatKey(workflowID, runID, nodeID)

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(ackHeartbeatInterval)
		defer ticker.Stop()
		tick := 0
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					r.logger.Warn("Failed to extend ack deadline for in-flight message; it may be redelivered while still running",
						zap.String("stream", r.stream),
						zap.String("consumer", r.consumer),
						zap.String("workflow_id", workflowID),
						zap.String("run_id", runID),
						zap.String("node_id", nodeID),
						zap.Error(err))
					continue
				}
				r.logger.Debug("Extended ack deadline for in-flight message",
					zap.String("stream", r.stream),
					zap.String("consumer", r.consumer),
					zap.String("workflow_id", workflowID),
					zap.String("run_id", runID),
					zap.String("node_id", nodeID))

				tick++
				if r.heartbeatKV == nil || tick%executionHeartbeatEveryNTicks != 0 {
					continue
				}
				hb := executionHeartbeat{
					WorkflowID:  workflowID,
					RunID:       runID,
					NodeID:      nodeID,
					ExecutionID: executionID,
					Attempt:     deliverAttempt(msg),
					Pod:         pod,
					StartedAt:   startedAt,
				}
				payload, marshalErr := json.Marshal(hb)
				if marshalErr != nil {
					r.logger.Warn("Failed to marshal execution heartbeat", zap.Error(marshalErr))
					continue
				}
				// Put, not Create: the claim in processMessage already Created this key before
				// this ticker started, so this pod owns it. Every write here is a refresh.
				if _, err := r.heartbeatKV.Put(ctx, key, payload); err != nil {
					r.logger.Warn("Failed to write execution heartbeat; Zeus's sweeper may see this unit as stalled while it is actually running",
						zap.String("workflow_id", workflowID),
						zap.String("run_id", runID),
						zap.String("node_id", nodeID),
						zap.Error(err))
				}
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-stopped
	}
}

func (r *Runner) processMessage(ctx context.Context, msg *message.Message) error {
	// Extract workflow information for reporting
	var workflowID, runID, correlationID string
	if msg.Workflow != nil {
		workflowID = msg.Workflow.WorkflowID
		runID = msg.Workflow.RunID
	}
	correlationID = msg.CorrelationID

	// Generate correlation ID if not present
	if correlationID == "" && workflowID != "" && runID != "" {
		correlationID = fmt.Sprintf("%s-%s", workflowID, runID)
		msg.CorrelationID = correlationID
	}

	executionID := ""
	nodeID := ""
	if msg.Payload != nil {
		executionID = msg.Payload.ExecutionID
		nodeID = msg.Payload.NodeID
	}
	jetstreamDeliver := ""
	if msg.Metadata != nil {
		jetstreamDeliver = msg.Metadata[message.MetaJetStreamDeliverCount]
	}
	queueWaitMs := int64(0)
	if msg.Metadata != nil {
		if s := msg.Metadata[message.MetaIcarusEnqueueUnixMs]; s != "" {
			if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
				queueWaitMs = time.Now().UnixMilli() - ms
			}
		}
	}
	r.logger.Info("Runner processMessage start",
		zap.String("stream", r.stream),
		zap.String("consumer", r.consumer),
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("correlation_id", correlationID),
		zap.String("execution_id", executionID),
		zap.String("node_id", nodeID),
		zap.String("jetstream_deliver_count", jetstreamDeliver),
		zap.Int64("queue_wait_ms", queueWaitMs))

	// Extract the publisher's traceparent (if any) from Metadata before starting our span, so
	// this becomes a child of the publisher's span instead of a disconnected root. Icarus's NATS
	// publish path has no header option, so this travels inside the already-serialized Metadata
	// map (message.MetaTraceParent) rather than a NATS message header — see that constant's doc
	// comment. A no-op (fresh root span) if absent, e.g. an older publisher.
	if msg.Metadata != nil {
		if tp := msg.Metadata[message.MetaTraceParent]; tp != "" {
			ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": tp})
		}
	}

	// Start tracing span for message processing
	ctx, span := r.tracer.Start(ctx, "runner.processMessage",
		trace.WithAttributes(
			attribute.String("workflow.id", workflowID),
			attribute.String("workflow.run_id", runID),
			attribute.String("correlation.id", correlationID),
			attribute.String("stream", r.stream),
			attribute.String("consumer", r.consumer),
		))
	defer span.End()

	// Create a timeout context for message processing that respects both the main context and timeout
	processCtx, cancel := context.WithTimeout(ctx, r.processTimeout)
	defer cancel()

	// Check if the main context is already cancelled before starting processing
	select {
	case <-ctx.Done():
		r.logger.Info("Skipping message processing due to context cancellation",
			zap.String("workflowID", workflowID),
			zap.String("runID", runID),
			zap.String("correlationID", correlationID))
		span.SetStatus(codes.Error, "Context cancelled before processing")
		return ctx.Err()
	default:
		// Continue with processing
	}

	start := time.Now()
	r.logger.Info("Processing message",
		zap.String("workflowID", workflowID),
		zap.String("runID", runID),
		zap.String("correlationID", correlationID))

	// Start nested span for processor.Process call
	processCtx, processSpan := r.tracer.Start(processCtx, "processor.Process")
	// Add message attributes if available
	if correlationID != "" {
		processSpan.SetAttributes(attribute.String("correlation.id", correlationID))
	}
	if msg.Node != nil {
		processSpan.SetAttributes(attribute.String("message.node.id", msg.Node.NodeID))
	}
	if msg.Payload != nil {
		attrs := []attribute.KeyValue{}
		// Add plugin type from metadata if available
		if pluginType := msg.Metadata["plugin_type"]; pluginType != "" {
			attrs = append(attrs, attribute.String("message.plugin_type", pluginType))
		}
		// Add execution context fields from Payload
		if msg.Payload.ExecutionID != "" {
			attrs = append(attrs, attribute.String("message.execution_id", msg.Payload.ExecutionID))
		}
		if msg.Payload.WorkflowID != "" {
			attrs = append(attrs, attribute.String("message.workflow_id", msg.Payload.WorkflowID))
		}
		if msg.Payload.RunID != "" {
			attrs = append(attrs, attribute.String("message.run_id", msg.Payload.RunID))
		}
		if msg.Payload.NodeID != "" {
			attrs = append(attrs, attribute.String("message.node_id", msg.Payload.NodeID))
		}
		processSpan.SetAttributes(attrs...)
	}
	processSpan.SetAttributes(attribute.String("message.created_at", msg.CreatedAt))
	defer processSpan.End()

	// Claim this execution unit before doing any work, so a second pod racing the same unit
	// (a redelivery landing on a different pod while the first pod is still alive and
	// heartbeating) backs off instead of running the plugin twice. This is the idempotency
	// replacement for Temporal's deterministic-workflow-ID dedup.
	if !r.claimOrNak(processCtx, msg, workflowID, runID, nodeID, executionID) {
		return nil
	}

	// Hold the JetStream ack deadline open for as long as Process runs. Without this, any
	// handler slower than the server's AckWait is redelivered while it is still executing —
	// see startAckHeartbeat.
	stopHeartbeat := r.startAckHeartbeat(processCtx, msg, workflowID, runID, nodeID)

	// Process the message
	resultMessage, processErr := r.processor.Process(processCtx, msg)
	stopHeartbeat()
	processingTime := time.Since(start)

	// Add processing time to spans
	span.SetAttributes(attribute.Int64("processing.duration_ms", processingTime.Milliseconds()))
	processSpan.SetAttributes(attribute.Int64("processing.duration_ms", processingTime.Milliseconds()))

	if processErr != nil {
		// Record error in spans
		span.RecordError(processErr)
		span.SetStatus(codes.Error, processErr.Error())
		processSpan.RecordError(processErr)
		processSpan.SetStatus(codes.Error, processErr.Error())

		// Surface which embedded node actually failed (if any) onto msg.Metadata before
		// ReportError/the failure observer run, so a registered ProcessFailureObserver can
		// take its safe, no-re-emit path instead of blanket-marking every embedded node in
		// the unit as failed — which would overwrite nodes that already reported their own,
		// real "success".
		var failureDetail EmbeddedFailureDetail
		if errors.As(processErr, &failureDetail) {
			if failedNodeID, rootCause := failureDetail.EmbeddedFailureDetail(); failedNodeID != "" {
				if msg.Metadata == nil {
					msg.Metadata = make(map[string]string)
				}
				msg.Metadata[MetaEmbedFailedNodeID] = failedNodeID
				msg.Metadata[MetaEmbedRootCause] = rootCause
			}
		}

		// Explicitly detect process-timeout vs parent-shutdown vs regular errors so callers
		// can distinguish "Icarus processTimeout hit" from a genuine plugin error.
		if processCtx.Err() == context.DeadlineExceeded {
			r.logger.Warn("Process timeout exceeded for message (processCtx deadline exceeded — Icarus processTimeout config; node.ended will be emitted via ProcessFailureObserver if client_id is set)",
				zap.Duration("processingTime", processingTime),
				zap.Duration("process_timeout_config", r.processTimeout),
				zap.String("workflowID", workflowID),
				zap.String("runID", runID),
				zap.String("correlationID", correlationID),
				zap.String("execution_id", executionID),
				zap.String("node_id", nodeID),
				zap.String("jetstream_deliver_count", jetstreamDeliver),
				zap.String("grep_hint", "If Temporal run.Get is blocked, FHIR/HL7 workflow may be stuck; check Temporal for workflow_id"))
		} else if ctx.Err() != nil {
			r.logger.Warn("Process cancelled by parent context (runner shutting down?)",
				zap.Duration("processingTime", processingTime),
				zap.String("workflowID", workflowID),
				zap.String("runID", runID),
				zap.String("correlationID", correlationID),
				zap.String("execution_id", executionID),
				zap.String("node_id", nodeID),
				zap.String("jetstream_deliver_count", jetstreamDeliver),
				zap.Error(ctx.Err()))
		}

		r.logger.Error("Error processing message",
			zap.Duration("processingTime", processingTime),
			zap.String("workflowID", workflowID),
			zap.String("runID", runID),
			zap.String("correlationID", correlationID),
			zap.String("execution_id", executionID),
			zap.String("node_id", nodeID),
			zap.String("jetstream_deliver_count", jetstreamDeliver),
			zap.Error(processErr))

		// Report error if we have workflow information
		// Use a background context with timeout to ensure reporting works even if parent context is cancelled
		if workflowID != "" && runID != "" {
			// Extract executionID from Payload
			executionID := ""
			if msg.Payload != nil {
				executionID = msg.Payload.ExecutionID
			}

			reportCtx, reportCancel := context.WithTimeout(backgroundWithSpan(span), 10*time.Minute)
			defer reportCancel()

			if reportErr := r.client.Messages.ReportError(reportCtx, executionID, workflowID, runID, correlationID, processErr, msg.GetJetStreamMsg()); reportErr != nil {
				// Critical: If we can't report the error, log it extensively but don't fail silently
				r.logger.Error("CRITICAL: Failed to report error to JetStream - workflow may hang",
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.String("executionID", executionID),
					zap.String("originalError", processErr.Error()),
					zap.Error(reportErr))

				if icarusnats.IsTransportError(reportErr) || !r.client.IsConnected() {
					r.tryReconnectNATS()
				}

				// Try one more time with a fresh context after a brief delay
				time.Sleep(2 * time.Second)
				retryCtx, retryCancel := context.WithTimeout(backgroundWithSpan(span), 30*time.Second)
				if retryErr := r.client.Messages.ReportError(retryCtx, executionID, workflowID, runID, correlationID, processErr, msg.GetJetStreamMsg()); retryErr != nil {
					r.logger.Error("CRITICAL: Retry also failed to report error to JetStream",
						zap.String("workflowID", workflowID),
						zap.String("runID", runID),
						zap.String("executionID", executionID),
						zap.Error(retryErr))
				} else {
					r.logger.Info("Successfully reported error to JetStream on retry",
						zap.String("workflowID", workflowID),
						zap.String("executionID", executionID))
				}
				retryCancel()
			}
		} else {
			// If we don't have workflow info, still nak the message since processing failed
			r.logger.Warn("Runner: processing failed without workflow/run_id on message — NAK for redelivery",
				zap.String("execution_id", executionID),
				zap.String("node_id", nodeID),
				zap.String("jetstream_deliver_count", jetstreamDeliver),
				zap.String("correlation_id", correlationID))
			if nakErr := msg.Nak(); nakErr != nil {
				r.logger.Error("Error naking message after processing failure",
					zap.Error(nakErr))
			}
		}

		if r.processFailureObserver != nil {
			obsCtx, obsCancel := context.WithTimeout(backgroundWithSpan(span), 30*time.Second)
			if obsErr := r.processFailureObserver(obsCtx, msg, processErr); obsErr != nil {
				r.logger.Error("process failure observer failed after ReportError",
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.Error(obsErr))
			}
			obsCancel()
		} else {
			r.logger.Error("process failure observer not set, skipping",
				zap.String("workflowID", workflowID),
				zap.String("runID", runID))
		}

		return processErr
	}

	// Mark spans as successful
	span.SetStatus(codes.Ok, "Message processed successfully")
	processSpan.SetStatus(codes.Ok, "Message processed successfully")

	// Ensure result message has correlation ID and workflow/node info
	if resultMessage.CorrelationID == "" && correlationID != "" {
		resultMessage.CorrelationID = correlationID
	}
	// Ensure workflow info is preserved in result message
	if resultMessage.Workflow == nil && workflowID != "" && runID != "" {
		resultMessage.Workflow = &message.Workflow{
			WorkflowID: workflowID,
			RunID:      runID,
		}
	}
	// Ensure node info is preserved in result message
	if resultMessage.Node == nil && msg.Node != nil {
		resultMessage.Node = msg.Node
	}

	// Add result message attributes if available
	if resultMessage.CorrelationID != "" {
		span.SetAttributes(attribute.String("result.correlation.id", resultMessage.CorrelationID))
	}
	if resultMessage.Node != nil {
		span.SetAttributes(attribute.String("result.message.node.id", resultMessage.Node.NodeID))
	}
	if resultMessage.Output != nil {
		span.SetAttributes(attribute.String("result.message.output.destination_type", resultMessage.Output.DestinationType))
	}

	jetstreamDeliverEnd := ""
	if msg.Metadata != nil {
		jetstreamDeliverEnd = msg.Metadata[message.MetaJetStreamDeliverCount]
	}
	r.logger.Info("Successfully processed message",
		zap.String("workflowID", workflowID),
		zap.String("runID", runID),
		zap.String("correlationID", correlationID),
		zap.String("jetstream_deliver_count", jetstreamDeliverEnd),
		zap.Duration("processingTime", processingTime))
	// Report success if we have workflow information
	// Use a longer timeout for large blob uploads (10 minutes to handle very large files)
	if workflowID != "" && runID != "" {
		reportCtx, reportCancel := context.WithTimeout(backgroundWithSpan(span), 10*time.Minute)
		defer reportCancel()
		if reportErr := r.client.Messages.ReportSuccess(reportCtx, resultMessage, msg.GetJetStreamMsg()); reportErr != nil {
			r.logger.Error("Error reporting success, will report as error to workflow",
				zap.String("workflowID", workflowID),
				zap.String("runID", runID),
				zap.Error(reportErr))

			if icarusnats.IsTransportError(reportErr) || !r.client.IsConnected() {
				r.tryReconnectNATS()
			}

			// Extract executionID from resultMessage metadata
			executionID := ""
			if resultMessage.Metadata != nil {
				executionID = resultMessage.Metadata["execution_id"]
			}

			// Report the failure as an error to Temporal so the workflow knows about it
			// Use longer timeout (30s) to match ReportError's retry logic
			errorCtx, errorCancel := context.WithTimeout(backgroundWithSpan(span), 30*time.Second)
			defer errorCancel()

			if errorReportErr := r.client.Messages.ReportError(errorCtx, executionID, workflowID, runID, correlationID, reportErr, msg.GetJetStreamMsg()); errorReportErr != nil {
				// Critical: If we can't report the error, log it extensively
				r.logger.Error("CRITICAL: Failed to report error to workflow after success report failed - workflow may hang",
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.String("executionID", executionID),
					zap.String("originalError", reportErr.Error()),
					zap.Error(errorReportErr))

				if icarusnats.IsTransportError(errorReportErr) || !r.client.IsConnected() {
					r.tryReconnectNATS()
				}

				// Try one more time with a fresh context
				time.Sleep(2 * time.Second)
				retryCtx, retryCancel := context.WithTimeout(backgroundWithSpan(span), 30*time.Second)
				if retryErr := r.client.Messages.ReportError(retryCtx, executionID, workflowID, runID, correlationID, reportErr, msg.GetJetStreamMsg()); retryErr != nil {
					r.logger.Error("CRITICAL: Retry also failed to report error to JetStream",
						zap.String("workflowID", workflowID),
						zap.String("executionID", executionID),
						zap.Error(retryErr))
				} else {
					r.logger.Info("Successfully reported error to JetStream on retry",
						zap.String("workflowID", workflowID),
						zap.String("executionID", executionID))
				}
				retryCancel()
			}
			return reportErr
		}
	} else {
		// If we don't have workflow info, still ack the message since processing succeeded
		r.logger.Warn("Runner: processed message success without workflow/run_id — direct Ack (no result publish to Zeus)",
			zap.String("jetstream_deliver_count", jetstreamDeliverEnd),
			zap.String("correlation_id", correlationID),
			zap.String("execution_id", executionID),
			zap.String("node_id", nodeID))
		if ackErr := msg.Ack(); ackErr != nil {
			r.logger.Error("Error acking message after successful processing",
				zap.Error(ackErr))
			return ackErr
		}
	}

	return nil
}

// isFatalConsumeError reports whether an error surfaced by the JetStream
// ConsumeErrHandler requires tearing down and restarting the Consume loop
// (as opposed to transient errors the library retries internally).
func isFatalConsumeError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, jetstream.ErrConnectionClosed)
}

// tryReconnectNATS attempts to restore a dead NATS connection. Returns true on success.
func (r *Runner) tryReconnectNATS() bool {
	reconnectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.client.EnsureConnected(reconnectCtx); err != nil {
		r.logger.Warn("NATS reconnect failed", zap.Error(err))
		return false
	}
	r.logger.Info("NATS reconnected after transport failure")
	return true
}
