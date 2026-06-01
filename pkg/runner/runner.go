// Package runner provides a concurrent message processing framework using NATS JetStream.
// It allows processing messages from a stream with configurable batch sizes and worker-pool-based concurrency,
// with built-in success and error reporting capabilities.
package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	internaltracing "github.com/wehubfusion/Icarus/internal/tracing"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

// WithSubjectFilter configures the runner to pull messages from the specified subject
// instead of the default (stream-level) subject.
func WithSubjectFilter(subject string) RunnerOption {
	return func(r *Runner) {
		r.subject = subject
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
	subject                string
	stream                 string
	consumer               string
	batchSize              int
	logger                 *zap.Logger
	processTimeout         time.Duration
	tracer                 trace.Tracer
	tracingShutdown        func(context.Context) error
	config                 Config
	jobChan                chan *message.Message
	processFailureObserver ProcessFailureObserver
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

	// Ensure the stream and consumer exist, create them if necessary
	if err := client.Messages.EnsureStream(stream); err != nil {
		return nil, fmt.Errorf("failed to ensure stream '%s' exists: %w", stream, err)
	}

	if err := client.Messages.EnsureConsumer(stream, consumer); err != nil {
		return nil, fmt.Errorf("failed to ensure consumer '%s' exists: %w", consumer, err)
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

	for _, opt := range opts {
		if opt != nil {
			opt(runner)
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
// Startup: Run launches WorkerCount worker goroutines and one puller goroutine,
// then enters a pull-dispatch-ack loop. Worker pool sizing is resolved from
// Config.WorkerCount (see NewRunner for the resolution order).
//
// Shutdown — cancel the context to stop: cancelling ctx signals both the puller
// and all workers. The puller stops pulling immediately; any messages already in
// jobChan are drained and processed to completion. Run returns only after every
// in-flight Process call has returned. There is no explicit stop timeout for
// draining workers — each Process call is bounded by processTimeout, so the
// maximum drain time is bounded by processTimeout.
//
// Error handling on pull failures: transient pull errors are retried with
// exponential backoff (100 ms → 5 s). Permanent or context-cancelled pull
// failures cause Run to return the context error.
//
// Return values:
//   - nil — the puller goroutine exited cleanly (rare; requires the pull loop to
//     return on its own rather than via ctx.Done()).
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

	// Start message puller goroutine
	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()

		backoffDelay := 100 * time.Millisecond
		maxBackoff := 5 * time.Second

		for {
			select {
			case <-ctx.Done():
				r.logger.Info("Shutting down message processor...")
				return
			default:
				// Pull messages from the stream
				messages, err := r.client.Messages.PullMessages(ctx, r.subject, r.stream, r.consumer, r.batchSize)
				if err != nil {
					// Check if this is due to context cancellation (graceful shutdown)
					if ctx.Err() != nil {
						r.logger.Debug("Message pulling stopped due to context cancellation")
						return // Context cancelled
					}
					// This is an actual error, not graceful shutdown
					r.logger.Error("Error pulling messages", zap.Error(err))
					// Exponential backoff for errors
					time.Sleep(backoffDelay)
					if backoffDelay < maxBackoff {
						backoffDelay *= 2
					}
					continue
				}

				if len(messages) == 0 {
					// No messages available, use shorter backoff to avoid busy waiting
					// but don't reset backoff completely as this is normal behavior
					select {
					case <-time.After(500 * time.Millisecond):
					case <-ctx.Done():
						return
					}
					continue
				}

				// Reset backoff on successful pull
				backoffDelay = 100 * time.Millisecond

				// Dispatch messages via worker pool
				batchLen := len(messages)
				for i, msg := range messages {
					select {
					case <-ctx.Done():
						return // Context cancelled, stop sending messages
					default:
						if msg.Metadata == nil {
							msg.Metadata = make(map[string]string)
						}
						if _, ok := msg.Metadata[message.MetaIcarusEnqueueUnixMs]; !ok {
							msg.WithMetadata(message.MetaIcarusEnqueueUnixMs, fmt.Sprintf("%d", time.Now().UnixMilli()))
						}
						r.logger.Info("Runner dispatching pulled message to job queue",
							zap.String("stream", r.stream),
							zap.String("consumer", r.consumer),
							zap.Int("batch_index", i),
							zap.Int("batch_len", batchLen),
							zap.Int("job_queue_depth", len(r.jobChan)),
							zap.Int("job_queue_cap", cap(r.jobChan)),
							zap.Int("worker_count", r.config.WorkerCount),
							zap.Int("batch_size_config", r.batchSize))
						dispatchMessage(msg)
					}
				}
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

// processMessage handles the actual message processing logic.
// It returns an error when message processing or result reporting fails.
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

	// Process the message
	resultMessage, processErr := r.processor.Process(processCtx, msg)
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

			reportCtx, reportCancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer reportCancel()

			if reportErr := r.client.Messages.ReportError(reportCtx, executionID, workflowID, runID, correlationID, processErr, msg.GetNATSMsg()); reportErr != nil {
				// Critical: If we can't report the error, log it extensively but don't fail silently
				r.logger.Error("CRITICAL: Failed to report error to JetStream - workflow may hang",
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.String("executionID", executionID),
					zap.String("originalError", processErr.Error()),
					zap.Error(reportErr))

				// Try one more time with a fresh context after a brief delay
				time.Sleep(2 * time.Second)
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if retryErr := r.client.Messages.ReportError(retryCtx, executionID, workflowID, runID, correlationID, processErr, msg.GetNATSMsg()); retryErr != nil {
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
			obsCtx, obsCancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		reportCtx, reportCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer reportCancel()
		if reportErr := r.client.Messages.ReportSuccess(reportCtx, resultMessage, msg.GetNATSMsg()); reportErr != nil {
			r.logger.Error("Error reporting success, will report as error to workflow",
				zap.String("workflowID", workflowID),
				zap.String("runID", runID),
				zap.Error(reportErr))

			// Extract executionID from resultMessage metadata
			executionID := ""
			if resultMessage.Metadata != nil {
				executionID = resultMessage.Metadata["execution_id"]
			}

			// Report the failure as an error to Temporal so the workflow knows about it
			// Use longer timeout (30s) to match ReportError's retry logic
			errorCtx, errorCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer errorCancel()

			if errorReportErr := r.client.Messages.ReportError(errorCtx, executionID, workflowID, runID, correlationID, reportErr, msg.GetNATSMsg()); errorReportErr != nil {
				// Critical: If we can't report the error, log it extensively
				r.logger.Error("CRITICAL: Failed to report error to workflow after success report failed - workflow may hang",
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.String("executionID", executionID),
					zap.String("originalError", reportErr.Error()),
					zap.Error(errorReportErr))

				// Try one more time with a fresh context
				time.Sleep(2 * time.Second)
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if retryErr := r.client.Messages.ReportError(retryCtx, executionID, workflowID, runID, correlationID, reportErr, msg.GetNATSMsg()); retryErr != nil {
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
