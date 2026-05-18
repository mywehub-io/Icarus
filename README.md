# Icarus - NATS JetStream SDK for Go

A clean, future-proof Go SDK for messaging over NATS JetStream with idiomatic patterns, comprehensive error handling, and built-in support for advanced JetStream messaging patterns.

## Features

- **JetStream-Only**: Exclusively uses NATS JetStream for all messaging operations with persistence and advanced features
- **Clean Architecture**: Well-organized package structure with clear separation of concerns
- **Idiomatic Go**: Follows Go best practices and conventions
- **Context Support**: All operations support `context.Context` for cancellation and timeout control
- **Structured Messages**: Rich message format with workflow, node, payload, and output information
- **Central Client**: Single client provides access to all JetStream services with automatic initialization
- **JetStream Messaging Patterns**:
  - Message Publishing (JetStream-backed persistence)
  - Pull-based consumers (batch message processing)
  - Concurrent message processing with worker pools (Runner)
- **Runner Framework**: Built-in concurrent message processing with configurable worker pools, batch processing, and automatic success/error callback reporting; optional `ProcessFailureObserver` after every successful `ReportError`
- **Distributed Tracing**: Integrated OpenTelemetry tracing support with Jaeger and OTLP exporters for observability
- **Callback Reporting**: Automatic success and error reporting to result streams with proper message acknowledgment
- **Robust Error Handling**: SDK-specific errors with proper error wrapping
- **Connection Management**: Automatic reconnection and connection health monitoring
- **Full JetStream Integration**: Automatic JetStream context initialization and advanced operations
- **Structured Logging**: Built-in zap logger integration with configurable log levels
- **Kubernetes-Aware Concurrency Control**: Production-ready goroutine limiting with automatic CPU quota detection, circuit breaker protection, and comprehensive observability
- **Future-Proof**: Designed for easy extension with growing process management capabilities

## Installation

```bash
go get github.com/wehubfusion/Icarus
```

## Quick Start

### 1. Connect to NATS

```go
import (
    "context"
    "time"

    "github.com/wehubfusion/Icarus/pkg/client"
    "github.com/wehubfusion/Icarus/pkg/message"
    "github.com/wehubfusion/Icarus/pkg/runner"
    "go.uber.org/zap"
)

// Create and connect client
c := client.NewClient("nats://localhost:4222")
ctx := context.Background()

// Create logger (optional, defaults will be used if not set)
logger, _ := zap.NewProduction()
c.SetLogger(logger)

if err := c.Connect(ctx); err != nil {
    log.Fatalf("Failed to connect: %v", err)
}
defer c.Close()
```

## Schema Engine (JSON / CSV / HL7)

This repo also includes a schema engine that can parse/validate data against:

- **JSON schemas** (validation + defaults + structuring)
- **CSV schemas** (typed column schemas; input is JSON array of rows)
- **HL7 v2.x schemas** (validation only; results include `Errors`, `Warnings`, `Infos` based on mode)

See:

- `pkg/schema/README.md` for the unified API
- `pkg/schema/hl7/README.md` for HL7-specific behavior
- `examples/schema-engine/` for a runnable example

### 2. Publish Messages

```go
// Create a message
msg := message.NewWorkflowMessage("workflow-123", "run-456").
    WithPayload("api-server", "Hello, NATS!", "msg-123").
    WithMetadata("priority", "high")

// Publish using the client's Messages service (automatically initialized)
if err := c.Messages.Publish(ctx, "events.user.created", msg); err != nil {
    log.Printf("Failed to publish: %v", err)
}
```

### 3. Pull-Based Message Consumption

```go
// Pull messages from a JetStream consumer
messages, err := c.Messages.PullMessages(ctx, "EVENTS", "my-consumer", 10)
if err != nil {
    log.Printf("Failed to pull messages: %v", err)
    return
}

for _, msg := range messages {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.GetInlineData()
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)
    msg.Ack() // Acknowledge message processing
}
```

### 4. Concurrent Message Processing (Runner)

```go
// Define a message processor that implements the Processor interface
type MyProcessor struct{}

func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.GetInlineData()
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)

    // Create result message with processing outcome
    resultMessage := message.NewMessage().
        WithPayload("processor", "Successfully processed", "result-ref")

    // Copy workflow information for callback reporting
    if msg.Workflow != nil {
        resultMessage.Workflow = msg.Workflow
    }

    return *resultMessage, nil
}

// Create and configure runner with tracing
processor := &MyProcessor{}
tracingConfig := tracing.JaegerConfig("my-service")

cfg := runner.DefaultConfig()
cfg.WorkerCount = 10 // or rely on ICARUS_RUNNER_WORKERS / ICARUS_RUNNER_WORKER_MULTIPLIER

runner, err := runner.NewRunner(
    c,                // client
    processor,        // processor implementation
    "TASKS",          // stream name
    "task-consumer",  // consumer name
    10,               // batch size
    5*time.Minute,    // process timeout
    logger,           // zap logger
    &tracingConfig,   // tracing configuration (optional)
    &cfg,             // worker config (nil uses defaults/env)
)
if err != nil {
    log.Fatal(err)
}
defer runner.Close() // Clean up tracing resources

// Start processing (blocks until context cancelled)
if err := runner.Run(ctx); err != nil {
    log.Printf("Runner error: %v", err)
}
```

The Runner provides:
- Concurrent message processing with a bounded worker pool
- Batch message pulling from JetStream consumers
- Automatic success/error reporting to the "result" subject
- Built-in OpenTelemetry tracing support
- Graceful shutdown on context cancellation
- Middleware support for cross-cutting concerns (e.g., blob resolution)

## Client Architecture

The `Client` struct provides a central hub for all NATS operations with automatic service initialization:

```go
type Client struct {
    conn   *natsclient.Conn
    js     natsclient.JetStreamContext
    config *nats.ConnectionConfig
    logger *zap.Logger

    // Messages provides access to all JetStream messaging operations including
    // publish, pull-based consumers, and callback reporting
    Messages *message.MessageService

    // Reserved for future expansion
    // Processes *process.ProcessService
}
```

When `Connect()` is called, it:

- Establishes NATS connection
- Initializes JetStream context (if available)
- Automatically creates and assigns the `MessageService`

### Direct Service Access

Users can now access messaging operations directly through the client:

```go
c := client.NewClient("nats://localhost:4222")

// Configure logger (optional - defaults will be used if not set)
logger, _ := zap.NewProduction()
c.SetLogger(logger)

c.Connect(ctx)
// c.Messages is ready to use immediately
c.Messages.Publish(ctx, "subject", msg)
```

### JetStream Context Access

Direct access to JetStream for advanced operations:

```go
js := c.JetStream()
if js != nil {
    // Create streams, consumers, etc.
    js.AddStream(&nats.StreamConfig{
        Name:     "EVENTS",
        Subjects: []string{"events.>"},
    })
}
```

## Runner Framework

The Runner provides a powerful concurrent message processing framework that automatically handles batch processing, bounded worker-pool concurrency, success/error callbacks, and distributed tracing.

### Runner Architecture

The Runner consists of the following components:

- **Processor Interface**: Defines how individual messages are processed
- **Worker Pool**: Governs how many messages are processed in parallel (config/env driven)
- **Batch Puller**: Pulls messages in configurable batches from JetStream consumers
- **Callback Reporter**: Automatically reports success/error results to the "result" subject
- **Tracing Integration**: Built-in OpenTelemetry tracing with span propagation
- **Graceful Shutdown**: Handles context cancellation and proper cleanup

### Processor Interface

Implement the `Processor` interface to define your message processing logic:

```go
type Processor interface {
    Process(ctx context.Context, msg *message.Message) (message.Message, error)
}
```

The `Process` method should:
- Return a result `Message` containing processing outcomes
- Copy workflow information from input to result message for callback reporting
- Handle errors appropriately (they'll be automatically reported)

### Creating a Runner

```go
// Implement the Processor interface
type MyProcessor struct{}

func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    // Your processing logic here

    // Create result message
    result := message.NewMessage().
        WithPayload("processor", "Processing complete", "result-ref")

    // Copy workflow info for callback reporting
    if msg.Workflow != nil {
        result.Workflow = msg.Workflow
    }

    return *result, nil
}

// Configure tracing (optional)
tracingConfig := tracing.JaegerConfig("my-service")

cfg := runner.DefaultConfig()
cfg.WorkerCount = 5 // or rely on env: ICARUS_RUNNER_WORKERS / _WORKER_MULTIPLIER

// Create runner. Optional: append runner.WithProcessFailureObserver(obs) as extra arguments.
runner, err := runner.NewRunner(
    client,           // Connected Icarus client
    &MyProcessor{},   // Processor implementation
    "TASKS",          // Stream name
    "task-consumer",  // Consumer name
    10,               // Batch size (messages per pull)
    5*time.Minute,    // Processing timeout per message
    logger,           // Zap logger
    &tracingConfig,   // Tracing configuration (nil to disable)
    &cfg,             // Worker config (nil uses defaults/env)
)
if err != nil {
    log.Fatal(err)
}
defer runner.Close() // Cleanup tracing resources

// Start processing (blocks until context cancelled)
if err := runner.Run(ctx); err != nil {
    log.Printf("Runner stopped: %v", err)
}
```

### Runner Configuration Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `*client.Client` | Connected Icarus client |
| `processor` | `Processor` | Message processing implementation |
| `stream` | `string` | JetStream stream name to consume from |
| `consumer` | `string` | Durable consumer name |
| `batchSize` | `int` | Messages to pull per batch (1-1000) |
| `numWorkers` | `int` | Concurrent worker goroutines (1-100) |
| `processTimeout` | `time.Duration` | Max processing time per message |
| `logger` | `*zap.Logger` | Structured logger (cannot be nil) |
| `tracingConfig` | `*tracing.TracingConfig` | Tracing setup (nil disables tracing) |

### Configuring MaxDeliver (Retry Limit)

Control how many times messages are retried before being considered failed. This prevents infinite retry loops for permanent failures while allowing transient failures to recover.

**Default**: 5 retries (2.5 minutes with 30s AckWait)

```go
// Configure MaxDeliver in ConnectionConfig before Connect()
config := nats.DefaultConnectionConfig("nats://localhost:4222")
config.MaxDeliver = 20  // 20 retries = 10 minutes total (with 30s AckWait)

c := client.NewClientWithConfig(config)
if err := c.Connect(ctx); err != nil {
    logger.Fatal("Failed to connect", zap.Error(err))
}

// Or use one of these common configurations:
config.MaxDeliver = 5    // Default: 2.5 minutes (good for fast-moving systems)
config.MaxDeliver = 20   // 10 minutes (handles most incidents)
config.MaxDeliver = 100  // 50 minutes (very conservative)
config.MaxDeliver = -1   // Unlimited retries (not recommended)
```

**Calculation**: `Total Retry Time = MaxDeliver × AckWait`

- With default AckWait of 30 seconds:
  - MaxDeliver: 5 → 2.5 minutes
  - MaxDeliver: 20 → 10 minutes
  - MaxDeliver: 100 → 50 minutes

**When to adjust**:

- **Increase** if your services have longer MTTR (Mean Time To Recovery)
- **Decrease** for fast-moving systems where old messages become stale
- Consider your typical incident response time

### Automatic Callback Reporting

The Runner automatically reports processing results:

**Success Callbacks**: Published to the "result" subject when processing succeeds
**Error Callbacks**: Published to the "result" subject when processing fails
**Message Acknowledgment**: Source messages are properly ack'd/nack'd based on processing outcome

After a successful error-result publish, **INFO** logs `JetStream source message disposition after error result publish` with `jetstream_deliver_count` and `jetstream_source_ack_action` (`nak_transient_redelivery` vs `ack_permanent_suppress_redelivery`) so you can correlate retries with redelivery. Early exits (missing `execution_id` or workflow context) and failed error-result publishes still **NAK** with `jetstream_source_ack_action` values `nak_missing_execution_id`, `nak_missing_workflow_context`, or `nak_error_result_publish_failed`. Successful publishes log `JetStream source message ack after successful result publish` (including `jetstream_deliver_count`) before the source `Ack`. During pull, malformed messages now log `Dropping malformed pulled message; NAK for redelivery` with `stream`, `consumer`, `subject`, and `jetstream_deliver_count` so decode failures are visible in race investigations. On processor failures, the runner now emits explicit timeout/cancel diagnostics: `Process timeout exceeded for message (processCtx deadline exceeded...)` for per-message timeout hits, and `Process cancelled by parent context (runner shutting down?)` when the parent run context is canceled.

### Process failure observer (optional)

After a **successful** publish via `ReportError`, the runner invokes a `ProcessFailureObserver` you register with `runner.WithProcessFailureObserver`, for **every** failed `processErr` (including errors `ReportError` classifies as transient for JetStream retry semantics). Observers should be **idempotent** or tolerate duplicates: the same message may fail and redeliver, triggering `ReportError` and the observer again.

#### Argus `node.ended` observer (`pkg/runner/argus`)

For services that report workflow node completion to Argus, `github.com/wehubfusion/Icarus/pkg/runner/argus` provides `NewProcessFailureObserver(emitter argusemitter.NodeEndEmitter, logger *zap.Logger) runner.ProcessFailureObserver`. After `ReportError`, it emits Argus `node.ended` for failed processor runs. When message metadata includes `embed_failed_node_id` and `embed_root_cause` (set by trigger embedded-failure handling), the observer skips re-emitting the parent, the failing embedded node, and downstream embedded nodes that never ran; otherwise it emits the parent and embedded nodes as failed using the process error text. If emission is skipped because required identifiers are missing, it now logs WARN lines (`node.ended NOT emitted`) for missing `client_id`, `workflow_id/run_id`, or parent `node_id` so stuck-running nodes can be diagnosed from logs.

```go
import (
    argusemitter "github.com/wehubfusion/Argus/pkg/emitter"
    "github.com/wehubfusion/Icarus/pkg/runner"
    runnerargus "github.com/wehubfusion/Icarus/pkg/runner/argus"
    "go.uber.org/zap"
)

// obs := runnerargus.NewProcessFailureObserver(nodeEndEmitter, logger)
// runner.NewRunner(..., runner.WithProcessFailureObserver(obs))
```

### Stream and Consumer Setup

Ensure your streams and consumers are configured for the Runner:

```go
js := client.JetStream()

// Create stream
_, err := js.AddStream(&nats.StreamConfig{
    Name:        "TASKS",
    Description: "Task messages for processing",
    Subjects:    []string{"tasks.>"},
    Storage:     nats.FileStorage,
    Replicas:    1,
    MaxAge:      24 * time.Hour,
})

// Create consumer
_, err = js.AddConsumer("TASKS", &nats.ConsumerConfig{
    Durable:       "task-consumer",
    Description:   "Consumer for task processing",
    AckPolicy:     nats.AckExplicitPolicy,
    MaxDeliver:    3,               // Retry failed messages
    AckWait:       5 * time.Minute, // Wait for acknowledgment
    MaxAckPending: 100,             // Allow pending messages
})
```

### Middleware Support

The Runner supports middleware for cross-cutting concerns. Middleware wraps the processor and can modify messages before processing or handle responses after processing.

```go
// Apply blob resolving middleware to transparently download large payloads
runner.WithMiddleware(
    icarusRunner.BlobResolvingMiddleware(blobClient, logger),
)

// Chain multiple middlewares
runner.WithMiddleware(middleware1).
       WithMiddleware(middleware2).
       WithMiddleware(middleware3)
```

**Built-in Middleware:**

- **BlobResolvingMiddleware**: Transparently downloads large payloads from Azure Blob Storage when messages contain a `BlobReference`. Plugins receive inline data without knowing about blob storage.

**Creating Custom Middleware:**

```go
func MyMiddleware(logger *zap.Logger) runner.Middleware {
    return func(next runner.Processor) runner.Processor {
        return runner.ProcessorFunc(func(ctx context.Context, msg *message.Message) (message.Message, error) {
            // Pre-processing logic
            logger.Info("Processing message", zap.String("id", msg.CorrelationID))
            
            // Call next processor in chain
            result, err := next.Process(ctx, msg)
            
            // Post-processing logic
            if err != nil {
                logger.Error("Processing failed", zap.Error(err))
            }
            
            return result, err
        })
    }
}
```

### BlobReference for Large Payloads

Messages can contain a `BlobReference` in the payload for large data that exceeds inline limits (>1.5MB):

```go
// Payload with blob reference (set by Zeus when publishing large inputs)
type Payload struct {
    InlineData    *string        `json:"inlineData,omitempty"`   // Inline data (for small payloads) - nullable
    BlobReference *BlobReference `json:"blobReference,omitempty"` // Reference to blob storage
    // Execution context fields (automatically populated by WithPayload)
    ExecutionID string `json:"execution_id"` // Unique identifier for this execution
    WorkflowID  string `json:"workflow_id"`  // Workflow identifier
    RunID       string `json:"run_id"`       // Workflow run identifier
    NodeID      string `json:"node_id"`      // Node identifier
}

// Helper methods:
// GetData() returns the inline data as string, or empty string if nil
// HasData() returns true if inline data is present

type BlobReference struct {
    URL       string `json:"url"`       // Direct blob URL (for logging)
    SizeBytes int    `json:"sizeBytes"` // Original data size in bytes
}
```

When `BlobResolvingMiddleware` is applied:
1. It checks if the message has a `BlobReference`
2. If yes, downloads the data from Azure Blob Storage using the shared-key client
3. Populates `Payload.InlineData` with the downloaded content
4. Clears the `BlobReference`
5. Passes the message to the processor with inline data

**Plugins never need to know about blob storage** - they always receive inline data.

### Tracing Integration

The Runner includes comprehensive tracing:

- **Automatic Span Creation**: Spans for message pulling, processing, and reporting
- **Context Propagation**: Trace context flows through the entire processing pipeline
- **Rich Attributes**: Workflow IDs, run IDs, worker information, timing data
- **Error Recording**: Failed processing is recorded in spans
- **Cleanup**: Proper shutdown of tracing resources

## Distributed Tracing

Icarus includes built-in OpenTelemetry tracing support for observability and debugging of message processing pipelines.

### Tracing Setup

Configure tracing when creating a Runner or set it up manually:

```go
// Option 1: Jaeger configuration (recommended for development)
tracingConfig := runner.JaegerTracingConfig("my-service-name")
tracingConfig.SampleRatio = 1.0 // Sample all traces in development

// Option 2: Generic OTLP configuration
tracingConfig := runner.TracingConfig{
    ServiceName:    "my-service",
    ServiceVersion: "1.0.0",
    Environment:    "production",
    OTLPEndpoint:   "otel-collector.company.com:4318", // Host:port only
    SampleRatio:    0.1, // Sample 10% of traces in production
}
```

### Tracing with Runner

The Runner automatically integrates tracing:

```go
runner, err := runner.NewRunner(
    client,
    processor,
    "TASKS",
    "task-consumer",
    10,             // batch size
    5*time.Minute,  // timeout
    logger,
    &tracingConfig, // Enable tracing
    nil,            // worker config (nil uses defaults/env)
)
if err != nil {
    log.Fatal(err)
}
defer runner.Close() // Automatically shuts down tracing
```

### Tracing Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `ServiceName` | Service identifier for traces | Required |
| `ServiceVersion` | Service version | "1.0.0" |
| `Environment` | Deployment environment | "development" |
| `OTLPEndpoint` | OTLP exporter endpoint (host:port) | "127.0.0.1:4318" |
| `SampleRatio` | Trace sampling ratio (0.0-1.0) | 1.0 (development), 0.1 (production) |

### What Gets Traced

The Runner creates spans for:

- **Message Pulling**: Batch message retrieval from JetStream
- **Message Processing**: Individual message processing by workers
- **Success Reporting**: Callback publication for successful processing
- **Error Reporting**: Callback publication for failed processing

### Span Attributes

Traces include rich metadata:

```go
// Workflow information
workflow.id = "workflow-123"
workflow.run_id = "run-456"

// Worker information
worker.id = 2

// Stream/consumer details
stream = "TASKS"
consumer = "task-consumer"

// Timing information
processing.duration_ms = 1500

// Message details
message.node.id = "process-payment"
// Source field removed - plugin type available in Metadata["plugin_type"]
message.created_at = "2024-01-01T12:00:00Z"
```

### Jaeger Integration

For local development with Jaeger:

```bash
# Start Jaeger
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest

# Configure Icarus to send traces to Jaeger
tracingConfig := tracing.JaegerConfig("my-service")
tracingConfig.OTLPEndpoint = "localhost:14268" // Jaeger OTLP endpoint
```

Then visit `http://localhost:16686` to view traces.

### Production Setup

For production environments:

```go
tracingConfig := tracing.TracingConfig{
    ServiceName:    "order-processor",
    ServiceVersion: "2.1.0",
    Environment:    "production",
    OTLPEndpoint:   "otel-collector.company.com:4318",
    SampleRatio:    0.1, // Sample 10% of traces
}
```

## Callback Reporting

Icarus provides automatic callback reporting for workflow orchestration, allowing downstream systems to be notified of processing results.

### Callback Architecture

The callback system consists of:

- **Result Stream**: A dedicated "result" subject for all callback messages
- **Success Callbacks**: Published when message processing succeeds
- **Error Callbacks**: Published when message processing fails
- **Message Acknowledgment**: Proper ack/nak handling based on processing outcome

### Automatic Callbacks with Runner

The Runner automatically handles callback reporting:

```go
// When processing succeeds, a success callback is published to "result"
func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    // Processing logic...

    result := message.NewMessage().
        WithPayload("processor", "Success", "result-ref")

    // Copy workflow info for callback correlation
    if msg.Workflow != nil {
        result.Workflow = msg.Workflow
    }

    return *result, nil // Triggers success callback
}

// When processing fails, an error callback is published to "result"
func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    // Processing logic...

    if someErrorCondition {
        return message.Message{}, fmt.Errorf("processing failed") // Triggers error callback
    }

    return *result, nil
}
```

### Manual Callback Reporting

You can also report callbacks manually using the MessageService:

```go
// Report success
successResult := message.NewWorkflowMessage(workflowID, runID).
    WithPayload("processor", "Task completed successfully", "success-ref")

err := client.Messages.ReportSuccess(ctx, *successResult, originalNATSMsg)
if err != nil {
    logger.Error("Failed to report success", zap.Error(err))
}

// Report error
err := client.Messages.ReportError(ctx, executionID, workflowID, runID, correlationID, fmt.Errorf("Processing failed: timeout"), originalNATSMsg)
if err != nil {
    logger.Error("Failed to report error", zap.Error(err))
}
```

### Callback Message Format

Success callbacks contain:

```json
{
  "workflow": {
    "workflowID": "workflow-123",
    "runID": "run-456"
  },
  "payload": {
    "source": "processor",
    "result": "Processing completed successfully",
  },
  "metadata": {
    "result_type": "success"
  },
  "createdAt": "2024-01-01T12:00:00Z",
  "updatedAt": "2024-01-01T12:00:00Z"
}
```

Error callbacks contain:

```json
{
  "workflow": {
    "workflowID": "workflow-123",
    "runID": "run-456"
  },
  "payload": {
    "source": "callback-service",
    "result": "Processing failed: connection timeout",
  },
  "metadata": {
    "result_type": "error",
    "correlation_id": "workflow-123-run-456"
  },
  "createdAt": "2024-01-01T12:00:00Z",
  "updatedAt": "2024-01-01T12:00:00Z"
}
```

### Setting Up Result Streams

Create a stream to capture callback messages:

```go
js := client.JetStream()

// Create RESULTS stream for callbacks
_, err := js.AddStream(&nats.StreamConfig{
    Name:        "RESULTS",
    Description: "Stream for success/error callback reporting",
    Subjects:    []string{"result"},
    Storage:     nats.FileStorage,
    Replicas:    1,
    MaxAge:      7 * 24 * time.Hour, // Keep results for 7 days
})
```

### Consuming Callbacks

Pull callbacks for monitoring or further processing:

```go
// Pull callback messages from the RESULTS stream
messages, err := client.Messages.PullMessages(ctx, "RESULTS", "callback-consumer", 10)
if err != nil {
    logger.Error("Failed to pull callback messages", zap.Error(err))
    return
}

for _, msg := range messages {
    resultType := msg.Metadata["result_type"]

    if resultType == "success" {
        logger.Info("Processing succeeded",
            zap.String("workflow_id", msg.Workflow.WorkflowID),
            zap.String("run_id", msg.Workflow.RunID))
    } else if resultType == "error" {
        logger.Error("Processing failed",
            zap.String("workflow_id", msg.Workflow.WorkflowID),
            zap.String("run_id", msg.Workflow.RunID),
            zap.String("error", msg.Payload.GetInlineData()))
    }

    msg.Ack()
}
```

### Message Acknowledgment

The callback system handles message acknowledgment automatically:

- **Success Callbacks**: Original message is acknowledged after successful callback publication
- **Error Callbacks**: Original message is negatively acknowledged after callback publication
- **Retry Logic**: Callbacks are published with retry logic for reliability

## Concurrency Control

Icarus provides production-ready, Kubernetes-aware concurrency control that prevents unlimited goroutine creation, provides observability, and scales properly in containerized environments.

### Features

- **Semaphore-Based Limiting**: Prevents unlimited goroutine creation with bounded concurrency
- **Circuit Breaker**: Automatic failure detection and graceful degradation under load
- **Kubernetes-Aware**: Automatically detects and respects container CPU limits using `automaxprocs`
- **Configurable**: Environment variables for fine-tuning based on workload characteristics
- **Observable**: Comprehensive metrics logged every 30 seconds
- **Thread-Safe**: All operations use atomic counters and proper synchronization

### Quick Start

```go
import (
    "github.com/wehubfusion/Icarus/pkg/runner"
)

func main() {
    // Configure runner workers (env overrides are applied automatically)
    cfg := runner.DefaultConfig() // respects ICARUS_RUNNER_WORKERS / _WORKER_MULTIPLIER

    runnerInstance, err := runner.NewRunner(
        client,
        processor,
        "TASKS",
        "consumer",
        10,           // batch size
        5*time.Minute, // process timeout
        logger,
        nil,   // tracing config
        &cfg,  // worker config (nil uses defaults/env)
    )
    if err != nil {
        logger.Fatal("failed to create runner", zap.Error(err))
    }
    defer runnerInstance.Close()
}
```

### Configuration

Concurrency is configured through environment variables with intelligent defaults:

| Variable | Description | Default |
|----------|-------------|---------|
| `ICARUS_MAX_CONCURRENT` | Explicit max concurrent goroutines | Auto-detected |
| `ICARUS_CONCURRENCY_MULTIPLIER` | CPU × multiplier for max concurrent | K8s: 2, Bare: 4 |
| `ICARUS_RUNNER_WORKERS` | Number of NATS consumer workers | GOMAXPROCS |
| `ICARUS_RUNNER_WORKER_MULTIPLIER` | CPU × multiplier for runner workers | 1× CPU when unset |
| `ICARUS_EMBEDDED_WORKERS` | Worker count for embedded runtime pool | GOMAXPROCS |
| `ICARUS_EMBEDDED_WORKER_MULTIPLIER` | CPU × multiplier for embedded workers | 1× CPU when unset |
| `ICARUS_PROCESSOR_MODE` | `concurrent` or `sequential` processing | `concurrent` |
| `ICARUS_ITERATOR_MODE` | `parallel` or `sequential` array iteration | `sequential` |

**Configuration Priority**: Explicit env vars > auto-detection > defaults

**Environment Detection**:
- **Kubernetes**: Detected via `KUBERNETES_SERVICE_HOST` environment variable
- **CPU Limits**: Automatically detected using `runtime.GOMAXPROCS(0)` which respects cgroup limits

### Usage with Processor

The embedded runtime now uses an internal worker pool instead of the shared limiter. Configure workers via code or environment:

```go
import (
    "github.com/wehubfusion/Icarus/pkg/embedded"
)

// Configure worker pool (or rely on ICARUS_EMBEDDED_WORKERS / ICARUS_EMBEDDED_WORKER_MULTIPLIER)
cfg := embedded.DefaultProcessorConfig()
cfg.WorkerPool.NumWorkers = 16

processor := embedded.NewEmbeddedProcessor(registry, cfg)

// Process embedded nodes with the configured worker pool
results, events, err := processor.ProcessEmbeddedNodes(ctx, parentOutput, unit, priorUnitOutputs)
```

`events` is a lightweight flat-map subset of embedded output intended for event-trigger evaluation when full results are blob-backed.

**Mid-flow iteration concurrency**: When an embedded node produces an array and a downstream node iterates over it, single-level iterations can run concurrently. Set `cfg.NestedIteration.IterationConcurrency` > 0 (e.g. `cfg.NestedIteration.IterationConcurrency = 2`) to enable. Nested iterations always run sequentially for correctness.

### Embedded Runtime Package Layout

The embedded processor has been modularized into focused runtime packages to simplify maintenance:

- `pkg/embedded/runtime`: processor orchestration, executor registry, and public APIs (re-exported through `pkg/embedded` for backward compatibility).
- `pkg/embedded/runtime/output`: shared `StandardOutput` types, wrappers, and registries.
- `pkg/embedded/runtime/storage`: smart storage with consumer-aware cleanup and iteration context helpers.
- `pkg/embedded/runtime/mapping`: field mapping engine and path utilities bridging into `pathutil`.
- `pkg/embedded/runtime/errors`: standardized error categorization and retry hints reused by output wrappers.
- `pkg/embedded/runtime/logging`: lightweight logger interface and structured field helpers used across runtime components.
- **Embedded hard failures**: when an embedded node fails without a downstream error listener, the subflow emits `node.ended` for that node and returns `*runtime.EmbeddedNodeFailureError` (wraps `FailedNodeID`, `FailedNodeLabel`, and `Cause`). **Observation**: Argus `node.started` for embedded nodes is emitted only when that node is about to execute (resolved input), not for all embedded nodes in advance—so downstream embedded nodes that never run do not appear in observation stores and are not coerced to empty-error `failed` on `run.ended`.
- `pkg/embedded/processors/registry`: consolidated helper that wires up all built-in executors (formerly `pkg/embedded/all`).

Import paths that previously referenced `github.com/wehubfusion/Icarus/pkg/embedded` continue to work unchanged because the top-level package re-exports the runtime APIs.

### Observability

The runner logs concurrency metrics every 30 seconds:

```json
{
  "level": "info",
  "msg": "Concurrency metrics",
  "active_goroutines": 45,
  "peak_concurrent": 78,
  "total_acquired": 1523,
  "total_released": 1478,
  "avg_wait_time": "15ms",
  "circuit_breaker": "closed"
}
```

**Key Metrics**:
- **active_goroutines**: Current concurrent operations
- **peak_concurrent**: Maximum concurrency reached since start
- **total_acquired/released**: Cumulative slot acquisitions
- **avg_wait_time**: Average time waiting for a concurrency slot
- **circuit_breaker**: Protection state (closed/open/half-open)

### Circuit Breaker

Protects against cascade failures during overload:

- **Threshold**: Opens after 100 consecutive failures
- **Reset Timeout**: 30 seconds before attempting recovery
- **Half-Open State**: Tests recovery with limited traffic
- **Automatic Recovery**: Closes after 5 consecutive successes

When the circuit breaker is open, new operations fail immediately instead of overwhelming the system.

### Tuning Guidelines

#### CPU-Bound Workloads

Increase concurrency and enable parallelism:

```bash
export ICARUS_CONCURRENCY_MULTIPLIER=4
export ICARUS_PROCESSOR_MODE=concurrent
export ICARUS_ITERATOR_MODE=parallel
```

Monitor CPU throttling and adjust as needed.

#### I/O-Bound Workloads

Use higher concurrency with conservative CPU allocation:

```bash
export ICARUS_CONCURRENCY_MULTIPLIER=8
export ICARUS_RUNNER_WORKERS=12
```

Higher concurrency allows better I/O overlap.

#### Memory-Constrained Environments

Reduce concurrency and use sequential processing:

```bash
export ICARUS_CONCURRENCY_MULTIPLIER=1
export ICARUS_PROCESSOR_MODE=sequential
export ICARUS_ITERATOR_MODE=sequential
export ICARUS_RUNNER_WORKERS=2
```

### Kubernetes Deployment

See the `k8s/` directory for complete Kubernetes manifests including:
- ConfigMap with concurrency settings
- Deployment with proper resource limits
- HorizontalPodAutoscaler for automatic scaling
- Service for health and metrics endpoints

```bash
# Deploy with default settings
kubectl apply -f k8s/

# Monitor concurrency metrics
kubectl logs -f -l app=icarus-processor | grep "Concurrency metrics"

# Check autoscaling
kubectl get hpa icarus-processor-hpa
```

**Critical for Kubernetes**: Set CPU limits in your deployment:

```yaml
resources:
  limits:
    cpu: "2000m"  # automaxprocs uses this to set GOMAXPROCS
    memory: "4Gi"
```

### Best Practices

1. **Initialize CPUs**: Call `concurrency.InitializeForKubernetes()` at the start of `main()` if you still use the concurrency package elsewhere
2. **Configure Workers**: Set runner workers via `Runner.Config` or `ICARUS_RUNNER_WORKERS` / `_WORKER_MULTIPLIER`
3. **Monitor Throughput**: Watch processing latency and adjust worker counts or batch size
4. **Start Conservative**: Begin with defaults, scale up based on metrics
5. **Set Resource Limits**: In Kubernetes, always set both requests and limits
6. **Test Scaling**: Verify behavior under load before production deployment

### Troubleshooting

**High Wait Times**: Increase `ICARUS_CONCURRENCY_MULTIPLIER` or scale horizontally

**Circuit Breaker Opens**: Check downstream services and reduce load temporarily

**CPU Throttling**: Increase CPU limits in Kubernetes or reduce concurrency multiplier

**Memory Pressure**: Reduce max concurrent operations and use sequential modes

See `k8s/README.md` for detailed troubleshooting guides.

## Advanced Usage

### Custom Connection Configuration

```go
import "github.com/wehubfusion/Icarus/internal/nats"

config := &nats.ConnectionConfig{
    URL:           "nats://localhost:4222",
    Name:          "my-service",
    MaxReconnects: 10,
    ReconnectWait: 2 * time.Second,
    Timeout:       5 * time.Second,
    Username:      "user",
    Password:      "pass",
}

c := client.NewClientWithConfig(config)
```


### Pull-Based Consumers (JetStream)

```go
// Pull messages from a JetStream consumer
messages, err := c.Messages.PullMessages(ctx, "mystream", "myconsumer", 10)
if err != nil {
    log.Printf("Failed to pull messages: %v", err)
}

for _, msg := range messages {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.GetInlineData()
    }
    fmt.Printf("Pulled message: %s\n", content)
}
```

This method:

- Connects to a JetStream pull consumer
- Fetches messages in batches
- Automatically acknowledges successfully deserialized messages
- Respects context cancellation/timeout

### Context-Based Cancellation

```go
// Create a context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

// All operations respect the context
if err := c.Messages.Publish(ctx, "events.test", msg); err != nil {
    log.Printf("Publish timed out or was cancelled: %v", err)
}
```

## Project Structure

```text
Icarus/
├── go.mod
├── go.sum
├── README.md
├── internal/                 # Private helpers
│   └── nats/                 # NATS connection utilities
│       └── connection.go     # Connection management, reconnection logic
├── pkg/                      # Public API
│   ├── client/               # Central NATS client
│   │   └── client.go         # Client with connection management
│   ├── message/              # Message handling and JetStream operations
│   │   ├── message.go        # Message struct and serialization
│   │   ├── handler.go        # Handler types and utilities
│   │   └── service.go        # Message service with pub/sub operations
│   ├── runner/               # Concurrent message processing
│   │   └── runner.go         # Worker pool-based message processing
│   ├── tracing/              # Distributed tracing utilities
│   │   └── tracing.go        # OpenTelemetry tracing setup
│   ├── errors/               # SDK-specific errors
│   │   └── errors.go         # Error types and utilities
│   └── process/              # Process management utilities
│       └── strings/          # String processing utilities
│           └── strings.go    # String manipulation functions
├── examples/                 # Usage examples
│   ├── message/              # JetStream messaging patterns
│   │   └── main.go           # All messaging patterns with the client
│   ├── runner/               # Concurrent processing examples
│   │   └── main.go           # Runner usage examples
│   ├── runner-with-tracing/  # Runner with tracing examples
│   │   └── main.go           # Runner with OpenTelemetry tracing
│   └── process/              # Process management examples
│       └── strings/          # String processing utilities
│           └── main.go       # String processing demo
└── tests/                    # Test suite
    ├── client_test.go        # Client functionality tests
    ├── message_test.go       # Message and service tests
    ├── message_service_test.go # Message service tests
    ├── runner_test.go        # Runner functionality tests
    ├── tracing_test.go       # Tracing functionality tests
    ├── nats_mock_test.go     # Mock implementations for testing
    ├── integration_test.go   # Integration tests
    └── error_test.go         # Error handling tests
```

## Error Handling

The SDK provides structured error handling with typed errors:

```go
import (
    sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
    "errors"
)

err := c.Messages.Publish(ctx, "test", msg)
if err != nil {
    // Check for specific error types using errors.As
    var appErr *sdkerrors.AppError
    if errors.As(err, &appErr) {
        switch appErr.Type {
        case sdkerrors.ValidationFailed:
            fmt.Println("Validation error:", appErr.Message)
        case sdkerrors.NotFound:
            fmt.Println("Resource not found:", appErr.Message)
        case sdkerrors.Internal:
            fmt.Println("Internal error:", appErr.Message)
        }
    }

    // Or check specific error codes
    if errors.As(err, &appErr) && appErr.Code == "NOT_CONNECTED" {
        fmt.Println("Not connected to NATS")
    }
}
```

## Message Format

Messages are strongly typed with a rich structure designed for workflow orchestration and complex messaging scenarios:

```go
type Message struct {
    // Workflow contains workflow execution information (optional)
    Workflow *Workflow `json:"workflow,omitempty"`

    // Node contains node information within a workflow (optional)
    Node *Node `json:"node,omitempty"`

    // Payload contains the message data (optional)
    Payload *Payload `json:"payload,omitempty"`

    // Output contains output destination information (optional)
    Output *Output `json:"output,omitempty"`

    // Metadata holds additional key-value pairs
    Metadata map[string]string `json:"metadata,omitempty"`

    // EmbeddedNodes contains child nodes to be executed within the parent node (optional)
    EmbeddedNodes []EmbeddedNode `json:"embeddedNodes,omitempty"`

    // Connection contains connection details for the parent node (optional)
    Connection *ConnectionDetails `json:"connection,omitempty"`

    // Schema contains schema details for the parent node (optional)
    Schema *SchemaDetails `json:"schema,omitempty"`

    // CreatedAt is the timestamp when the message was created
    CreatedAt string `json:"createdAt"`

    // UpdatedAt is the timestamp when the message was last updated
    UpdatedAt string `json:"updatedAt"`
}
```

### Message Components

- **Workflow**: Contains `WorkflowID` and `RunID` for tracking workflow executions
- **Node**: Contains `NodeID` and `Configuration` for workflow node information
- **Payload**: Contains `InlineData` for the message content (execution ID is stored in `Metadata["execution_id"]`, plugin type in `Metadata["plugin_type"]`)
- **Output**: Contains `DestinationType` for routing information
- **Metadata**: Flexible key-value pairs for additional information
- **EmbeddedNodes**: Array of child nodes to execute within the parent node (for unit processing)
- **Connection**: Connection details (ConnectionID, Type, Config) for database/API connections
- **Schema**: Schema details (SchemaID, Name, Fields) for data validation
- **Timestamps**: RFC3339 formatted creation and update timestamps

#### Unit Processing

Messages now support **unit processing** where a parent node can contain multiple embedded child nodes to be executed together. This enables atomic execution of related operations with proper field mapping between nodes.

```go
// Create a message with embedded nodes (unit processing)
embeddedNodes := []message.EmbeddedNode{
    {
        NodeID:         "child-node-1",
        PluginType:     "transform",
        Configuration:  []byte(`{"operation": "map"}`),
        ExecutionOrder: 1,
        FieldMappings: []message.FieldMapping{
            {
                SourceNodeID:         "parent",
                SourceEndpoint:       "/data/input",
                DestinationEndpoints: []string{"/input"},
                DataType:             "json",
            },
        },
    },
    {
        NodeID:         "child-node-2",
        PluginType:     "output",
        Configuration:  []byte(`{"destination": "database"}`),
        ExecutionOrder: 2,
    },
}

msg := message.NewWorkflowMessage("workflow-123", "run-456").
    WithNode("parent-node", nil).
    WithEmbeddedNodes(embeddedNodes).
    WithConnection(&message.ConnectionDetails{
        ConnectionID: "conn-789",
        Type:         "postgres",
        Config:       []byte(`{"host": "localhost", "port": 5432}`),
    }).
    WithSchema(&message.SchemaDetails{
        SchemaID: "schema-456",
        Name:     "users",
        Fields:   []byte(`[{"name": "id", "type": "int"}]`),
    })
```

**Key Features:**
- **ExecutionOrder**: Controls the sequence of embedded node execution
- **FieldMappings**: Maps data between parent and child nodes or between child nodes
- **Connection/Schema**: Shared across parent and embedded nodes for consistency
- **Backward Compatible**: Messages without embedded nodes work as before

### Creating Messages

```go
// Simple workflow message
msg := message.NewWorkflowMessage("workflow-123", "run-456").
    WithPayload("user-service", "User created", "user-789").
    WithMetadata("priority", "high")

// Message with node information
msg = message.NewMessage().
    WithNode("process-payment", map[string]interface{}{"amount": 100}).
    WithOutput("payment-gateway")
```

## StandardOutput Format

All nodes (parent and embedded) output a standardized structure with reserved namespaces for consistent error handling and event routing.

### Output Structure

```json
{
  "_meta": {
    "status": "success|failed|skipped",
    "node_id": "node-123",
    "plugin_type": "plugin-http",
    "execution_time_ms": 150,
    "timestamp": "2024-01-15T10:30:00Z"
  },
  "_events": {
    "success": true|null,
    "error": null|true
  },
  "_error": {
    "code": "ERROR_CODE",
    "message": "Error description",
    "retryable": true|false
  },
  "result": {
    "... actual plugin output ..."
  }
}
```

### Reserved Namespaces

- **`_meta`**: Execution metadata (status, timing, node information)
- **`_events`**: Event endpoints for routing (success/error events)
- **`_error`**: Detailed error information (only present on failure)
- **`result`**: Actual plugin output result (null on error)

### Error Events

Failed nodes emit error events that can trigger error handling nodes:

```go
// Failed node output
{
  "_events": {
    "error": true  // Error event fires!
  },
  "_error": {
    "code": "HTTP_TIMEOUT",
    "message": "Request timed out",
    "retryable": true
  }
}

// Error handler field mapping
{
  "sourceEndpoint": "_events/error",
  "isEventTrigger": true
}
```

**Key Feature**: Failed nodes store their outputs in the registry, enabling:
- Error event routing to error handlers
- Access to error details for intelligent recovery
- Rich observability through error metadata

See [Error Handling Guide](../Olympus/docs/ERROR_HANDLING.md) for comprehensive patterns.

## Embedded Node Processors

Icarus includes powerful embedded node processors that enable sophisticated data processing within workflow nodes.

### JSRunner - JavaScript Processor

The JSRunner processor executes JavaScript code with schema-aware capabilities, enabling flexible data transformation and validation.

**Key Features:**

- **Schema-Aware Processing**: Access input schemas directly in JavaScript for meta-programming
- **Strict by Default**: When `inputSchema` / `outputSchema` is provided, schema validation defaults to **strict**. Set `"strict_validation": false` explicitly to allow invalid data to pass without failing the node.
- **Raw Byte Output**: Always returns raw bytes for efficient processing
- **VM Pooling**: Reuses JavaScript VMs for performance
- **Configurable Security**: Multiple security levels (strict, standard, permissive)
- **Timeout Protection**: Configurable execution timeouts

**Basic Usage:**

```go
import "github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"

// Create executor
executor := jsrunner.NewExecutor()
defer executor.Close()

// Configure with schema
config := jsrunner.Config{
    Script: `
        // Access input and schema
        var requiredFields = Object.keys(schema.properties);
        
        // Validate and transform
        var output = {};
        for (var key in schema.properties) {
            var field = schema.properties[key];
            if (input[key] !== undefined) {
                output[key] = input[key];
            } else if (field.default !== undefined) {
                output[key] = field.default;
            }
        }
        
        JSON.stringify(output);
    `,
}

// Add schema if needed
schemaBytes, _ := json.Marshal(map[string]interface{}{
    "type": "OBJECT",
    "properties": map[string]interface{}{
        "name": map[string]interface{}{
            "type": "STRING",
            "required": true,
        },
        "status": map[string]interface{}{
            "type": "STRING",
            "default": "active",
        },
    },
})
config.SchemaDefinition = schemaBytes

// Execute
configBytes, _ := json.Marshal(config)
inputBytes, _ := json.Marshal(map[string]interface{}{"name": "John"})

result, err := executor.Execute(ctx, embedded.NodeConfig{
    Configuration: configBytes,
    Input:         inputBytes,
})

// Result is raw bytes: {"name":"John","status":"active"}
fmt.Println(string(result))
```

**JavaScript Context:**

Your JavaScript code has access to two global variables:

- **`input`**: The input data (always available)
- **`schema`**: The schema definition (when `schema_id` is provided and enriched)

**Output Format:**

JSRunner always returns raw bytes (not wrapped JSON):

| JavaScript Return | Output |
|------------------|--------|
| String | Raw string as bytes |
| Number | JSON-marshaled: `42` → `"42"` |
| Object | JSON-marshaled: `{a:1}` → `{"a":1}` |
| Array | JSON-marshaled: `[1,2]` → `[1,2]` |

**Schema Integration:**

When used in Elysium workflows, specify `schema_id` in the configuration. Elysium's enrichment framework automatically fetches the schema from Morpheus and injects it before execution:

```json
{
  "plugin_type": "plugin-js",
  "configuration": {
    "script": "// Your JS code with schema access",
    "schema_id": "user-schema"
  }
}
```

The enrichment is transparent - jsrunner receives the full schema definition and makes it available as the `schema` global variable.

**Common Use Cases:**

- Custom validation logic beyond schema rules
- Dynamic field mapping based on schema
- Conditional data transformation
- Applying business rules to data
- Schema-driven default value application

For detailed examples and API documentation, see `pkg/embedded/processors/jsrunner/README.md`.

### JSONOps Processor

The JSONOps processor provides schema-driven JSON data validation and transformation using Icarus schemas.

**Operations:**

- **parse**: Validate and structure incoming JSON data against a schema
- **produce**: Validate and encode JSON data to base64 output

**Strict Validation (default behavior):**

- `strict_validation` defaults to **true** for both `parse` and `produce`.
- When strict validation is enabled, any schema validation failure (type mismatch, required missing, invalid format, etc.) returns a node error.
- To opt into lenient behavior, set `"strict_validation": false` explicitly.

**Configuration:**

```go
import "github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"

// Parse operation - validate and structure incoming data
config := jsonops.Config{
    Action:   "parse",
    SchemaID:  "user-input-schema",
}

// Produce operation - validate and encode output
config := jsonops.Config{
    Action:   "produce",
    SchemaID:  "api-output-schema",
}
```

**Key Features:**

- Flat configuration structure (no nested params)
- Operation-specific smart defaults
- Schema-based validation using Icarus schemas
- Base64 encoding for produce operation
- Integration with Elysium schema enrichment

When executed in Elysium, the `schema_id` is automatically enriched with the full schema definition from Morpheus before processing.

For detailed documentation, see `pkg/embedded/processors/jsonops/README.md`.

### Error Processor (`plugin-error`)

The Error processor is a control-flow node that intentionally emits an error instead of a success payload.

**Configuration and inputs:**

- **label**: Display name for the node (from the Apollo node schema).
- **default_error_message**: Fallback error message used when no runtime message is provided.
- **message**: Optional input field populated via field mappings at runtime; when present and non-empty, it takes precedence over `default_error_message`.

When neither `message` nor `default_error_message` is set, the runtime uses a default of the form:  
`Error node "<label>" (plugin-error): no message was provided`, preferring the schema **label**, then the runtime node label, then the node id.

**Behavior:**

- The node always returns an error (no success data payload).
- When a downstream node listens to this node's `pluginError` section, the runtime exposes an error-only output:
  - `pluginError/error` = `true`
  - `pluginError/errorDescription` = resolved error message.
- A **listener** is any mapping from this node's `pluginError` section, either:
  - an **event mapping** (e.g. `/error` as an `EVENT` trigger), or
  - a **field mapping** (e.g. `/errorDescription` as a `FIELD` into a handler node).
- If at least one such mapping exists, the embedded runtime treats the error as **handled** and does not fail-fast; if no mappings from `pluginError` exist, the error bubbles up and causes the embedded unit / workflow run to fail, making this node useful for hard-stop guards as well as structured error flows.

**Success-path guarantee for `pluginError` consumers:**

Any node that maps from another node's `pluginError` section receives `error = false` and `errorDescription = ""` when that source node **succeeded**, regardless of whether the source is the parent unit, an embedded sibling in the same unit, or a node from a prior unit. The runtime synthesises these defaults at input-resolution time so that a downstream condition checking `error equals false` evaluates correctly without the source node having to include those keys in its own output payload.

## String Processing Utilities

The SDK includes comprehensive string processing utilities for workflow orchestration and data manipulation:

```go
import "github.com/wehubfusion/Icarus/pkg/process/strings"

// Basic operations
result := strings.Concatenate("-", "hello", "world", "test") // "hello-world-test"
parts := strings.Split("a,b,c", ",")                         // ["a", "b", "c"]
joined := strings.Join([]string{"x", "y", "z"}, " ")         // "x y z"

// Case manipulation
upper := strings.ToUpper("hello")        // "HELLO"
lower := strings.ToLower("WORLD")        // "world"
title := strings.TitleCase("hello world") // "Hello World"
cap := strings.Capitalize("hello")        // "Hello"

// Search and replace
contains, _ := strings.Contains("hello world", "world", false) // true
replaced, _ := strings.Replace("hello 123", "\\d+", "456", -1, true) // "hello 456"

// Encoding/Decoding
encoded := strings.Base64Encode("hello")     // "aGVsbG8="
decoded, _ := strings.Base64Decode(encoded)   // "hello"
urlSafe := strings.URIEncode("hello world")  // "hello%20world"

// Template formatting
formatted := strings.Format("Hello {name}!", map[string]string{"name": "Alice"}) // "Hello Alice!"

// Text normalization (removes diacritics)
normalized := strings.Normalize("café") // "cafe"
```

### Available Functions

- **Basic Operations**: `Concatenate`, `Split`, `Join`, `Trim`, `Substring`
- **Search & Replace**: `Replace` (with regex support), `Contains`
- **Case Operations**: `ToUpper`, `ToLower`, `TitleCase`, `Capitalize`
- **Encoding**: `Base64Encode`, `Base64Decode`, `URIEncode`, `URIDecode`
- **Text Processing**: `Format` (template replacement), `Normalize` (diacritic removal)
- **Regex Operations**: `RegexExtract`
- **Utilities**: `Length` (rune-aware character count)

All functions are Unicode-aware and handle multi-byte characters correctly.

## Schema Validation and Transformation

The SDK includes a comprehensive schema package for data validation, transformation, and structuring according to JSON Schema definitions:

```go
import "github.com/wehubfusion/Icarus/pkg/schema"

// Create schema engine
engine := schema.NewEngine()

// Parse and process data with schema
result, err := engine.ProcessWithSchema(
    inputData,           // JSON data to process
    schemaDefinition,    // Schema definition (JSON)
    schema.ProcessOptions{
        ApplyDefaults: true,  // Apply default values from schema
        StructureData: false, // Restructure data to match schema
    },
)

if err != nil {
    log.Fatalf("Schema processing failed: %v", err)
}

// Access results
if result.Valid {
    processedData := result.Data
    fmt.Println("Data validated and transformed successfully")
} else {
    for _, err := range result.Errors {
        fmt.Printf("Validation error at %s: %s\n", err.Path, err.Message)
    }
}
```

### CSV Schema Support (typed CSV)

Icarus also understands the typed CSV schema format described in `Olympus/csv.md`. Provide a CSV schema (with `columnHeaders`, optional `delimiter`, and basic validation) plus a JSON array of row objects, and run them through the CSV helpers:

```go
csvSchema := []byte(`{
  "name": "User Array - v3.0",
  "delimiter": ",",
  "columnHeaders": {
    "username": { "type": "STRING", "required": true },
    "email": { "type": "STRING", "required": true, "validation": { "format": "email" } },
    "employeeno": { "type": "NUMBER", "validation": { "minimum": 0, "maximum": 999999 } }
  }
}`)

rows := []byte(`[
  {"username": "jdoe", "email": "jdoe@example.com", "employeeno": 42},
  {"username": "asmith", "email": "asmith@example.com"}
]`)

result, err := engine.ProcessCSVWithSchema(
  rows,
  csvSchema,
  schema.ProcessOptions{
    ApplyDefaults: true, // fill missing defaults
    StructureData: true, // drop columns not in schema
  },
)
```

- CSV column order from the schema is preserved in `ColumnOrder` metadata for downstream CSV emitters.
- Supported CSV column types: `STRING`, `NUMBER`, `DATE` (uses `format: "date"` validation).
- Validators reuse the same format rules (`email`, `uri`, `uuid`, `date`, `datetime`), enums, lengths, and number min/max as JSON schemas.

### HL7 v2.x Schema Validation

The schema engine includes a dedicated **HL7 v2.x** validator in `pkg/schema/hl7` that validates raw HL7 messages against JSON-based schema definitions and optional CEL custom rules.

```go
import "github.com/wehubfusion/Icarus/pkg/schema"

engine := schema.NewEngine()

result, err := engine.ProcessHL7WithSchema(
    []byte(hl7Message),    // raw HL7 v2.x message bytes
    []byte(hl7SchemaJSON), // HL7 schema definition (JSON)
    schema.ProcessOptions{
        CollectAllErrors: true,
    },
)
if err != nil {
    log.Fatalf("HL7 processing failed: %v", err)
}
// result.Errors   — ERROR-severity issues
// result.Warnings — WARNING-severity issues
// result.Infos    — INFO-severity issues
// result.Data     — original message bytes (unchanged)
for _, e := range result.Errors {
    fmt.Printf("error at %s (%s): %s\n", e.Path, e.Code, e.Message)
}
```

**Key validation layers:**

- **Structural**: segment order, required/optional/repeated groups, unexpected segments.
- **Message header**: MSH-9 message type and MSH-12 version (semantic: `"2.5"` == `"2.5.0"`).
- **Field / component**: usage (R/X/W), repetition limits, length (truncation-delimiter-aware for HL7 v2.7+), datatype format and calendar range (month-specific day count, leap-year, TZ offset).
- **Composite datatypes**: registry-driven decomposition — leaf components are validated by their own scalar types.
- **CEL custom rules**: declarative `assert`/`require`/`forbid` rules with `when` guards, scope inference per segment, and helper functions (`msg`, `valued`, `validateAs`, `toDTM`, `toNumber`, `matchesPattern`).

**Security guards:** messages larger than 10 MB or with more than 5,000 segments are rejected before parsing.

For full details on schema format, error codes, severity tables, and CEL rule authoring, see [`pkg/schema/hl7/README.md`](pkg/schema/hl7/README.md).

### Schema Types

The schema package supports the following data types:

- **STRING**: Text data with optional length, pattern, format, and enum constraints
- **NUMBER**: Numeric data with optional min/max constraints
- **BOOLEAN**: True/false values
- **OBJECT**: Nested objects with properties
- **ARRAY**: Arrays with item schema and optional length, uniqueness constraints
- **DATE**: Date values (YYYY-MM-DD format)
- **DATETIME**: DateTime values (RFC3339 format)
- **BYTE**: Base64-encoded binary data with optional decoded byte length constraints
- **UUID**: UUID values with optional `prefix` and `postfix` (e.g. `urn:uuid:...`). Missing UUID fields are auto-generated when `ApplyDefaults` is true. No validation rules (minLength, pattern, etc.) are supported for this type.
- **ANY**: Any JSON value (no validation)

### Validation Rules

Each property in a schema can have validation rules:

**String Validations:**
```json
{
  "type": "STRING",
  "validation": {
    "minLength": 3,
    "maxLength": 50,
    "pattern": "^[a-zA-Z]+$",
    "format": "email",
    "enum": ["option1", "option2"]
  }
}
```

**Number Validations:**
```json
{
  "type": "NUMBER",
  "validation": {
    "minimum": 0,
    "maximum": 100
  }
}
```

**Array Validations:**
```json
{
  "type": "ARRAY",
  "validation": {
    "minItems": 1,
    "maxItems": 10,
    "uniqueItems": true
  },
  "items": {
    "type": "STRING"
  }
}
```

**Byte Validations:**
```json
{
  "type": "BYTE",
  "validation": {
    "minLength": 100,
    "maxLength": 1048576
  }
}
```

The `BYTE` type expects base64-encoded string data. Validation rules (`minLength` and `maxLength`) apply to the decoded byte length, not the base64 string length. This allows precise control over binary data sizes for file attachments, images, certificates, and other binary content.

**Example Usage:**
```go
// Schema for file upload with size constraints
schemaJSON := `{
  "type": "OBJECT",
  "properties": {
    "filename": {
      "type": "STRING",
      "required": true
    },
    "content": {
      "type": "BYTE",
      "required": true,
      "validation": {
        "minLength": 1,
        "maxLength": 5242880
      }
    }
  }
}`

// Input data with base64-encoded file
inputData := `{
  "filename": "document.pdf",
  "content": "JVBERi0xLjQKJeLjz9MKMyAwIG9iago8PC9UeXBl..."
}`

// Validates that decoded content is between 1 byte and 5MB
result, err := engine.ProcessWithSchema(
    []byte(inputData),
    []byte(schemaJSON),
    schema.ProcessOptions{ValidateData: true},
)
```

### Supported Formats

The schema validator includes built-in format validators:

- `email`: RFC 5322 email validation
- `uri`: URI/URL validation (HTTP, HTTPS, FTP, WS, WSS)
- `uuid`: UUID v4 format validation
- `date`: ISO 8601 date format (YYYY-MM-DD)
- `datetime`: RFC3339 datetime format

You can also register custom format validators:

```go
validator := schema.NewValidator()
validator.RegisterFormat("phone", func(value string) bool {
    // Custom phone number validation logic
    return regexp.MustCompile(`^\+?[1-9]\d{1,14}$`).MatchString(value)
})
```

### Default Values

The schema engine can automatically apply default values to missing fields:

```go
// Schema with default values
schemaJSON := `{
  "type": "OBJECT",
  "properties": {
    "name": {
      "type": "STRING",
      "required": true
    },
    "country": {
      "type": "STRING",
      "default": "US"
    },
    "active": {
      "type": "BOOLEAN",
      "default": true
    }
  }
}`

// Input data missing 'country' and 'active'
inputData := `{"name": "John"}`

// Process with defaults
result, _ := engine.ProcessWithSchema(
    []byte(inputData),
    []byte(schemaJSON),
    schema.ProcessOptions{ApplyDefaults: true},
)

// Result: {"name": "John", "country": "US", "active": true}
```

### Data Structuring

The schema engine can restructure data to match the schema structure:

```go
// Flat input data
inputData := `{
  "firstName": "John",
  "lastName": "Doe",
  "email": "john@example.com"
}`

// Nested schema structure
schemaJSON := `{
  "type": "OBJECT",
  "properties": {
    "user": {
      "type": "OBJECT",
      "properties": {
        "name": {
          "type": "OBJECT",
          "properties": {
            "first": {"type": "STRING"},
            "last": {"type": "STRING"}
          }
        },
        "contact": {
          "type": "OBJECT",
          "properties": {
            "email": {"type": "STRING"}
          }
        }
      }
    }
  }
}`

// Process with structuring
result, _ := engine.ProcessWithSchema(
    []byte(inputData),
    []byte(schemaJSON),
    schema.ProcessOptions{StructureData: true},
)

// Result is restructured to match schema
```

### Field Truncation

Remove fields not defined in the schema:

```go
result, _ := engine.ProcessWithSchema(
    inputData,
    schemaDefinition,
    schema.ProcessOptions{
        TruncateFields: true,
        TruncateLevel:  0,  // 0 = all levels, 1 = first level only, etc.
    },
)
```

### Components

The schema package is organized into focused components:

- **`types.go`**: Schema structure definitions (Schema, Property, ValidationRules)
- **`parser.go`**: Schema parsing from JSON
- **`validator.go`**: Data validation against schemas
- **`transformer.go`**: Data transformation (defaults, structuring, truncation)
- **`engine.go`**: Main orchestrator coordinating all operations
- **`formats.go`**: Format validators (email, UUID, date, etc.)
- **`errors.go`**: Schema-specific error types

### Usage in Plugins

The schema package is designed to be used by Elysium plugins like `plugin-stage-data`, `plugin-json-operations`, `plugin-http-client`, and others that need schema-based validation and transformation:

```go
import (
    "github.com/wehubfusion/Icarus/pkg/schema"
)

// In your plugin activity
type StageDataActivity struct {
    schemaEngine *schema.Engine
}

func NewActivity(logger *zap.Logger) *Activity {
    return &Activity{
        schemaEngine: schema.NewEngine(),
    }
}

func (a *Activity) Execute(ctx context.Context, req *Request) (*Response, error) {
    // Fetch schema from Morpheus
    schemaDefinition := fetchSchemaFromMorpheus(req.SchemaID)
    
    // Process data with schema
    result, err := a.schemaEngine.ProcessWithSchema(
        req.InputData,
        schemaDefinition,
        schema.ProcessOptions{
            ApplyDefaults:  true,
            ValidateData:   true,
            TruncateFields: true,
        },
    )
    
    if err != nil {
        return nil, err
    }
    
    if !result.Valid {
        return &Response{
            Success: false,
            ValidationErrors: result.Errors,
        }, nil
    }
    
    return &Response{
        Success: true,
        Data:    result.Data,
    }, nil
}
```

## Complete Example

Here's a comprehensive example showing the client usage pattern:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/wehubfusion/Icarus/pkg/client"
    "github.com/wehubfusion/Icarus/pkg/message"
    "github.com/google/uuid"
)

func main() {
    // Create and connect
    c := client.NewClient("nats://localhost:4222")
    ctx := context.Background()

    if err := c.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Publish a test message
    msg := message.NewWorkflowMessage("test-workflow", uuid.New().String()).
        WithPayload("example-service", "Hello, World!", "msg-123").
        WithMetadata("test", "true")
    if err := c.Messages.Publish(ctx, "events.test", msg); err != nil {
        log.Printf("Failed to publish: %v", err)
    }

    // Simulate consuming the message using pull-based approach
    // In a real application, you would pull from a consumer
    messages, err := c.Messages.PullMessages(ctx, "TEST_EVENTS", "test-consumer", 5)
    if err != nil {
        log.Printf("Failed to pull messages: %v", err)
    } else {
        for _, pulledMsg := range messages {
            var content string
            if pulledMsg.Payload != nil {
                content = pulledMsg.Payload.InlineData
            }
            fmt.Printf("Pulled: %s (Workflow: %s)\n", content, pulledMsg.Workflow.WorkflowID)
            pulledMsg.Ack() // Acknowledge message
        }
    }
}
```

### Pull-Based Consumers Example

```go
// Pull messages from JetStream
messages, err := c.Messages.PullMessages(ctx, "EVENTS", "pull-consumer", 10)
if err != nil {
    log.Printf("Failed to pull messages: %v", err)
    return
}

for _, msg := range messages {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.GetInlineData()
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)
    // Messages are automatically acknowledged after successful deserialization
}
```

### Runner Example

```go
// Define a message processor for concurrent processing
type RunnerProcessor struct{}

func (RunnerProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.GetInlineData()
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)
    // Simulate processing work
    time.Sleep(100 * time.Millisecond)

    result := message.NewMessage().WithPayload("processor", "ok", "result-ref")
    if msg.Workflow != nil {
        result.Workflow = msg.Workflow
    }
    return *result, nil
}

processor := &RunnerProcessor{}

// Create and configure runner for concurrent processing
cfg := runner.DefaultConfig()
cfg.WorkerCount = 3

runnerInstance, err := runner.NewRunner(
    c,
    runner.ProcessorFunc(processor), // wrap func to implement Processor
    "EVENTS",
    "worker-consumer",
    5,               // batchSize
    5*time.Minute,   // timeout per message
    logger,
    nil,             // tracing
    &cfg,            // worker config (nil uses defaults/env)
)
if err != nil {
    log.Fatalf("failed to create runner: %v", err)
}

// Start processing in background (blocks until context cancelled)
go func() {
    if err := runnerInstance.Run(ctx); err != nil {
        log.Printf("Runner error: %v", err)
    }
}()

// Publish messages for processing
for i := 0; i < 10; i++ {
    msg := message.NewWorkflowMessage("batch-workflow", fmt.Sprintf("run-%d", i)).
        WithPayload("batch-service", fmt.Sprintf("Message %d", i), fmt.Sprintf("msg-%d", i))
    if err := c.Messages.Publish(ctx, "events.process", msg); err != nil {
        log.Printf("Failed to publish: %v", err)
    }
}

time.Sleep(2 * time.Second) // Wait for processing
```

## Examples

See the `examples/` directory for complete working examples:

- **examples/message/main.go**: Demonstrates JetStream messaging patterns including message publishing, pull-based consumers, and callback reporting
- **examples/runner/main.go**: Shows concurrent message processing using the Runner with worker pools and callback reporting
- **examples/runner-with-tracing/main.go**: Demonstrates the Runner with OpenTelemetry tracing integration using Jaeger
- **examples/process/strings/main.go**: Comprehensive string processing utilities demo

To run examples:

```bash
# Start NATS server
docker run -p 4222:4222 nats:latest

# For tracing examples, also start Jaeger
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest

# Run message example
go run examples/message/main.go

# Run runner example
go run examples/runner/main.go

# Run runner with tracing example
go run examples/runner-with-tracing/main.go

# Run string processing example
go run examples/process/strings/main.go
```

## Migration Guide

If you have existing code using the old pattern:

**Before:**

```go
c := client.NewClient(url)
c.Connect(ctx)
msgService, _ := message.NewMessageService(c.Connection())
msgService.Publish(ctx, subject, msg)
```

**After:**

```go
c := client.NewClient(url)
c.Connect(ctx)
c.Messages.Publish(ctx, subject, msg)
```

The old pattern still works but the new pattern is more concise.

## Best Practices

1. **Always use context**: Pass context to all operations for proper cancellation
2. **JetStream is required**: This SDK requires JetStream to be enabled on the NATS server
3. **Handle errors properly**: Check and handle all errors returned by SDK methods
4. **Close resources**: Always defer `c.Close()` after connecting
5. **Use structured logging**: Configure logging for better observability and debugging
6. **Set timeouts**: Use context timeouts for operations that shouldn't block indefinitely
7. **Unique message IDs**: Generate unique IDs (e.g., using UUID) for message tracing
8. **Service access**: Use `c.Messages` directly instead of creating MessageService manually

## Public API

### Constructor Functions

```go
// Create with default config
NewClient(url string) *Client

// Create with custom config
NewClientWithConfig(config *nats.ConnectionConfig) *Client
```

### Connection Methods

```go
Connect(ctx context.Context) error
Close() error
IsConnected() bool
Ping(ctx context.Context) error
```

### Service Access

```go
Messages *message.MessageService  // Access to all messaging operations
JetStream() natsclient.JetStreamContext  // Direct JetStream access
Connection() *natsclient.Conn  // Low-level connection access
```

### Monitoring

```go
Stats() ConnectionStats  // Get connection statistics
```

### Usage Pattern

```go
// 1. Create and connect
c := client.NewClient("nats://localhost:4222")
ctx := context.Background()
c.Connect(ctx)
defer c.Close()

// 2. Use services directly
msg := message.NewWorkflowMessage("workflow-123", "run-456").
    WithPayload("service", "Hello World", "msg-789")
c.Messages.Publish(ctx, "subject", msg)
```

## Future Roadmap

- **Process Management**: Service registration, discovery, and health monitoring (string processing utilities implemented)
- **Metrics**: Built-in metrics collection and export
- **Advanced JetStream Features**: Enhanced streams, consumers, and persistence management
- **Message Encryption**: End-to-end encryption support
- **Schema Validation**: Message schema validation
- **Advanced Routing**: Content-based routing and message transformation
- **Health Monitoring**: Service health checks and automatic recovery

## Requirements

- Go 1.25.1 or higher
- NATS Server 2.x or higher with JetStream enabled (required for all operations)

## Changelog

Recent entries (last 3):

- **[0.10.0] 2026-05-07**: Embedded node failures now propagate correctly through
  `ProcessFailureObserver` via the new `NodeFailureError` type carrying `NodeID`,
  `PluginType`, and `Permanent bool`.
- **[0.9.0] 2026-05-03**: CSV column ordering is now controlled by the `position` field
  in the schema definition.
- **[0.8.0] 2026-04-23**: New `ProcessOptions.CodeSeverityOverrides` replaces the
  removed `ValidationMode`/`StrictValidation`. Allows per-code severity override
  including `SeverityDrop` to suppress a code entirely.

See [CHANGELOG.md](CHANGELOG.md) for the full history.
