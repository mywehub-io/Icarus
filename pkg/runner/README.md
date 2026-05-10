# pkg/runner

Concurrent message processing framework. Pulls messages from a NATS JetStream consumer,
dispatches them through a worker pool, and reports results back to Zeus.

## Quick start

```go
import (
    "github.com/wehubfusion/Icarus/pkg/client"
    "github.com/wehubfusion/Icarus/pkg/runner"
)

c := client.NewClient("nats://localhost:4222", "RESULTS_UAT", "result.uat")
c.Connect(ctx)
defer c.Close()

r, err := runner.NewRunner(
    c,
    myProcessor,       // implements runner.Processor
    "WORKFLOWS",       // stream
    "my-service",      // consumer
    10,                // batchSize
    30*time.Second,    // processTimeout per message
    logger,
    nil,               // tracingConfig — nil disables tracing
    nil,               // Config — nil uses DefaultConfig
)
if err != nil { ... }
defer r.Close()

if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    logger.Error("runner exited", zap.Error(err))
}
```

## `Processor` interface

```go
type Processor interface {
    Process(ctx context.Context, msg *message.Message) (message.Message, error)
}
```

Your business logic goes here. Return `error` to trigger `ReportError`; return a result
`message.Message` to trigger `ReportSuccess`.

## `NewRunner` parameters

| Parameter | Description |
|---|---|
| `client` | Connected `*client.Client`; must not be nil |
| `processor` | Your `Processor` implementation |
| `stream` | JetStream stream name; created if it does not exist |
| `consumer` | JetStream consumer (durable) name; created if it does not exist |
| `batchSize` | Number of messages to pull per fetch |
| `processTimeout` | Per-message deadline added on top of the parent context |
| `logger` | Required `*zap.Logger` |
| `tracingConfig` | Optional OTel tracing; pass nil to disable |
| `cfg` | Optional `*Config` for worker pool tuning; nil uses `DefaultConfig` |
| `opts` | Functional options, e.g. `WithProcessFailureObserver` |

## Worker pool config

```go
type Config struct {
    WorkerCount int // 0 → ICARUS_RUNNER_WORKERS env → ICARUS_RUNNER_WORKER_MULTIPLIER × GOMAXPROCS → GOMAXPROCS
    QueueSize   int // 0 → 4×WorkerCount (min WorkerCount, max 1000)
}
```

Environment overrides:

| Env var | Description |
|---|---|
| `ICARUS_RUNNER_WORKERS` | Exact worker count |
| `ICARUS_RUNNER_WORKER_MULTIPLIER` | Multiplied by `GOMAXPROCS` |

(`pkg/runner/runner.go:104`)

## Error reporting

When `Process` returns an error:

1. `ReportError` publishes a failure result to the result subject with a 10-minute timeout
   (to handle slow Temporal signal delivery). On failure, one automatic retry after 2 s.
2. The original message is acknowledged (so JetStream does not redeliver).
3. `ProcessFailureObserver` is called (if registered) for side effects like Argus `node.ended` emission.

## `ProcessFailureObserver`

```go
type ProcessFailureObserver func(ctx context.Context, msg *message.Message, processErr error) error
```

Called after every successful `ReportError`. Use to emit Argus observation events on failure.
Register with `WithProcessFailureObserver`:

```go
r, _ := runner.NewRunner(..., runner.WithProcessFailureObserver(func(ctx context.Context, msg *message.Message, err error) error {
    return nodeEndEmitter.EmitNodeEnd(ctx, ...)
}))
```

(`pkg/runner/runner.go:37`)

## Tracing

```go
tracingCfg := runner.DefaultTracingConfig("my-service")
// or
tracingCfg := runner.JaegerTracingConfig("my-service")
```

The runner starts an OTel trace span per message (`runner.processMessage`) with a nested
span for the `Processor.Process` call. Spans carry `workflow.id`, `workflow.run_id`,
`correlation.id`, `stream`, `consumer`, and `processing.duration_ms`. Tracing is shut down
gracefully in `r.Close()`.

## Process timeout vs context cancellation

The runner distinguishes three error cases:

| Condition | Log message |
|---|---|
| `processCtx.Err() == context.DeadlineExceeded` | `"Process timeout exceeded for message (processCtx deadline exceeded — Icarus processTimeout config; …)"` |
| `ctx.Err() != nil` (parent context) | `"Process cancelled by parent context (runner shutting down?)"` |
| Other error | `"Error processing message"` |

(`pkg/runner/runner.go:515`)
