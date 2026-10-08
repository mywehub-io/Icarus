# pkg/runner

Concurrent message processing framework. Consumes messages from a NATS JetStream consumer
via the new `nats.go/jetstream` `Consume` API, dispatches them through a worker pool, and
reports results back to Zeus.

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
| `batchSize` | Cap on messages per fetch; a fetch never asks for more than the idle workers |
| `processTimeout` | Per-message deadline added on top of the parent context |
| `logger` | Required `*zap.Logger` |
| `tracingConfig` | Optional OTel tracing; pass nil to disable |
| `cfg` | Optional `*Config` for worker pool tuning; nil uses `DefaultConfig` |
| `opts` | Functional options, e.g. `WithProcessFailureObserver` |

## Worker pool config

```go
type Config struct {
    WorkerCount  int           // 0 → ICARUS_RUNNER_WORKERS env → ICARUS_RUNNER_WORKER_MULTIPLIER × GOMAXPROCS → GOMAXPROCS
    QueueSize    int           // ignored since v0.28.0
    StallTimeout time.Duration // 0 → ICARUS_RUNNER_STALL_TIMEOUT env → 2m; negative disables the watchdog
}
```

The runner fetches only as many messages as it has idle workers and hands each straight to
one. A message it cannot start yet stays undelivered in the stream: no ack deadline runs on it,
no delivery attempt is used, and another replica with a free worker can take it.

Environment overrides:

| Env var | Description |
|---|---|
| `ICARUS_RUNNER_WORKERS` | Exact worker count |
| `ICARUS_RUNNER_WORKER_MULTIPLIER` | Multiplied by `GOMAXPROCS` |
| `ICARUS_RUNNER_STALL_TIMEOUT` | Watchdog stall timeout, a Go duration (e.g. `2m`); used when `Config.StallTimeout` is 0 |

### Stall watchdog and `Health`

`Run` starts a watchdog. If the runner goes `StallTimeout` without a successful fetch while a
worker is idle and the NATS connection is up, or a worker holds one unit for longer than
`processTimeout` plus 15 minutes, it logs at Error with the idle slots, in-flight count and
consumer, and resolves the consumer again. `Runner.Health()` returns an error for as long as the
stall lasts, and nil before `Run`, after its context ends and while NATS is disconnected. Wire it
into the host's liveness probe, so a runner that cannot recover is restarted.

The fetch loop also resolves the consumer again when the client's connection is replaced or a
fetch fails with a transport error, and creates the consumer again if it no longer exists. A
consumer handle stays bound to the connection it was resolved on; previously a fetch on a
closed connection (`nats.ErrConnectionClosed`) kept the old handle, so the loop failed every
fetch without sending a pull after the client reconnected.

(`pkg/runner/runner.go:104`)

## Error reporting

When `Process` returns a transient error (`message.IsTransientError`: an `Internal` `AppError`,
a `TransientClassifier`, or a failure reaching a dependency; anything else is permanent) and
delivery attempts remain, and the unit did not outrun `processTimeout`, the runner retries:
it marks the unit `retrying` in `EXECUTION_HEARTBEATS` and naks the message with a delay
(5 s, 15 s, 30 s, then 60 s). Nothing is reported.

On a permanent error, or the last attempt:

1. `ReportError` publishes a failure result to the result subject, with its attempt number,
   with a 10-minute timeout. On failure, one automatic retry after 2 s.
2. The original message is acknowledged (permanent) or terminated (transient), so JetStream
   does not redeliver it.
3. `ProcessFailureObserver` is called (if registered) for side effects like Argus `node.ended` emission.

See `docs/error-handling-patterns.md`.

## Execution claim

Before running a unit the runner claims it in the `EXECUTION_HEARTBEATS` KV bucket (key
`<workflow>.<run>.<node>`, 90 s TTL). The entry's `state` decides what a later delivery does:

| Entry | Delivery |
|---|---|
| none | Creates the entry and runs |
| `retrying` | Takes over (revision-checked `Update`) and runs |
| `running`, no write for 60 s | Takes over: the owning pod is presumed dead |
| `running`, fresh | Nak'd with a 30 s delay: another worker is running it |
| `done`, same execution ID | Terminated: a duplicate of a completed execution |
| `done`, other execution ID | Takes over: Zeus dispatched the node again |

While running, the entry is refreshed every 30 s. A KV error fails open: the unit runs.

## `ProcessFailureObserver`

```go
type ProcessFailureObserver func(ctx context.Context, msg *message.Message, processErr error) error
```

Called after `ReportError` publishes a failure: once per unit, never for a failure that is
being retried. Use to emit Argus observation events on failure.
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
