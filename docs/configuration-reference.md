# Configuration reference

## Environment variables

| Variable | Used by | Description |
|---|---|---|
| `ICARUS_RUNNER_WORKERS` | `pkg/runner` | Exact worker goroutine count. Overrides `Config.WorkerCount` |
| `ICARUS_RUNNER_WORKER_MULTIPLIER` | `pkg/runner` | Multiplied by `GOMAXPROCS` to derive worker count. Applied when `ICARUS_RUNNER_WORKERS` is unset |
| `ICARUS_RUNNER_STALL_TIMEOUT` | `pkg/runner` | Go duration, default `2m`, negative turns it off. How long a runner may go without a successful fetch while a worker is idle and NATS is connected before its watchdog logs an error and resolves the consumer again, and `Runner.Health` reports it stalled. Overridden by `Config.StallTimeout` |

Resolution order for worker count: `Config.WorkerCount` (if > 0) → `ICARUS_RUNNER_WORKERS` → `ICARUS_RUNNER_WORKER_MULTIPLIER × GOMAXPROCS` → `GOMAXPROCS`.

## `client.NewClient` parameters

| Parameter | Description |
|---|---|
| `url` | NATS server URL, e.g. `"nats://nats:4222"` |
| `resultStream` | JetStream stream where results are published, e.g. `"RESULTS_UAT"` |
| `resultSubject` | Subject within the result stream, e.g. `"result.uat"` |

## `ConnectionConfig` fields (via `NewClientWithConfig`)

| Field | Default | Description |
|---|---|---|
| `URL` | — | NATS server address |
| `Name` | — | Client name shown in NATS monitoring |
| `MaxReconnects` | `-1` | Maximum reconnection attempts (`-1` = unlimited; default via `DefaultConnectionConfig`) |
| `ReconnectWait` | `2s` | Delay between reconnection attempts |
| `Logger` | — | Optional `*zap.Logger` for disconnect, reconnect, and closed events |
| `Timeout` | — | Connection and ping timeout |
| `MaxDeliver` | `5` | JetStream consumer max deliver (redelivery count before dead-letter) |
| `PublishMaxRetries` | `3` | Retry count for `ReportSuccess` / `ReportError` publishes |
| `ResultStream` | — | Overrides the result stream at config level |
| `ResultSubject` | — | Overrides the result subject at config level |

## `Client.EnsureConnected`

```go
func (c *Client) EnsureConnected(ctx context.Context) error
```

Reconnects when the NATS TCP connection is missing or closed. Safe for concurrent use (for example multiple `pkg/runner` instances sharing one client). The Icarus runner calls this automatically after fatal consume errors or publish transport errors.

## `runner.NewRunner` parameters

| Parameter | Type | Description |
|---|---|---|
| `batchSize` | `int` | Cap on messages per fetch; a fetch never asks for more than the idle workers, or with `WithGate`, than the gate admits and the consumer reports waiting. Must be > 0 |
| `processTimeout` | `time.Duration` | Per-message deadline; must be > 0 |
| `tracingConfig` | `*TracingConfig` | nil disables OTel tracing |
| `cfg` | `*Config` | nil uses `DefaultConfig()` (workers = GOMAXPROCS) |

## `runner.Config`

```go
type Config struct {
    WorkerCount int // explicit goroutine count; 0 = auto-detect
    QueueSize   int // ignored since v0.28.0: the runner fetches only for idle workers
}
```

## Runner options

| Option | Effect |
|---|---|
| `WithGate(Gate)` | No worker pool: the runner takes work as the `Gate` admits it (`Acquire(ctx, max)`, `Release(n)`), one goroutine per unit. `Config.WorkerCount` is ignored (`pkg/runner/gate.go`) |
| `WithUnitObserver(UnitObserver)` | Unit start and finish, for metrics. If it also implements `ConsumerObserver`, the consumer's backlog every 15 s |
| `WithProcessFailureObserver(ProcessFailureObserver)` | Called after a failed result is published |
| `WithConsumerFilterSubject(string)` | The durable's FilterSubject |

A processor may return `runner.ErrHandOff` while `runner.HandOffAllowed(ctx)` to have the unit
re-published for another replica; see the upgrade guide.

## `TracingConfig`

| Field | Description |
|---|---|
| `ServiceName` | OTel service name (required) |
| `ServiceVersion` | Semantic version string |
| `Environment` | e.g. `"uat"` or `"production"` |
| `OTLPEndpoint` | OTLP HTTP endpoint, e.g. `"http://jaeger:4318"` |
| `SampleRatio` | 0.0–1.0 sampling ratio |

Convenience constructors:

```go
runner.DefaultTracingConfig("my-service")  // development defaults
runner.JaegerTracingConfig("my-service")   // Jaeger-optimised defaults
```

## Blob storage

| Constructor parameter | Description |
|---|---|
| `connectionString` | Azure Blob Storage connection string (shared key) |
| `containerName` | Container for result uploads |
| `logger` | Required `*zap.Logger` |

For local development with Azurite, use an `http://` endpoint in the connection string.
The client detects `http://` and sets `InsecureAllowCredentialWithHTTP`.

## Schema engine (`pkg/schema`)

No environment variables. All configuration is passed via `ProcessOptions`:

| Option | Type | Default |
|---|---|---|
| `ApplyDefaults` | `bool` | `false` |
| `StructureData` | `bool` | `false` |
| `CollectAllErrors` | `bool` | `false` — stops at first error |
| `CodeSeverityOverrides` | `map[string]Severity` | `nil` |
