# Configuration reference

## Environment variables

| Variable | Used by | Description |
|---|---|---|
| `ICARUS_RUNNER_WORKERS` | `pkg/runner` | Exact worker goroutine count. Overrides `Config.WorkerCount` |
| `ICARUS_RUNNER_WORKER_MULTIPLIER` | `pkg/runner` | Multiplied by `GOMAXPROCS` to derive worker count. Applied when `ICARUS_RUNNER_WORKERS` is unset |

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
| `MaxReconnects` | — | Maximum reconnection attempts (-1 = infinite) |
| `ReconnectWait` | — | Delay between reconnection attempts |
| `Timeout` | — | Connection and ping timeout |
| `MaxDeliver` | `5` | JetStream consumer max deliver (redelivery count before dead-letter) |
| `PublishMaxRetries` | `3` | Retry count for `ReportSuccess` / `ReportError` publishes |
| `ResultStream` | — | Overrides the result stream at config level |
| `ResultSubject` | — | Overrides the result subject at config level |

## `runner.NewRunner` parameters

| Parameter | Type | Description |
|---|---|---|
| `batchSize` | `int` | Messages pulled per JetStream fetch; must be > 0 |
| `processTimeout` | `time.Duration` | Per-message deadline; must be > 0 |
| `tracingConfig` | `*TracingConfig` | nil disables OTel tracing |
| `cfg` | `*Config` | nil uses `DefaultConfig()` (workers = GOMAXPROCS, queue = 4×workers) |

## `runner.Config`

```go
type Config struct {
    WorkerCount int // explicit goroutine count; 0 = auto-detect
    QueueSize   int // job queue depth; 0 = 4×WorkerCount (min WorkerCount, max 1000)
}
```

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
