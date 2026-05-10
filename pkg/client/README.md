# pkg/client

Central JetStream client. The single entry point for all Icarus messaging operations.

## Quick start

```go
import "github.com/wehubfusion/Icarus/pkg/client"

c := client.NewClient("nats://localhost:4222", "RESULTS_UAT", "result.uat")
if err := c.Connect(ctx); err != nil {
    logger.Fatal("connect failed", zap.Error(err))
}
defer c.Close()
```

## `NewClient`

```go
func NewClient(url, resultStream, resultSubject string) *Client
```

Creates a client with default connection config. `resultStream` and `resultSubject` are
where `ReportSuccess` / `ReportError` publish their result messages.

## `NewClientWithConfig`

```go
func NewClientWithConfig(config *nats.ConnectionConfig) *Client
```

Full control over reconnection, timeouts, `MaxDeliver`, and `PublishMaxRetries`. See
[`internal/nats`](../../internal/nats/) for `ConnectionConfig` fields.

## `Connect`

```go
func (c *Client) Connect(ctx context.Context) error
```

Establishes TCP, creates the JetStream context, and initialises `c.Messages`. Idempotent —
returns nil if already connected. Fails with `JETSTREAM_NOT_ENABLED` if the NATS server
does not have JetStream enabled. (`pkg/client/client.go:119`)

## `Client.Messages`

`*message.MessageService` — access pull consumers, publish results, and manage streams.
Only available after `Connect` succeeds.

## Blob storage

```go
c.SetBlobStorage(blobClient) // inject after Connect
```

Propagates the `storage.BlobStorageClient` into `Messages` so that large result payloads
are uploaded to Azure Blob Storage instead of sent inline over NATS. (`pkg/client/client.go:292`)

## Connection helpers

| Method | Description |
|---|---|
| `IsConnected()` | Returns true when the NATS TCP connection is live |
| `Ping(ctx)` | Flushes and returns an error if the server is unreachable |
| `Stats()` | Returns `ConnectionStats` (messages sent/received, reconnect count) |
| `JetStream()` | Returns the raw `nats.JetStreamContext` for advanced operations |
| `Close()` | Drains in-flight messages and closes the TCP connection |

## Testing without a real NATS server

```go
js := myMockJetStream{} // implements message.JSContext
c := client.NewClientWithJSContext(js)
// c.Messages is ready; no Connect needed
```

`NewClientWithJSContext` wires a pre-built `message.JSContext` directly, bypassing TCP
connection. Use it in unit tests with a stub or mock. (`pkg/client/client.go:163`)
