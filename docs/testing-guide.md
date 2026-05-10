# Testing guide

## Running the tests

```bash
go test ./...
```

All packages include unit tests. Integration tests that require NATS or Azure Blob can
be run with the services started locally (see below).

## Unit testing the runner with a mock

`tests/` includes a `MockJetStream` implementing `nats.JetStreamContext`. Use it without
a live NATS server:

```go
import "github.com/wehubfusion/Icarus/tests"

js := tests.NewMockJetStream()
c := client.NewClientWithJSContext(message.WrapNATSJetStream(js))
```

`MockJetStream` captures all published messages in memory and supports `SetPublishError`
for fault injection:

```go
js.SetPublishError(errors.New("broker unavailable"))
// Emit will now fail
js.SetPublishError(nil) // reset
```

Inspect published messages:

```go
msgs := js.GetPublishedMessages()
// msgs[i].Subject, msgs[i].Data (raw JSON), msgs[i].MsgID
```

## Testing a `Processor`

```go
type testProcessor struct{ called bool }

func (p *testProcessor) Process(_ context.Context, msg *message.Message) (message.Message, error) {
    p.called = true
    return *msg, nil
}

js := tests.NewMockJetStream()
c := client.NewClientWithJSContext(message.WrapNATSJetStream(js))

r, _ := runner.NewRunner(c, &testProcessor{}, "STREAM", "consumer", 1, time.Second, zap.NewNop(), nil, nil)
```

## Testing with a local NATS server

```bash
docker run -p 4222:4222 nats -js
```

```go
c := client.NewClient("nats://localhost:4222", "RESULTS", "result")
c.Connect(ctx)
defer c.Close()
```

## Testing with Azurite (local Azure Blob Storage)

```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
```

```go
connStr := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KbiE9mZbM9w==;" +
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"

blob, _ := storage.NewAzureBlobClient(connStr, "results", zap.NewNop())
c.SetBlobStorage(blob)
```

## Testing the schema engine

The schema engine has no external dependencies:

```go
engine := schema.NewEngine()

result, err := engine.ProcessWithSchema(
    []byte(`{"name": "Alice"}`),
    []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`),
    schema.ProcessOptions{CollectAllErrors: true},
)
assert.True(t, result.Valid)
assert.Empty(t, result.Errors)
```

## Testing embedded nodes

Implement `EmbeddedNode` with a stub:

```go
type stubNode struct{ id string }
func (n *stubNode) NodeId() string     { return n.id }
func (n *stubNode) PluginType() string { return "stub" }
func (n *stubNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
    return runtime.ProcessOutput{Data: []byte(`{"ok":true}`)}
}
```

Register with a factory:

```go
factory := runtime.NewEmbeddedNodeFactory()
factory.Register("stub", func(cfg runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
    return &stubNode{id: cfg.NodeID}, nil
})
```

## Testing resolver with a stub blob client

```go
type stubBlob struct{}

func (s *stubBlob) DownloadResult(_ context.Context, url string) ([]byte, error) {
    return []byte(`{"stub":true}`), nil
}
func (s *stubBlob) UploadResult(_ context.Context, _ string, _ []byte, _ map[string]string) (string, error) {
    return "https://stub/result", nil
}
func (s *stubBlob) DownloadFromURL(_ context.Context, url string) ([]byte, error) {
    return []byte(`{"stub":true}`), nil
}

svc := resolver.NewService(&stubBlob{}, 0)
data, err := svc.ResolveInput(ctx, nil, &message.BlobReference{URL: "https://any"})
```

## Runnable examples

The `examples/` directory contains four self-contained programs you can run to verify your setup:

| Example | Package | What it demonstrates |
| --- | --- | --- |
| [`examples/message/`](../examples/message/main.go) | `pkg/message` | Publish a message to a NATS JetStream stream and pull it back. |
| [`examples/runner/`](../examples/runner/main.go) | `pkg/runner` | Start a worker-pool runner, process a message, and shut down cleanly. |
| [`examples/runner-with-tracing/`](../examples/runner-with-tracing/main.go) | `pkg/runner` + OpenTelemetry | Same as above with OTLP tracing enabled. |
| [`examples/schema-engine/`](../examples/schema-engine/main.go) | `pkg/schema` | Run a JSON schema validation with a custom severity override and CEL rule. |

Run any example directly:

```bash
cd examples/runner
go run main.go
```

Each example reads its configuration from environment variables; see the comments at the top of each `main.go` for the required vars (typically `NATS_URL` and, for tracing, `OTEL_EXPORTER_OTLP_ENDPOINT`).
