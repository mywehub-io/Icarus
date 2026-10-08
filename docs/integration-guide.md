# Integration guide

This guide shows how to wire Icarus into a new Elysium-style plugin service.

## 1. Import and connect

```go
import (
    "github.com/wehubfusion/Icarus/pkg/client"
    "github.com/wehubfusion/Icarus/pkg/runner"
    "github.com/wehubfusion/Icarus/pkg/storage"
    "go.uber.org/zap"
)

logger, _ := zap.NewProduction()

c := client.NewClient(
    "nats://nats:4222",
    "RESULTS_UAT",   // result stream — must match Zeus config
    "result.uat",    // result subject
)
if err := c.Connect(ctx); err != nil {
    logger.Fatal("nats connect failed", zap.Error(err))
}
defer c.Close()
```

## 2. Inject blob storage (required)

```go
blob, err := storage.NewAzureBlobClient(os.Getenv("BLOB_CONNECTION_STRING"), "results", logger)
if err != nil {
    logger.Fatal("blob client failed", zap.Error(err))
}
c.SetBlobStorage(blob)
```

Every result is a blob: `resolver.Service.CreateResult` writes the unit's output document to
storage and `ReportSuccess` publishes the `BlobReference`. There is no inline result and no size
threshold; a result message without a blob reference is refused.

## 3. Implement `Processor`

```go
type MyProcessor struct { /* dependencies */ }

func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    // Resolve the input from the consumer graph and field mappings
    // (resolverSvc.ResolveMappedInputWithConsumerGraph)
    inputBytes, err := resolveInput(ctx, msg)
    if err != nil {
        return message.Message{}, err
    }

    // Do your work: the output is a flat document of "nodeId-/path" keys
    result, err := doWork(ctx, inputBytes)
    if err != nil {
        return message.Message{}, err // triggers ReportError
    }

    // Write the result as a blob and carry its reference
    res, err := resolverSvc.CreateResult(ctx, result, resolver.ResultMeta{
        WorkflowID: msg.Payload.WorkflowID, RunID: msg.Payload.RunID,
        NodeID: msg.Payload.NodeID, ExecutionID: msg.Payload.ExecutionID,
    })
    if err != nil {
        return message.Message{}, err
    }
    out := *msg // copy metadata forward
    out.Payload = &message.Payload{
        BlobReference: res.BlobReference,
        ExecutionID:   msg.Payload.ExecutionID,
        WorkflowID:    msg.Payload.WorkflowID,
        RunID:         msg.Payload.RunID,
        NodeID:        msg.Payload.NodeID,
    }
    return out, nil
}
```

## 4. Create and run the runner

```go
r, err := runner.NewRunner(
    c,
    &MyProcessor{},
    "WORKFLOWS_UAT",     // stream
    "my-service-uat",    // consumer (durable)
    10,                  // batchSize
    2*time.Minute,       // processTimeout per message
    logger,
    nil,                 // tracing config — nil to disable
    &runner.Config{WorkerCount: 4},
    runner.WithProcessFailureObserver(myArgusObserver),
)
if err != nil {
    logger.Fatal("runner create failed", zap.Error(err))
}
defer r.Close()

if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    logger.Error("runner exited with error", zap.Error(err))
}
```

## 5. Register a `ProcessFailureObserver` for Argus

```go
func myArgusObserver(ctx context.Context, msg *message.Message, processErr error) error {
    if msg.Payload == nil { return nil }
    return nodeEndEmitter.EmitNodeEnd(ctx, emitter.NodeEndEmitParams{
        ClientID:   msg.Payload.CorrelationID, // or extract from metadata
        WorkflowID: msg.Payload.WorkflowID,
        RunID:      msg.Payload.RunID,
        NodeID:     msg.Payload.NodeID,
        HasError:   true,
        ErrorMessage: processErr.Error(),
    })
}
```

## 6. Stream and consumer auto-creation

`NewRunner` calls `EnsureStream` and `EnsureConsumer` on startup. The stream and consumer
are created with defaults if they do not exist. In production, pre-create streams with
explicit retention and ack-wait settings to avoid defaults.

## 7. Graceful shutdown

```go
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer cancel()

if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    logger.Error("runner stopped with error", zap.Error(err))
}
```

`r.Run` returns `context.Canceled` on clean shutdown; treat that as success.

## See also

- [pkg/client](../pkg/client/README.md)
- [pkg/runner](../pkg/runner/README.md)
- [pkg/message](../pkg/message/README.md)
- [pkg/storage](../pkg/storage/README.md)
- [testing-guide.md](testing-guide.md)
