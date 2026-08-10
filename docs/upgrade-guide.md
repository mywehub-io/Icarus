# Upgrade guide

## Upgrading to v0.21.0

### Migration to the new `nats.go/jetstream` API (breaking)

v0.21.0 replaces the legacy `nats.JetStreamContext` pull API with the new
`nats.go/jetstream` package throughout `pkg/client`, `pkg/message`, and `pkg/runner`.
Stream/consumer configurations, result subject composition, metadata keys, and
ack/nak semantics are unchanged; only the API surface changes.

**`pkg/client`:**

- `Client.JetStream()` now returns `jetstream.JetStream` (was `nats.JetStreamContext`).
  For code that still needs the legacy context (e.g. Argus `NewObserver`), derive it
  from the raw connection: `client.Connection().JetStream()`.
- `NewClientWithJSContext` now takes a `message.JSContext` built around the new API.
- Removed: `Client.Ping`, `Client.Stats`, `ConnectionStats`.

**`pkg/message`:**

- `Message` now wraps a `jetstream.Msg`; `GetNATSMsg()` is replaced by
  `GetJetStreamMsg()`. New constructor: `FromJetStreamMsg(jsMsg)`.
- `ReportSuccess` / `ReportError` take `jetstream.Msg` instead of `*nats.Msg`.
- `EnsureStream` / `EnsureConsumer` now take a `context.Context` as first argument.
  Both remain create-only: existing streams and durables are never modified.
- New: `GetConsumer(ctx, stream, consumer)` returns a `jetstream.Consumer` for use
  with `Consume`.
- Removed: `MessageService.Publish`, `MessageService.PullMessages`, the `NATSMsg`
  wrapper, and the message middleware framework (`Handler`, `MiddlewareFunc`, etc.).
- Kept for Zeus and other plain-NATS consumers: `FromNATSMsg`, `ResultMessageFromNATSMsg`,
  and the `Message`/`ResultMessage` JSON contracts.

**`pkg/runner`:**

- The internal pull loop (`PullMessages` + idle backoff) is replaced by a supervised
  `consumer.Consume(cb, jetstream.PullMaxMessages(batchSize))` loop. The callback
  dispatches into the same bounded worker pool, so effective concurrency and
  backpressure are unchanged.
- `NewRunner` signature, worker pool config, `processTimeout`, functional options,
  metadata keys, and shutdown-drain semantics are unchanged.
- Reconnection during consumption is handled natively by the `jetstream` library;
  `EnsureConnected` is still used on fatal consume errors and result-publish failures.

**Migration example:**

```go
// Before
js := c.JetStream()                       // nats.JetStreamContext
msgs, _ := c.Messages.PullMessages(ctx, stream, consumer, 10)
c.Messages.ReportSuccess(ctx, result, msg.GetNATSMsg())

// After
js := c.JetStream()                       // jetstream.JetStream
consumer, _ := c.Messages.GetConsumer(ctx, stream, consumerName)
cc, _ := consumer.Consume(handler)        // or use pkg/runner
c.Messages.ReportSuccess(ctx, result, msg.GetJetStreamMsg())
```

## Upgrading to v0.8.0

### `ValidationMode` / `StrictValidation` removed

v0.8.0 removes `ProcessOptions.ValidationMode` and `ProcessOptions.StrictValidation`. These
fields no longer exist; code that sets them will fail to compile.

**Migration:** Use `CodeSeverityOverrides` instead.

Before:

```go
result, err := engine.ProcessWithSchema(input, schema, schema.ProcessOptions{
    StrictValidation: true,
})
```

After:

```go
result, err := engine.ProcessWithSchema(input, schema, schema.ProcessOptions{
    // Upgrade all warnings to errors for strict behaviour:
    CodeSeverityOverrides: map[string]schema.Severity{
        json.CodeUnknownField:       schema.SeverityError,
        json.CodeTypeMismatch:       schema.SeverityError,
    },
    CollectAllErrors: true,
})
```

For HL7, promote codes that were previously WARNING to ERROR:

```go
CodeSeverityOverrides: map[string]schema.Severity{
    hl7.CodeMissingOptionalSegment: schema.SeverityError,
}
```

Use `schema.SeverityDrop` to completely suppress a code that was previously causing failures
in your pipeline.

## Upgrading to v0.5.0

### Argus dependency added

v0.5.0 adds `github.com/wehubfusion/Argus` as a dependency for embedded node lifecycle
observation. If your `go.sum` pins a different version, run:

```bash
go get github.com/wehubfusion/Argus@v0.3.6
go mod tidy
```

### `ProcessFailureObserver` signature change

In v0.5.0, `ProcessFailureObserver` receives `*message.Message` as its second argument
(previously only `error`). Update all registered observers:

Before:

```go
runner.WithProcessFailureObserver(func(ctx context.Context, err error) error { ... })
```

After:

```go
runner.WithProcessFailureObserver(func(ctx context.Context, msg *message.Message, err error) error { ... })
```

## Upgrading from pre-v0.4.0

### `SourceSectionId` on `FieldMapping`

v0.4.0 adds `SourceSectionId` and `IsEventTrigger` to `FieldMapping`. These fields are
additive (zero value is backward-compatible). No action required unless you serialise
`FieldMapping` structs directly.

### Nested array iteration

`SubflowProcessor`, `ArrayPathSegment`, and `IterationStack` replace the flat iteration
model. If you have custom iteration logic in an embedded node processor, review the new
`pkg/embedded/runtime/subflow.go` for the updated traversal API.
