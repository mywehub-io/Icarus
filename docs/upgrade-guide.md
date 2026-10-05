# Upgrade guide

## Upgrading to v0.28.1

- **Unclassified errors fail at once.** Only an error marked or recognised as transient is
  retried: an `*AppError` of type `Internal` (anywhere in the chain), an error implementing
  `message.TransientClassifier`, or a failure reaching a dependency (network timeout, refused or
  reset connection, DNS failure, gRPC `Unavailable`/`ResourceExhausted`/`DeadlineExceeded`, Azure
  408/429/5xx). A plain `fmt.Errorf` or `errors.New` is permanent, as every failure was before
  v0.28.0. Wrap a cause with `%w`, not `%v`, so a dependency failure stays recognisable.
- **A unit that outruns `processTimeout` is reported, not retried.**
- Error text and the reported error type are unchanged.

### `pkg/embedded/processors/jsrunner`

- **The script timeout applies, and defaults to 1 minute.** A JS Runner script now fails with a
  `TimeoutError` after its `timeout`, and is interrupted. Since v0.6.0 the timeout was multiplied
  by a millisecond a second time, so a script ran until the runner's `processTimeout` instead,
  whatever its `timeout` said. The default was 5s; it is now 1 minute. A script that takes longer
  than its timeout, which used to succeed, now fails: set `timeout` (a duration string, such as
  `"2m"`) on that node's config.
- **One-line statements run.** `throw new Error("x");`, `while (…) {…}` and similar one-line
  statements without `return` used to be auto-returned as an expression and fail with a
  `SyntaxError`. They now run as written. One-line expressions are still returned.

## Upgrading to v0.28.0

No code change is required, but three runner behaviours change. Roll out Icarus users
(e.g. Elysium) before or after the result consumer (Zeus) in any order: each side tolerates the
other's old behaviour.

### `pkg/runner`

- **No prefetching.** The runner fetches only as many messages as it has idle workers
  (capped at `batchSize`) instead of running `Consume` with a buffered job queue.
  `Config.QueueSize` is ignored and logged once if set. Work waiting for a worker now stays
  undelivered in the stream, so it no longer uses delivery attempts or ack deadlines while it
  waits, and spreads across replicas by free capacity.
- **Transient failures are retried before they are reported.** A transient error with attempts
  left is nak'd with a backoff delay (5 s, 15 s, 30 s, 60 s) and nothing is published. Only
  the last attempt publishes the failure, with `Retryable: false` and its attempt number, and
  terminates the message. `ProcessFailureObserver` runs once, for that report. A result that
  cannot be published is retried the same way, and a unit cancelled by shutdown is handed back
  unreported, so a result consumer sees at most one failed result per execution.
- **The execution claim hands over.** The `EXECUTION_HEARTBEATS` entry now carries a `state`
  (`running`, `retrying`, `done`). A retry, a dead pod's unit (no write for 60 s) or a new
  dispatch of the node takes the claim over; a delivery that finds the unit running elsewhere is
  nak'd with a 30 s delay instead of immediately, and a duplicate of a completed execution is
  terminated. Before, every redelivery within 90 s of the last write was nak'd at once, which
  used up all delivery attempts in milliseconds.

### `pkg/message`

- `ReportError` takes optional `ReportErrorOption`s: `WithAttempt(n)` and `FinalAttempt()`.
- `ReportSuccess` no longer reports a failure or naks when it cannot publish the result. Check
  the returned error with `errors.Is`: `ErrResultNotPublished` means the message is unsettled
  and yours to retry or report; `ErrAckAfterPublish` means the result was published.
  Existing calls compile and behave as before.
- New: `Message.NakWithDelay`, `IsTransientError`, and `ResultMessage.Attempt`
  (`attempt`, omitted when 0).

## Upgrading to v0.27.0

No action required. v0.27.0 only adds streaming primitives (`archive.Reader.EntryRange`,
`archive.WriteDocument`, `storage.NewRangeReader`, `resolver.Service.CreateResultStream`,
`resolver.Service.LocateEntry`). The archive format is unchanged: archives written by
`CreateResultStream` are byte-identical to those written by `CreateResult`, so readers on
v0.26.0 read them without change and services can upgrade in any order.

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
