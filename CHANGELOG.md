# Changelog

All notable changes to the Icarus SDK are documented here.

## Tagging convention

Each entry is tagged `` `public:` `` or `` `internal:` ``:

- `` `public:` `` — customer- or developer-visible change: new API surface, breaking
  removals, observable behaviour changes. Include in external release notes.
- `` `internal:` `` — infrastructure or implementation detail: dependency bumps,
  internal telemetry, private type additions. Omit from external release notes.

## [Unreleased]

### Changed

- `public:` The runner fetches only as many messages as it has idle workers (capped at
  `batchSize`) instead of consuming into a buffered job queue. `Config.QueueSize` is ignored.
  Messages waiting for a worker stay undelivered, so they no longer use delivery attempts or
  ack deadlines, and other replicas can take them.
- `public:` A transient processing error with delivery attempts left is retried with backoff
  (`NakWithDelay` 5 s, 15 s, 30 s, 60 s) and not reported. The last attempt reports it once,
  not retryable, and terminates the message. `ProcessFailureObserver` runs only for a reported
  failure.
- `public:` `EXECUTION_HEARTBEATS` entries carry a `state` (`running`, `retrying`, `done`) and
  `retry_at`. A retry, a dead pod's unit (no write for 60 s) or a new dispatch takes the claim
  over with a revision-checked update; a delivery that finds the unit running elsewhere is nak'd
  with a 30 s delay; a duplicate of a completed execution is terminated.

### Added

- `public:` `message.ReportErrorOption`, `message.WithAttempt`, `message.FinalAttempt`,
  `message.IsTransientError`, `Message.NakWithDelay` and `ResultMessage.Attempt`.

### Fixed

- `public:` Transient failures were never retried: the failed result reached the result
  consumer on the first attempt, and every redelivery found the unit's claim still held and was
  nak'd at once, using up `MaxDeliver` within milliseconds.

## [0.27.0] — 2026-10-02

Releases 0.22.0 to 0.26.0 have no entries here; see their tags.

### Added

- `public:` `archive.Reader.EntryRange(key)` returns a STORED entry's byte offset and
  length within the blob, so a consumer can stream one value with ranged GETs.
- `public:` `archive.WriteDocument(w, flat, streamed)` writes a document archive to an
  `io.Writer`, encoding `StreamedValue` byte values to base64 JSON strings while copying.
  The output is byte-identical to `Build` for the same data; `Build` now delegates to it.
- `public:` `storage.NewRangeReader` streams a blob byte range as an `io.Reader` in
  sequential chunks (default 8 MiB) with one chunk of read-ahead. A short or failed range,
  or a cancelled context, is an error, never a clean EOF.
- `public:` `resolver.Service.CreateResultStream` is `CreateResult` for documents whose
  large values are on disk: same inline-or-blob decision, path and archive bytes, with the
  blob branch encoded straight into `UploadStream`.
- `public:` `resolver.Service.LocateEntry` reports when a unit's resolved input would be a
  single archive entry placed at `/payload`, and where that entry is, so the unit can
  stream it instead of receiving it resolved. It declines every other mapping shape.
- `public:` `resolver.Service.MaxInlineBytes` and `RangeSource` accessors.

### Changed

- `internal:` `CreateResult`'s blob path and metadata construction moved into a helper
  shared with `CreateResultStream`. The path and metadata are unchanged.

## [0.21.0] — 2026-08-10

### Fixed

- `public:` Runner Consume supervision now restarts when `ConsumeContext.Closed`
  fires without an ErrHandler callback (e.g. consumer deleted), matching the old
  pull loop's continuous retry behaviour. Latest consume errors are retained so a
  fatal error is not dropped after a transient heartbeat miss.
- `public:` Consume ErrHandler no longer treats `!IsConnected` as fatal — during
  nats auto-reconnect, heartbeat misses are left to the library; only
  `isFatalConsumeError` (e.g. connection closed, consumer deleted) tears down Consume.
- `internal:` `Client.Connect` no longer nils `Messages` mid-reconnect; the new
  service is swapped in only after it is ready so concurrent `Report*` / `GetConsumer`
  calls cannot hit a nil pointer (stale service may still return transport errors).

### Changed (breaking)

- `public:` **Migration to the new `nats.go/jetstream` API**: `pkg/client`,
  `pkg/message`, and `pkg/runner` now use the modern `nats.go/jetstream` package
  instead of the legacy `nats.JetStreamContext` pull API.
  - `Client.JetStream()` returns `jetstream.JetStream` (was `nats.JetStreamContext`);
    derive a legacy context from `Client.Connection().JetStream()` if still needed.
  - `Message` wraps `jetstream.Msg`; `GetNATSMsg()` → `GetJetStreamMsg()`; new
    `FromJetStreamMsg` constructor.
  - `ReportSuccess` / `ReportError` accept `jetstream.Msg` instead of `*nats.Msg`.
  - `EnsureStream` / `EnsureConsumer` take `context.Context`; still create-only
    (existing streams/durables are never modified).
  - New `MessageService.GetConsumer(ctx, stream, consumer)` returning
    `jetstream.Consumer`.
  - Runner replaces the per-batch `PullSubscribe`/`Fetch` loop with a supervised
    `consumer.Consume(cb, jetstream.PullMaxMessages(batchSize))` loop; worker pool,
    backpressure, ack/nak classification, metadata keys, and shutdown-drain
    semantics are unchanged. Reconnects during consumption are healed natively by
    the `jetstream` library.
  - `NewClientWithJSContext` takes the new `message.JSContext` test-seam interface.
  - Stream/consumer configurations, result subject composition, and the
    `Message`/`ResultMessage` JSON contracts are byte-identical to v0.20.x.
  See [docs/upgrade-guide.md](docs/upgrade-guide.md) for the migration guide.

### Removed (breaking)

- `public:` `MessageService.Publish` and `MessageService.PullMessages` — publishing
  uses `Client.JetStream().Publish`; consumption uses `GetConsumer` + `Consume` or
  `pkg/runner`.
- `public:` `NATSMsg` wrapper and the `pkg/message` middleware framework
  (`Handler`, `HandlerFunc`, `MiddlewareFunc`, chain helpers).
- `public:` `Client.Ping`, `Client.Stats`, and `ConnectionStats`.
- `internal:` `internal/nats.WaitForConnection`.

### Kept for compatibility

- `public:` `FromNATSMsg` and `ResultMessageFromNATSMsg` remain for plain `*nats.Msg`
  consumers (used by Zeus).

## [0.10.0] — 2026-05-07

### Changed

- `public:` **Embedded node error handling**: failures in individual nodes within an
  execution unit now propagate correctly through the runner's `ProcessFailureObserver`
  path. The embedded `NodeFailureError` type carries `NodeID`, `PluginType`, and a
  `Permanent bool` flag. (`pkg/embedded/runtime/node_failure_error.go`)

## [0.9.0] — 2026-05-03

### Changed

- `public:` **CSV column ordering**: columns are now ordered by the `position` field in
  the schema definition, giving callers explicit control over column order in CSV output.
- `internal:` Argus dependency updated to `v0.3.4`.

## [0.8.0] — 2026-04-23

### Added

- `public:` `ProcessOptions.CodeSeverityOverrides map[string]Severity` — override
  per-code severity for any format (`json`, `csv`, `hl7`). Use `SeverityDrop` to
  suppress a code entirely.
- `public:` HL7 processor: specific codes now default to WARNING instead of ERROR where
  appropriate (e.g. optional-segment absence, truncation markers, version mismatches).

### Removed (breaking)

- `public:` `ValidationMode` / `StrictValidation` from `ProcessOptions` — replaced by
  `CodeSeverityOverrides`. Callers that relied on `ValidationMode: strict` must switch
  to overriding specific codes to `SeverityError`.

## [0.7.0] — 2026-04-15

### Added

- `internal:` CEL engine updated to `google/cel-go v0.28.0`.
- `public:` `EvalError.RuleSeverity` field: runtime errors during rule evaluation now
  carry the rule-configured severity so they can be bucketed correctly.
- `public:` `ManualInput` embedded processor: supports `"event"` data type; JSON values
  are unmarshalled from the raw literal without double-encoding.
  (`pkg/embedded/processors/`)

### Fixed

- `internal:` HL7 header validator: nil-safe version comparison and truncation marker
  handling.
- `public:` `SimpleCondition` default assertion failure message now includes `ErrorPath`
  for actionable diagnostics.

## [0.6.0] — 2026-03-31

### Added

- `internal:` Runner logs structured INFO at message pull, dispatch, and completion:
  `workflow_id`, `run_id`, `execution_id`, `node_id`, `jetstream_deliver_count`,
  `queue_wait_ms` (time between `PullMessages` and `processMessage`).
- `internal:` `MetaIcarusEnqueueUnixMs` metadata key set by the runner when a message
  enters the job queue; enables queue-wait telemetry.
  (`pkg/message/message.go:20`)

## [0.5.0] — 2026-02-16

### Added

- `internal:` Argus SDK integration:
  `pkg/embedded/runtime/argus_lifecycle_emitter.go` emits `node.started` and
  `node.ended` Argus events for embedded nodes.
- `internal:` Argus dependency (`github.com/wehubfusion/Argus`) added to `go.mod`.

### Changed

- `public:` `ProcessFailureObserver` now receives the original `*message.Message` (not
  just the error) so observers can include node IDs and execution IDs in Argus emission.

## [0.4.0] — 2026-02-05

### Added

- `public:` `SourceSectionId` on `FieldMapping`: `"default"` selects normal output;
  `"pluginError"` selects the error-event path for downstream error handlers.
- `public:` `PriorUnitOutputs` support in embedded processor for chaining multiple
  execution units.
- `internal:` Nested array iteration: `ArrayPathSegment` and `IterationStack` for
  deeply nested field-mapping traversal. (`pkg/embedded/runtime/subflow.go`)

### Changed

- `public:` `FieldMapping.IsEventTrigger bool` added for conditional-execution event
  triggers.

## [0.3.0] — 2026-01-07

### Added

- `public:` `ConsumerGraph` in `pkg/resolver` for multi-source blob download when a
  node consumes outputs from multiple upstream nodes.
- `public:` JSON processor: auto-unwrapping of arrays at root level; array handling in
  JS runner.

## [0.2.0] — 2025-12-xx

### Added

- `public:` `pkg/runner`: `ProcessFailureObserver` callback; `Config` with `WorkerCount`
  and `QueueSize`; env-driven concurrency via `ICARUS_RUNNER_WORKERS` and
  `ICARUS_RUNNER_WORKER_MULTIPLIER`.
- `public:` `pkg/storage`: `DownloadFromURL` for cross-container blob downloads.
- `internal:` OpenTelemetry tracing integrated into runner via `TracingConfig`.

## [0.1.0] — initial release

- `public:` `pkg/client`, `pkg/runner`, `pkg/message`, `pkg/resolver`, `pkg/storage`,
  `pkg/errors`
- `public:` `pkg/schema` (JSON, CSV, HL7), `pkg/cel`, `pkg/embedded`
- `public:` `tests/` with mock JetStream and integration test helpers
