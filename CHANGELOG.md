# Changelog

All notable changes to the Icarus SDK are documented here.

## Tagging convention

Each entry is tagged `` `public:` `` or `` `internal:` ``:

- `` `public:` `` — customer- or developer-visible change: new API surface, breaking
  removals, observable behaviour changes. Include in external release notes.
- `` `internal:` `` — infrastructure or implementation detail: dependency bumps,
  internal telemetry, private type additions. Omit from external release notes.

## [Unreleased]

### Fixed

- `public:` **Embedded subflow skip logic**: `shouldSkipNode` (non-iteration path) now
  skips a downstream node when every one of its default-section sources is absent from
  the output store, matching the per-item behaviour of `shouldSkipNodeForItem`
  (`f8bf0ae`). Previously, a node whose sole upstream had itself been skipped would
  run with an empty input map and surface as `status: success` rather than being
  absent. Affects chains such as `SimpleCondition` → event-gated producer → FIELD-only
  downstream node. (`pkg/embedded/runtime/subflow.go`)

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
