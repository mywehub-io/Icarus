# Changelog

All notable changes to the platform module (`github.com/wehubfusion/Icarus/platform`) are
documented here. Releases are tagged `platform/vX.Y.Z`.

## [Unreleased]

### Added

- `audit`: `Emitter.Publish` publishes one event and returns the stream's answer, for a caller
  that keeps its own durable copy until it is acknowledged, such as a transactional outbox. It
  queues and retries nothing, counts no drops, and sends no producer sequence, so a retry never
  shows as a gap. It returns `ErrEventTooLarge` for an event over the size cap and
  `ErrEmitterInactive` on a no-op, nil or closed emitter.

## [0.1.0] — 2026-11-25

### First version

- `audit`: `Emitter` publishes audit events on `<prefix>.<service>` with the event id as
  `Nats-Msg-Id`, enforces the event contract's field limits and its 1 KiB size cap, and fails
  open through a bounded retry queue. It emits `producer.started`, `producer.heartbeat` and
  `producer.stopped`, increases the producer sequence for every event built so a drop shows as a
  gap, and reports `audit.events.published`, `audit.events.dropped`, `audit.publish.retries` and
  `audit.queue.depth` through OTel. It never creates or updates the stream.
- `audit`: the request-scoped `Collector`, with `RecordDecision`, `SetJobID`, `SetResource`,
  `SetAttr` and `SetReason`.
- `audit/grpcaudit`: `UnaryServerInterceptor`, which records denials at any hop and writes and
  sensitive reads at an entry; `EntryMarker` and `HeaderMatcher` for the gRPC-Gateway;
  `Classify` from the `google.api.http` binding with per-service overrides.
- `audit/grpcaudit/grpcaudittest`: `AssertAllClassified`.
- `audit/httpaudit`: the same rules as `net/http` middleware.
- `jobs`: `Coordinator.Begin` calls Iris `CreateJobWithLease` with a stable request key, retries
  once on `Unavailable` or `DeadlineExceeded`, fails open unless asked to fail closed, and returns
  `ErrResourceLocked` and `ErrAlreadyCompleted`. `Op` publishes running, progress and terminal
  updates; `PublishStatus` and `Renew` serve Temporal activities; `KeepAlive` renews a lease in
  the background. It never creates or updates the JOBS stream.
- `jobs/iriswire`: the Iris wire messages under package `wehub.platform.iriswire.v1`.
