# pkg/message

Core message types, metadata constants, and the `MessageService` used by `pkg/client`.

## `Message`

The central envelope passed through the Icarus processing pipeline.

| Field | Type | Description |
|---|---|---|
| `ID` | `string` | Unique message ID |
| `CorrelationID` | `string` | Links related messages (auto-set from `workflowID-runID` if missing) |
| `CreatedAt` | `string` | ISO-8601 creation timestamp |
| `Workflow` | `*Workflow` | `WorkflowID`, `RunID` |
| `Node` | `*Node` | `NodeID`, `Configuration` (raw JSON) |
| `Payload` | `*Payload` | Input data: inline string or blob reference + execution context fields |
| `Output` | `*Output` | `DestinationType` for result routing |
| `Metadata` | `map[string]string` | Arbitrary key-value pairs; JetStream diagnostics keys set here |

## `Payload`

```go
type Payload struct {
    InlineData    *string        // small payloads (<1.5 MB)
    BlobReference *BlobReference // large payloads: URL + SizeBytes
    FieldMappings []FieldMapping // resolver inputs
    CorrelationID string
    ExecutionID   string
    WorkflowID    string
    RunID         string
    NodeID        string
}
```

`GetInlineData()` returns `""` if `InlineData` is nil. `HasInlineData()` is nil-safe.

## `BlobReference`

```go
type BlobReference struct {
    URL       string // direct Azure Blob URL
    SizeBytes int    // original data size in bytes
}
```

## `FieldMapping`

Describes how data flows from a source node to a destination node.

| Field | Description |
|---|---|
| `SourceNodeID` | Upstream node producing the data |
| `SourceEndpoint` | Output endpoint name on the source node |
| `SourceSectionId` | `"default"` or `"pluginError"` for error-path handling |
| `DestinationEndpoints` | Input endpoint names on the destination node |
| `DataType` | Expected data type |
| `Iterate` | Whether to iterate over array items |
| `IsEventTrigger` | `true` for conditional-execution event triggers |

## Metadata constants

JetStream diagnostics populated by `message.FromJetStreamMsg` (called by the runner's
consume callback):

| Constant | Value | Description |
|---|---|---|
| `MetaJetStreamDeliverCount` | `"jetstream_deliver_count"` | Redelivery count from JetStream consumer |
| `MetaJetStreamStreamSeq` | `"jetstream_stream_seq"` | Stream sequence number |
| `MetaJetStreamConsumerSeq` | `"jetstream_consumer_seq"` | Consumer sequence number |
| `MetaJetStreamNumPending` | `"jetstream_num_pending"` | Messages waiting behind this one |
| `MetaIcarusEnqueueUnixMs` | `"icarus_enqueue_unix_ms"` | Unix ms when message entered the runner job queue |

`MetaIcarusEnqueueUnixMs` is set by the runner when the message enters the job queue.
Use it to calculate queue wait time: `time.Now().UnixMilli() - enqueueMs`.

## `MessageService`

Accessed via `client.Messages`. Key methods (all built on the new `nats.go/jetstream` API):

| Method | Description |
|---|---|
| `GetConsumer(ctx, stream, consumer)` | Resolve a `jetstream.Consumer` handle for `Consume` |
| `ReportSuccess(ctx, result, originalMsg)` | Publish result to the configured result subject, then ack (`originalMsg` is a `jetstream.Msg`) |
| `ReportError(ctx, executionID, workflowID, runID, correlationID, err, originalMsg)` | Publish error result; nak on transient errors, ack on permanent |
| `PublishResult(ctx, result)` | Publish a result message with retry/backoff |
| `EnsureStream(ctx, stream)` | Create stream if it does not exist (never updates existing) |
| `EnsureConsumer(ctx, stream, consumer, filterSubject)` | Create pull consumer if missing; optional `FilterSubject` for tenant/default routing |
| `SetBlobStorage(client)` | Inject blob client for large result uploads |

Message conversion helpers: `FromJetStreamMsg(jsMsg)` builds a `Message` from a consumed
`jetstream.Msg` (attaches ack handle + JetStream metadata); `FromNATSMsg` /
`ResultMessageFromNATSMsg` remain for plain `*nats.Msg` consumers (used by Zeus).
