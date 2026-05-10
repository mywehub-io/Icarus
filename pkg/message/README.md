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

JetStream pull diagnostics populated by `MessageService.PullMessages`:

| Constant | Value | Description |
|---|---|---|
| `MetaJetStreamDeliverCount` | `"jetstream_deliver_count"` | Redelivery count from JetStream consumer |
| `MetaJetStreamStreamSeq` | `"jetstream_stream_seq"` | Stream sequence number |
| `MetaJetStreamConsumerSeq` | `"jetstream_consumer_seq"` | Consumer sequence number |
| `MetaJetStreamNumPending` | `"jetstream_num_pending"` | Messages waiting behind this one |
| `MetaIcarusEnqueueUnixMs` | `"icarus_enqueue_unix_ms"` | Unix ms when message entered the runner job queue |

`MetaIcarusEnqueueUnixMs` is set by the runner, not by `PullMessages`. Use it to calculate
queue wait time: `time.Now().UnixMilli() - enqueueMs`.

## `MessageService`

Accessed via `client.Messages`. Key methods:

| Method | Description |
|---|---|
| `PullMessages(ctx, stream, consumer, batchSize)` | Pull up to `batchSize` messages from a JetStream pull consumer |
| `ReportSuccess(ctx, result, originalMsg)` | Ack + publish result to the configured result subject |
| `ReportError(ctx, executionID, workflowID, runID, correlationID, err, originalMsg)` | Ack + publish error result |
| `EnsureStream(stream)` | Create stream if it does not exist |
| `EnsureConsumer(stream, consumer)` | Create pull consumer if it does not exist |
| `SetBlobStorage(client)` | Inject blob client for large result uploads |
