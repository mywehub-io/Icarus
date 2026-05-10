# pkg/resolver

Payload resolution helpers: downloads blob-referenced inputs, applies field mappings, and
builds merged inputs for consumer nodes.

## `Service`

```go
func NewService(blobClient storage.BlobStorageClient, maxInlineBytes int) *Service
```

`blobClient` may be nil — in that case only inline resolution is available and any
`BlobReference` input returns an error. `maxInlineBytes <= 0` defaults to
`DefaultMaxInlineBytes` (500 KB = 512 000 bytes).

## `ResolveInput`

```go
func (s *Service) ResolveInput(ctx context.Context, inline []byte, blobRef *message.BlobReference) ([]byte, error)
```

Returns `inline` when non-empty. When `inline` is empty, downloads from `blobRef.URL`.
Returns an error if both are empty or if the download fails.

## `DefaultMaxInlineBytes`

```go
const DefaultMaxInlineBytes = 500 * 1024 // 500 KB
```

Matches the Argus emitter threshold and the NATS message size headroom. Payloads above this
limit must be stored in blob and referenced via `BlobReference`.

## `FieldMappingParams`

```go
type FieldMappingParams struct {
    FieldMappings    []message.FieldMapping
    SourceResults    map[string]*SourceResult
    TriggerData      []byte
    BlobSourceNodeID string
    ConsumerGraph    *ConsumerGraph // optional: multi-blob download
}
```

## `ConsumerGraph`

Tracks which source nodes supply each consumer node and how to merge their outputs.
Build it with `NewConsumerGraph`, then pass it into `FieldMappingParams` for multi-source
resolution. (`pkg/resolver/consumer_graph.go`)

## `ResultMeta`

```go
type ResultMeta struct {
    WorkflowID  string
    RunID       string
    NodeID      string
    ExecutionID string
}
```

Used when constructing blob paths for result uploads — mirrors the path format in
`pkg/storage`.
