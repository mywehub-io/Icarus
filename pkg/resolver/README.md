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

## `CreateResultStream`

```go
func (s *Service) CreateResultStream(ctx context.Context, small map[string]json.RawMessage,
    streamed map[string]archive.StreamedValue, meta ResultMeta) (*Result, error)
```

`CreateResult` for a document whose large values are files on disk. `small` holds values
already in JSON form; each `StreamedValue` is raw bytes, written as a base64 JSON string.
If the streamed values fit within the inline threshold, the document is built in memory and
passed to `CreateResult`. Otherwise the archive is encoded straight into `UploadStream`, so
the value is never held whole. Either way, path, outcome and stored bytes match `CreateResult`
for the equivalent document. Every key must be a flat node-output key (`<nodeId>-/<path>`).
(`pkg/resolver/stream.go`)

## `LocateEntry`

```go
func (s *Service) LocateEntry(ctx context.Context, mappings []message.FieldMapping,
    cg *ConsumerGraph) (*EntryLocator, bool, error)
```

Reports whether a unit's resolved input would be exactly `{"payload": V}`, with `V` one
archive entry (or `{RelPath: entry}` when the mapping names the entry's parent), and where
that entry sits in its blob. It succeeds only for one data mapping (event mappings are
ignored) to the single destination `/payload`, with no `//`, no `Iterate`, not a plugin-error
section, and a source in a consumer-graph blob file. Anything else returns `false` and the
caller resolves the ordinary way. Stream the entry with `EntryLocator.Open`.
(`pkg/resolver/stream.go`)

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
