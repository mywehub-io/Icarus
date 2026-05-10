# pkg/storage

Azure Blob Storage client for large Icarus result payloads.

## `BlobStorageClient` interface

```go
type BlobStorageClient interface {
    UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error)
    DownloadResult(ctx context.Context, blobURL string) ([]byte, error)
    DownloadFromURL(ctx context.Context, blobURL string) ([]byte, error)
}
```

| Method | When to use |
|---|---|
| `UploadResult` | Upload a node result to the configured container |
| `DownloadResult` | Download a blob from a URL within the configured container |
| `DownloadFromURL` | Download a blob from any URL within the same storage account but a different container (e.g. blobs produced by Elysium) |

Use `DownloadFromURL` when the blob URL points to a container other than the one the client
was created with. The method derives the container name from the URL and uses the same
shared-key credentials.

## `AzureBlobClient`

```go
func NewAzureBlobClient(connectionString, containerName string, logger *zap.Logger) (*AzureBlobClient, error)
```

Shared-key credential client. Supports `http://` endpoints (Azurite) for local development.
The container is created lazily on the first upload.

## Blob path convention

Icarus stores results under:

```
results/{executionID}/{nodeID}
```

The exact path is built by the `MessageService` or the calling service using `ResultMeta`
from `pkg/resolver`.

## Injecting into the client

```go
blob, err := storage.NewAzureBlobClient(connStr, "results", logger)
c.SetBlobStorage(blob)
```

This propagates the client into `MessageService` for large result uploads. Payloads above
the inline threshold (500 KB per `pkg/resolver.DefaultMaxInlineBytes`) are automatically
uploaded and replaced with a `BlobReference` in the result message.
