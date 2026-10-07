package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// defaultContentType is what a blob whose path says nothing about its format gets.
const defaultContentType = "application/json"

func contentTypeOrDefault(ct string) string {
	if strings.TrimSpace(ct) == "" {
		return defaultContentType
	}
	return ct
}

// contentTypeForPath derives the stored content type from the blob path's extension.
//
// UploadResult takes no content type and cannot be given one without changing an interface
// Zeus also implements against, so the path is what it has to go on. That is enough: every
// result blob is now an archive and ends ".zip", while the consumer-graph offload — the one
// caller of UploadResult outside this package — writes ".json". Without this a container
// full of ZIP files would report itself as JSON to anything reading the header, which is
// exactly the kind of quiet mislabelling that sends a later reader down the wrong path.
func contentTypeForPath(blobPath string) string {
	switch strings.ToLower(path.Ext(blobPath)) {
	case ".zip":
		return "application/zip"
	case ".json":
		return "application/json"
	default:
		return defaultContentType
	}
}

func metadataPointers(metadata map[string]string) map[string]*string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]*string, len(metadata))
	for k, v := range metadata {
		out[k] = to.Ptr(v)
	}
	return out
}

// countingReader tallies bytes passing through a stream so a streamed upload can
// report a size it never held in memory.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// BlobStorageClient interface for storing large workflow results
type BlobStorageClient interface {
	UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error)
	DownloadResult(ctx context.Context, blobURL string) ([]byte, error)
	// DownloadFromURL downloads a blob from any URL within the same storage account
	// using the shared-key credentials. Unlike DownloadResult, the container name is
	// derived from the URL, not from the configured container. This is the correct method
	// to use when downloading blobs produced by other services (e.g. Elysium) that share
	// the same storage account but write to a different container.
	DownloadFromURL(ctx context.Context, blobURL string) ([]byte, error)

	// DownloadRange returns the bytes [offset, offset+count) of the blob at blobURL.
	// A count of zero reads from offset to the end of the blob.
	//
	// This is the primitive every partial read rests on. Paired with NewBlobReaderAt it
	// lets archive/zip locate and serve individual entries, so a consumer can move only
	// the fields it was mapped to rather than the whole payload.
	DownloadRange(ctx context.Context, blobURL string, offset, count int64) ([]byte, error)

	// UploadStream uploads from body without materialising it, for writers that produce
	// output incrementally. An empty contentType defaults to application/json, matching
	// UploadResult.
	UploadStream(ctx context.Context, blobPath string, body io.Reader, contentType string, metadata map[string]string) (string, error)

	// BlobSize returns the blob's exact length without transferring its contents.
	//
	// Opening an archive over ranged reads needs the total size up front. The result
	// message's BlobReference carries one, but the resolver reads from the consumer
	// graph, whose RequiredBlobFile records only a URL and the nodes inside it — so on
	// this path the size has to be asked for. It is one small request per source file,
	// not per key, which is what keeps it worth paying against downloading the whole
	// object.
	BlobSize(ctx context.Context, blobURL string) (int64, error)
}

// AzureBlobClient implements BlobStorageClient for Azure Blob Storage using shared keys
// This implementation is intentionally close to the lightweight blob client used by the
// plugin backend so we can seamlessly target local Azurite instances over HTTP.
type AzureBlobClient struct {
	client        *azblob.Client
	serviceURL    string
	containerName string
	credential    *azblob.SharedKeyCredential
	logger        *zap.Logger

	// initMu guards containerInit. One client is shared across concurrently executing
	// units, so the unsynchronised check-and-set this replaced was a genuine data race
	// on every upload, not merely a theoretical one.
	initMu        sync.Mutex
	containerInit bool
}

// NewAzureBlobClient creates a new Azure Blob storage client from a standard connection string.
func NewAzureBlobClient(connectionString, containerName string, logger *zap.Logger) (*AzureBlobClient, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if connectionString == "" {
		return nil, fmt.Errorf("connection string is required")
	}
	if containerName == "" {
		return nil, fmt.Errorf("container name is required")
	}

	params := parseConnectionString(connectionString)
	accountName := params["AccountName"]
	accountKey := params["AccountKey"]
	serviceURL := params["BlobEndpoint"]
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("account name and key are required in the connection string")
	}
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
		return nil, fmt.Errorf("failed to create shared key credential: %w", err)
		}

	var clientOpts *azblob.ClientOptions
	if strings.HasPrefix(strings.ToLower(serviceURL), "http://") {
		clientOpts = &azblob.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				InsecureAllowCredentialWithHTTP: true,
			},
	}
	}

	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob client: %w", err)
	}

	return &AzureBlobClient{
		client:        client,
		serviceURL:    strings.TrimRight(serviceURL, "/"),
		containerName: containerName,
		credential:    credential,
		logger:        logger,
	}, nil
}

// UploadResult uploads data to the configured container.
func (a *AzureBlobClient) UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error) {
	ctx, op := a.startOp(ctx, spanBlobPut, logInfo, attribute.String("blob.path", blobPath))
	op.bytes(len(data))

	if err := a.ensureContainer(ctx); err != nil {
		op.finish(err)
		return "", err
	}

	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
	blobClient := containerClient.NewBlockBlobClient(blobPath)

	_, err := blobClient.UploadBuffer(ctx, data, &azblob.UploadBufferOptions{
		Metadata: metadataPointers(metadata),
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr(contentTypeForPath(blobPath)),
		},
	})
	if err != nil {
		op.finish(err)
		return "", fmt.Errorf("blob upload failed: %w", err)
	}

	op.finish(nil)
	return blobClient.URL(), nil
}

// UploadStream uploads from body without buffering the whole payload in memory.
//
// UploadBuffer requires the caller to hold every byte at once, which is the write-side
// half of the memory problem this package's ranged reads address on the read side.
func (a *AzureBlobClient) UploadStream(ctx context.Context, blobPath string, body io.Reader, contentType string, metadata map[string]string) (string, error) {
	if body == nil {
		return "", fmt.Errorf("body is required")
	}

	contentType = contentTypeOrDefault(contentType)
	ctx, op := a.startOp(ctx, spanBlobPut, logInfo,
		attribute.String("blob.path", blobPath),
		attribute.String("blob.content_type", contentType),
		attribute.Bool("blob.streamed", true),
	)

	if err := a.ensureContainer(ctx); err != nil {
		op.finish(err)
		return "", err
	}

	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
	blobClient := containerClient.NewBlockBlobClient(blobPath)

	// Count what actually went to the wire. Without this the span for a streamed
	// upload would carry no size at all, since the caller never knew one.
	counter := &countingReader{r: body}

	_, err := blobClient.UploadStream(ctx, counter, &azblob.UploadStreamOptions{
		Metadata: metadataPointers(metadata),
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr(contentType),
		},
	})
	op.bytes(int(counter.n))
	if err != nil {
		op.finish(err)
		return "", fmt.Errorf("blob stream upload failed: %w", err)
	}

	op.finish(nil)
	return blobClient.URL(), nil
}

// DownloadRange returns the bytes [offset, offset+count) of the blob at blobURL.
// A count of zero reads from offset to the end of the blob.
func (a *AzureBlobClient) DownloadRange(ctx context.Context, blobURL string, offset, count int64) ([]byte, error) {
	if blobURL == "" {
		return nil, fmt.Errorf("blob URL is required")
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset must not be negative, got %d", offset)
	}
	if count < 0 {
		return nil, fmt.Errorf("count must not be negative, got %d", count)
	}

	ctx, op := a.startOp(ctx, spanBlobGetRange, logDebug,
		attribute.Int64("blob.range_offset", offset),
		attribute.Int64("blob.range_count", count),
	)

	blobClient, err := a.blobClientForURL(blobURL)
	if err != nil {
		op.finish(err)
		return nil, err
	}

	resp, err := blobClient.DownloadStream(ctx, &azblob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: offset, Count: count},
	})
	if err != nil {
		op.finish(err)
		return nil, fmt.Errorf("failed to download blob range: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		op.finish(err)
		return nil, fmt.Errorf("failed to read blob range: %w", err)
	}

	op.bytes(len(data))
	op.finish(nil)
	return data, nil
}

// DownloadResult downloads blob contents using the shared-key client.
func (a *AzureBlobClient) DownloadResult(ctx context.Context, reference string) ([]byte, error) {
	ctx, op := a.startOp(ctx, spanBlobGet, logInfo, attribute.Bool("blob.whole_object", true))

	blobPath, err := a.extractBlobPath(reference)
	if err != nil {
		op.finish(err)
		return nil, err
	}
	op.attr(attribute.String("blob.path", blobPath))

	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
	blobClient := containerClient.NewBlobClient(blobPath)

	data, err := downloadAll(ctx, blobClient)
	if err != nil {
		op.finish(err)
		return nil, err
	}

	op.bytes(len(data))
	op.finish(nil)
	return data, nil
}

// DownloadFromURL downloads a blob identified by its full URL using the shared-key
// credential. It creates a blob-level client directly from the URL so it works with
// any container in the same storage account, not just the configured container.
func (a *AzureBlobClient) DownloadFromURL(ctx context.Context, blobURL string) ([]byte, error) {
	if blobURL == "" {
		return nil, fmt.Errorf("blob URL is required")
	}

	ctx, op := a.startOp(ctx, spanBlobGet, logInfo, attribute.Bool("blob.whole_object", true))

	blobClient, err := a.blobClientForURL(blobURL)
	if err != nil {
		op.finish(err)
		return nil, err
	}

	data, err := downloadAll(ctx, blobClient)
	if err != nil {
		op.finish(err)
		return nil, err
	}

	op.bytes(len(data))
	op.finish(nil)
	return data, nil
}

// BlobSize returns the blob's exact length via a properties request, transferring none
// of its contents.
func (a *AzureBlobClient) BlobSize(ctx context.Context, blobURL string) (int64, error) {
	if blobURL == "" {
		return 0, fmt.Errorf("blob URL is required")
	}

	ctx, op := a.startOp(ctx, spanBlobProperties, logDebug)

	blobClient, err := a.blobClientForURL(blobURL)
	if err != nil {
		op.finish(err)
		return 0, err
	}

	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		op.finish(err)
		return 0, fmt.Errorf("failed to read blob properties: %w", err)
	}
	if props.ContentLength == nil {
		err := fmt.Errorf("blob properties carried no content length")
		op.finish(err)
		return 0, err
	}

	op.attr(attribute.Int64("blob.size_bytes", *props.ContentLength))
	op.finish(nil)
	return *props.ContentLength, nil
}

// blobClientForURL builds a blob-level client for any URL in the same storage account.
// Shared by DownloadFromURL and DownloadRange so the HTTP/Azurite carve-out lives once.
func (a *AzureBlobClient) blobClientForURL(blobURL string) (*blob.Client, error) {
	var clientOpts *blob.ClientOptions
	if strings.HasPrefix(strings.ToLower(a.serviceURL), "http://") {
		clientOpts = &blob.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				InsecureAllowCredentialWithHTTP: true,
			},
		}
	}

	blobClient, err := blob.NewClientWithSharedKeyCredential(blobURL, a.credential, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob client for URL: %w", err)
	}
	return blobClient, nil
}

func downloadAll(ctx context.Context, blobClient *blob.Client) ([]byte, error) {
	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download blob: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob data: %w", err)
	}
	return data, nil
}

// ensureContainer creates the configured container once per client.
//
// The lock is held across the create call deliberately: concurrent first uploads then
// wait for one creation rather than each issuing their own and racing to set the flag.
// After initialisation this is an uncontended lock and unlock per upload.
func (a *AzureBlobClient) ensureContainer(ctx context.Context) error {
	a.initMu.Lock()
	defer a.initMu.Unlock()

	if a.containerInit {
		return nil
	}

	_, err := a.client.CreateContainer(ctx, a.containerName, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if strings.Contains(strings.ToLower(err.Error()), "containeralreadyexists") {
			a.containerInit = true
			return nil
		}
		if errors.As(err, &respErr) {
			if respErr.ErrorCode == "ContainerAlreadyExists" {
				a.containerInit = true
				return nil
			}
		}
		return fmt.Errorf("failed to ensure container: %w", err)
	}

	a.containerInit = true
	return nil
}

func parseConnectionString(connectionString string) map[string]string {
	parts := strings.Split(connectionString, ";")
	params := make(map[string]string, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx <= 0 {
			continue
		}
		key := part[:idx]
		value := part[idx+1:]
		params[key] = value
	}
	return params
}

func (a *AzureBlobClient) extractBlobPath(reference string) (string, error) {
	ref := strings.TrimSpace(reference)
	if ref == "" {
		return "", fmt.Errorf("blob reference is required")
	}

	lowerSvc := strings.ToLower(a.serviceURL)
	lowerRef := strings.ToLower(ref)
	if strings.HasPrefix(lowerRef, lowerSvc) {
		ref = ref[len(a.serviceURL):]
	}

	if idx := strings.Index(ref, "?"); idx != -1 {
		ref = ref[:idx]
	}

	ref = strings.TrimSpace(ref)
	decodedRef, err := url.PathUnescape(ref)
	if err == nil && decodedRef != "" {
		ref = decodedRef
	}

	u, err := url.Parse(ref)
	if err == nil && u.Host != "" {
		ref = u.Path
	}

	ref = strings.TrimPrefix(ref, "/")
	ref = strings.TrimPrefix(ref, a.containerName+"/")

	if ref == "" {
		return "", fmt.Errorf("blob path is empty")
	}

	return ref, nil
}

// URLFor returns the URL of blobPath in the configured container, escaped as the SDK escapes
// it, so a path can be handed to DownloadRange and BlobSize, which take URLs.
func (a *AzureBlobClient) URLFor(blobPath string) string {
	return a.client.ServiceClient().NewContainerClient(a.containerName).NewBlockBlobClient(blobPath).URL()
}

// DeleteBlob deletes blobPath from the configured container. A blob that does not exist is not
// an error: the caller wants it gone, and it is.
func (a *AzureBlobClient) DeleteBlob(ctx context.Context, blobPath string) error {
	ctx, op := a.startOp(ctx, spanBlobDelete, logInfo, attribute.String("blob.path", blobPath))
	_, err := a.client.DeleteBlob(ctx, a.containerName, blobPath, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && (respErr.ErrorCode == "BlobNotFound" || respErr.StatusCode == 404) {
			op.finish(nil)
			return nil
		}
		op.finish(err)
		return fmt.Errorf("blob delete failed: %w", err)
	}
	op.finish(nil)
	return nil
}
