package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"go.opentelemetry.io/otel/attribute"
)

// CopyFromURLSyncMaxBytes is the largest blob Copy Blob From URL copies in one synchronous request.
// Above it (or when the size is unknown) the copy is asynchronous and polled.
const CopyFromURLSyncMaxBytes int64 = 256 << 20

// DefaultCopyTimeout bounds an asynchronous copy when the caller gives no timeout.
const DefaultCopyTimeout = 30 * time.Minute

// copyPollInterval is how often an asynchronous copy is polled.
var copyPollInterval = 2 * time.Second

// ErrCopyNotStarted wraps the error of a copy the service refused before it began (the source URL
// could not be read, the service could not reach it, a bad request). The caller falls back to
// streaming the bytes. A copy that started and then failed is ErrCopyFailed.
var ErrCopyNotStarted = errors.New("server-side copy was not started")

// ErrCopyFailed wraps the failure of a copy that started and did not finish.
var ErrCopyFailed = errors.New("server-side copy failed")

// CopyFromURL copies the blob at sourceURL to blobPath of the configured container without the
// bytes passing through this process. sourceURL is read by the storage service, so it must carry
// its own authorisation (a read SAS) and be reachable from the service. size is the source's
// length; up to CopyFromURLSyncMaxBytes it is one synchronous request, otherwise (or when size is
// negative) an asynchronous copy is started and polled for at most timeout (DefaultCopyTimeout
// when zero). The result gets contentType and metadata, not the source's.
//
// An error wrapping ErrCopyNotStarted means nothing was written and the caller may stream the
// bytes instead. An error wrapping ErrCopyFailed means the destination may hold a partial blob;
// CopyFromURL deletes it before returning.
func (a *AzureBlobClient) CopyFromURL(ctx context.Context, blobPath, sourceURL, contentType string, size int64, timeout time.Duration, metadata map[string]string) (string, error) {
	if sourceURL == "" {
		return "", fmt.Errorf("source URL is required")
	}
	contentType = contentTypeOrDefault(contentType)
	async := size < 0 || size > CopyFromURLSyncMaxBytes
	ctx, op := a.startOp(ctx, spanBlobCopy, logInfo,
		attribute.String("blob.path", blobPath),
		attribute.String("blob.content_type", contentType),
		attribute.Bool("blob.copy_async", async),
	)

	if err := a.ensureContainer(ctx); err != nil {
		op.finish(err)
		return "", err
	}
	dst := a.client.ServiceClient().NewContainerClient(a.containerName).NewBlobClient(blobPath)
	if err := CopyBlob(ctx, dst, sourceURL, contentType, size, timeout, metadata); err != nil {
		op.finish(err)
		return "", err
	}
	if size >= 0 {
		op.bytes(int(size))
	}
	op.finish(nil)
	return dst.URL(), nil
}

// CopyBlob is the copy CopyFromURL performs, against any destination blob client: used for a copy
// inside a customer's account as well as into the results container. It has CopyFromURL's contract
// and errors.
func CopyBlob(ctx context.Context, dst *blob.Client, sourceURL, contentType string, size int64, timeout time.Duration, metadata map[string]string) error {
	contentType = contentTypeOrDefault(contentType)
	if size >= 0 && size <= CopyFromURLSyncMaxBytes {
		if _, err := dst.CopyFromURL(ctx, sourceURL, &blob.CopyFromURLOptions{Metadata: metadataPointers(metadata)}); err != nil {
			// A synchronous copy that errors wrote nothing: the service reports the blob only once
			// the copy completes.
			return fmt.Errorf("%w: %v", ErrCopyNotStarted, err)
		}
	} else if err := copyAsync(ctx, dst, sourceURL, timeout, metadata); err != nil {
		return err
	}

	// A copy keeps the source's properties; the file's content type is the caller's.
	if _, err := dst.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentType: to.Ptr(contentType)}, nil); err != nil {
		_, _ = dst.Delete(context.WithoutCancel(ctx), nil)
		return fmt.Errorf("%w: set content type: %v", ErrCopyFailed, err)
	}
	return nil
}

// copyAsync starts a copy and polls it. Start errors are ErrCopyNotStarted; a copy that ends in
// any state but success, or outlives timeout, is aborted, its partial blob deleted, and ErrCopyFailed.
func copyAsync(ctx context.Context, dst *blob.Client, sourceURL string, timeout time.Duration, metadata map[string]string) error {
	if timeout <= 0 {
		timeout = DefaultCopyTimeout
	}
	start, err := dst.StartCopyFromURL(ctx, sourceURL, &blob.StartCopyFromURLOptions{Metadata: metadataPointers(metadata)})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCopyNotStarted, err)
	}
	copyID := ""
	if start.CopyID != nil {
		copyID = *start.CopyID
	}

	cleanup := func() {
		bg := context.WithoutCancel(ctx)
		if copyID != "" {
			_, _ = dst.AbortCopyFromURL(bg, copyID, nil)
		}
		_, _ = dst.Delete(bg, nil)
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(copyPollInterval)
	defer tick.Stop()
	for {
		props, err := dst.GetProperties(ctx, nil)
		if err != nil {
			cleanup()
			return fmt.Errorf("%w: poll: %v", ErrCopyFailed, err)
		}
		status := blob.CopyStatusType("")
		if props.CopyStatus != nil {
			status = *props.CopyStatus
		}
		switch status {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypePending, "":
		default:
			desc := ""
			if props.CopyStatusDescription != nil {
				desc = *props.CopyStatusDescription
			}
			cleanup()
			return fmt.Errorf("%w: status %s %s", ErrCopyFailed, status, desc)
		}
		select {
		case <-ctx.Done():
			cleanup()
			return fmt.Errorf("%w: %v", ErrCopyFailed, ctx.Err())
		case <-deadline.C:
			cleanup()
			return fmt.Errorf("%w: not finished after %s", ErrCopyFailed, timeout)
		case <-tick.C:
		}
	}
}
