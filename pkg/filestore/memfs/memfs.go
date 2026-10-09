// Package memfs is an in-memory filestore.Backend for tests.
package memfs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/wehubfusion/Icarus/pkg/storage"
)

// Backend keeps blobs in memory, keyed by path.
type Backend struct {
	mu    sync.Mutex
	Blobs map[string][]byte
	Types map[string]string
	// Copies lists the paths written by CopyFromURL, in order.
	Copies []string
	// FailCopy, when non-nil, is the error CopyFromURL returns.
	FailCopy error
}

// New returns an empty Backend.
func New() *Backend { return &Backend{Blobs: map[string][]byte{}, Types: map[string]string{}} }

// Put stores data at path.
func (b *Backend) Put(path string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Blobs[path] = data
}

func (b *Backend) UploadStream(_ context.Context, path string, body io.Reader, ct string, _ map[string]string) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Blobs[path], b.Types[path] = data, ct
	return b.URLFor(path), nil
}

func (b *Backend) DownloadRange(_ context.Context, url string, off, count int64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.Blobs[strings.TrimPrefix(url, "mem://")]
	if !ok {
		return nil, fmt.Errorf("not found: %s", url)
	}
	end := off + count
	if count == 0 || end > int64(len(data)) {
		end = int64(len(data))
	}
	return append([]byte(nil), data[off:end]...), nil
}

func (b *Backend) URLFor(path string) string { return "mem://" + path }

func (b *Backend) DeleteBlob(_ context.Context, path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.Blobs, path)
	return nil
}

// CopyFromURL copies a blob of this backend ("mem://path"), as the storage service would copy one
// from a URL. Any other source is refused as a copy that was not started, so a caller falls back.
// FailCopy, when set, is returned instead, to test that fallback.
func (b *Backend) CopyFromURL(_ context.Context, path, sourceURL, contentType string, _ int64, _ time.Duration, _ map[string]string) (string, error) {
	if b.FailCopy != nil {
		return "", b.FailCopy
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.Blobs[strings.TrimPrefix(sourceURL, "mem://")]
	if !ok || !strings.HasPrefix(sourceURL, "mem://") {
		return "", fmt.Errorf("%w: cannot read %s", storage.ErrCopyNotStarted, sourceURL)
	}
	b.Blobs[path], b.Types[path] = append([]byte(nil), data...), contentType
	b.Copies = append(b.Copies, path)
	return b.URLFor(path), nil
}

// BlobSize is the length of the blob at url.
func (b *Backend) BlobSize(_ context.Context, url string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.Blobs[strings.TrimPrefix(url, "mem://")]
	if !ok {
		return 0, fmt.Errorf("not found: %s", url)
	}
	return int64(len(data)), nil
}
