// Package memfs is an in-memory filestore.Backend for tests.
package memfs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Backend keeps blobs in memory, keyed by path.
type Backend struct {
	mu    sync.Mutex
	Blobs map[string][]byte
	Types map[string]string
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
