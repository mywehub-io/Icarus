package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// DefaultReadAheadBytes is how far ahead BlobReaderAt fetches on a small read.
//
// It is a trade between round trips and wasted bytes, and the access pattern it is
// tuned for is archive/zip's: a ~1 KiB probe at the tail for the end-of-central-
// directory record, a sequential scan of the directory, then for each entry opened a
// ~30 byte local-header read immediately followed by the entry body at the next byte.
//
// Read-ahead is anchored at the requested offset rather than aligned to a global grid.
// Alignment looked tidier and was measurably worse: a 30-byte header read at an
// arbitrary offset pulled a whole aligned block, and the body read that followed it
// often landed in the next block, so a single small field cost two large fetches. With
// an anchored window the header read pre-fetches the body sitting right behind it, and
// one fetch serves both.
const DefaultReadAheadBytes int64 = 64 * 1024

// RangeDownloader fetches a byte range of a blob. AzureBlobClient implements it.
//
// It is declared as its own interface rather than taking BlobStorageClient so that
// BlobReaderAt can be driven by a byte-slice fake in tests with no storage account.
type RangeDownloader interface {
	DownloadRange(ctx context.Context, blobURL string, offset, count int64) ([]byte, error)
}

// BlobReaderAt adapts a blob to io.ReaderAt by issuing ranged GETs, so that readers
// which seek — archive/zip in particular — can work against blob storage without
// downloading the whole object.
//
// The context is held on the struct because io.ReadAt takes no context and cannot be
// given one. It is the adapter's only way to carry cancellation, so a BlobReaderAt is
// scoped to a single operation and must not outlive the context it was built with.
//
// ReadAt is safe for concurrent use, which archive/zip requires when several entries
// are opened from one Reader. Concurrency is serialised on a single mutex and the
// read-ahead holds one chunk, so parallel readers at distant offsets will evict each
// other's chunk. That is correct but not fast; sequential access is the intended shape.
type BlobReaderAt struct {
	ctx       context.Context
	src       RangeDownloader
	blobURL   string
	size      int64
	chunkSize int64

	mu       sync.Mutex
	window   []byte
	windowAt int64 // offset of window[0] within the blob; -1 when no window is held
	fetched  int64 // total bytes pulled from storage
	requests int64 // total ranged GETs issued
}

// NewBlobReaderAt builds a reader over the blob at blobURL.
//
// size must be the blob's exact length. Callers already have it without a round trip:
// BlobReference.SizeBytes carries it from the writer, and archive/zip needs the same
// value, so no HEAD request is required on this path.
func NewBlobReaderAt(ctx context.Context, src RangeDownloader, blobURL string, size int64) (*BlobReaderAt, error) {
	if src == nil {
		return nil, fmt.Errorf("range downloader is required")
	}
	if blobURL == "" {
		return nil, fmt.Errorf("blob URL is required")
	}
	if size < 0 {
		return nil, fmt.Errorf("size must not be negative, got %d", size)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &BlobReaderAt{
		ctx:       ctx,
		src:       src,
		blobURL:   blobURL,
		size:      size,
		chunkSize: DefaultReadAheadBytes,
		windowAt:  -1,
	}, nil
}

// WithReadAhead overrides how far ahead a small read fetches. A non-positive value
// restores the default. Intended for tests and for callers that know their pattern.
func (r *BlobReaderAt) WithReadAhead(n int64) *BlobReaderAt {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n <= 0 {
		n = DefaultReadAheadBytes
	}
	r.chunkSize = n
	r.window = nil
	r.windowAt = -1
	return r
}

// Size returns the blob length the reader was built with.
func (r *BlobReaderAt) Size() int64 { return r.size }

// Stats reports what this reader actually pulled from storage: bytes transferred and
// ranged GETs issued.
//
// bytes is the denominator of the ratio this whole change exists to move — bytes moved
// against fields requested — so it is exposed rather than merely logged.
func (r *BlobReaderAt) Stats() (bytes int64, requests int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetched, r.requests
}

// ReadAt implements io.ReaderAt.
func (r *BlobReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("storage: negative offset %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}

	// Clamp the request to the end of the blob. A short read at EOF must report
	// io.EOF alongside the bytes it did produce, per the io.ReaderAt contract.
	want := int64(len(p))
	short := false
	if off+want > r.size {
		want = r.size - off
		short = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		n   int
		err error
	)
	if r.windowCoversRangeLocked(off, want) {
		// Already held, typically because Preload fetched the span in one request.
		// Checked before the large-read branch, which would otherwise fetch it again.
		n = copy(p[:want], r.window[off-r.windowAt:])
	} else if want >= r.chunkSize {
		// Large read: go straight to storage. Buffering it would double the memory
		// for no benefit, since read-ahead cannot help a request this size.
		var b []byte
		b, err = r.fetchLocked(off, want)
		n = copy(p, b)
	} else {
		n, err = r.readCachedLocked(p[:want], off)
	}
	if err != nil {
		return n, err
	}
	if int64(n) < want {
		// The service answered with fewer bytes than the range asked for. io.ReaderAt
		// forbids reporting a short read with a nil error, and archive/zip trusts that:
		// returning nil here would hand it silently truncated entry bytes instead of a
		// failure. Only the large-read path can reach this, since readCachedLocked
		// loops until the buffer is full or it errors.
		return n, io.ErrUnexpectedEOF
	}
	if short {
		return n, io.EOF
	}
	return n, nil
}

// readCachedLocked serves a small read from the read-ahead window, re-anchoring the
// window at the current position whenever the request is not already covered.
func (r *BlobReaderAt) readCachedLocked(p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		cur := off + int64(total)

		if !r.windowCoversLocked(cur) {
			want := r.chunkSize
			if cur+want > r.size {
				want = r.size - cur
			}
			b, err := r.fetchLocked(cur, want)
			if err != nil {
				return total, err
			}
			if len(b) == 0 {
				return total, io.ErrUnexpectedEOF
			}
			r.window = b
			r.windowAt = cur
		}

		inWindow := cur - r.windowAt
		total += copy(p[total:], r.window[inWindow:])
	}
	return total, nil
}

// Preload fetches [off, off+count) in one ranged GET and holds it as the read-ahead
// window, so every later read inside that span is served from memory.
//
// It exists for a caller that knows it is about to read most of a span: one request
// replaces many anchored windows. The span is clamped to the end of the blob. Bytes the
// current window already holds at the start of the span are kept rather than fetched
// again; archive/zip reads the manifest from offset 0 while opening, so for an archive
// that is the first 64 KiB. A read outside the span re-anchors the window as usual,
// which drops the preloaded bytes.
func (r *BlobReaderAt) Preload(off, count int64) error {
	if off < 0 {
		return fmt.Errorf("storage: negative offset %d", off)
	}
	if count <= 0 || off >= r.size {
		return nil
	}
	if off+count > r.size {
		count = r.size - off
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var held []byte
	if r.windowCoversLocked(off) {
		held = r.window[off-r.windowAt:]
		if int64(len(held)) >= count {
			r.window = held[:count]
			r.windowAt = off
			return nil
		}
	}

	rest := count - int64(len(held))
	b, err := r.fetchLocked(off+int64(len(held)), rest)
	if err != nil {
		return err
	}
	if int64(len(b)) < rest {
		return io.ErrUnexpectedEOF
	}

	window := make([]byte, 0, count)
	window = append(window, held...)
	r.window = append(window, b...)
	r.windowAt = off
	return nil
}

// windowCoversRangeLocked reports whether the whole of [off, off+n) is in the window.
func (r *BlobReaderAt) windowCoversRangeLocked(off, n int64) bool {
	return r.windowAt >= 0 &&
		off >= r.windowAt &&
		off+n <= r.windowAt+int64(len(r.window))
}

func (r *BlobReaderAt) windowCoversLocked(off int64) bool {
	return r.windowAt >= 0 &&
		off >= r.windowAt &&
		off < r.windowAt+int64(len(r.window))
}

func (r *BlobReaderAt) fetchLocked(offset, count int64) ([]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	b, err := r.src.DownloadRange(r.ctx, r.blobURL, offset, count)
	if err != nil {
		return nil, fmt.Errorf("range read at %d (%d bytes): %w", offset, count, err)
	}
	if int64(len(b)) > count {
		// A service answering with more than the requested range must not be able to
		// push bytes past the window the caller sized. Both callers assume the result
		// is at most count long: the large-read path copies straight into the caller's
		// buffer, and the read-ahead path would otherwise cache a window it believes
		// extends further into the blob than it does.
		b = b[:count]
	}
	r.requests++
	r.fetched += int64(len(b))
	return b, nil
}
