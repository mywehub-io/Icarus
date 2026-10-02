package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// DefaultRangeChunkBytes is the ranged GET size RangeReader uses when none is given.
//
// The pattern it serves is the opposite of BlobReaderAt's: one long sequential read of a
// single large value, not many small reads scattered across a directory. Large chunks keep
// the request count low; the cost is memory, bounded at roughly three chunks (one being
// read, one queued, one in flight).
const DefaultRangeChunkBytes int64 = 8 * 1024 * 1024

// RangeReader streams the byte range [offset, offset+length) of a blob as an io.Reader,
// issuing sequential ranged GETs of chunk bytes and fetching the next chunk while the
// current one is consumed, so the network and the consumer overlap.
//
// It exists for consumers that need one large value whole but must not hold it whole — a
// parser fed a file in chunks, a decoder writing straight to disk. archive.Reader's
// EntryRange supplies the range.
//
// A RangeReader is single-use and not safe for concurrent Reads. Close must be called,
// including after an error or an early stop, to release the prefetch goroutine.
type RangeReader struct {
	cancel context.CancelFunc
	chunks chan rangeChunk
	done   chan struct{}

	cur []byte
	err error

	mu       sync.Mutex
	fetched  int64
	requests int64
	// exit is why the prefetch goroutine stopped: nil only when the whole range was
	// delivered. Read consults it when the channel closes, so an aborted stream is never
	// mistaken for a complete one.
	exit error
}

type rangeChunk struct {
	data []byte
	err  error
}

// NewRangeReader starts streaming the range. A non-positive chunk uses
// DefaultRangeChunkBytes. A zero length yields an immediately exhausted reader.
func NewRangeReader(ctx context.Context, src RangeDownloader, blobURL string, offset, length, chunk int64) (*RangeReader, error) {
	if src == nil {
		return nil, fmt.Errorf("range downloader is required")
	}
	if blobURL == "" {
		return nil, fmt.Errorf("blob URL is required")
	}
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("offset and length must not be negative, got %d and %d", offset, length)
	}
	if chunk <= 0 {
		chunk = DefaultRangeChunkBytes
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancel := context.WithCancel(ctx)
	r := &RangeReader{
		cancel: cancel,
		// Capacity one is the read-ahead: one chunk waits while the next is fetched.
		chunks: make(chan rangeChunk, 1),
		done:   make(chan struct{}),
	}
	go r.fetch(ctx, src, blobURL, offset, length, chunk)
	return r, nil
}

func (r *RangeReader) fetch(ctx context.Context, src RangeDownloader, blobURL string, offset, length, chunk int64) {
	defer close(r.done)
	defer close(r.chunks)

	exit := fmt.Errorf("range read stopped before the range was delivered")
	defer func() {
		r.mu.Lock()
		r.exit = exit
		r.mu.Unlock()
	}()

	end := offset + length
	for off := offset; off < end; {
		want := chunk
		if off+want > end {
			want = end - off
		}
		b, err := src.DownloadRange(ctx, blobURL, off, want)
		if err == nil && int64(len(b)) != want {
			// A range that comes back short or long is not a value this reader can vouch
			// for. Passing a short chunk on would hand the consumer silently truncated
			// bytes, so it is a failure, never a partial success.
			err = fmt.Errorf("range read at %d: got %d bytes, want %d: %w", off, len(b), want, io.ErrUnexpectedEOF)
			b = nil
		} else if err != nil {
			err = fmt.Errorf("range read at %d (%d bytes): %w", off, want, err)
		}
		if err == nil {
			r.mu.Lock()
			r.fetched += want
			r.requests++
			r.mu.Unlock()
		}

		select {
		case r.chunks <- rangeChunk{data: b, err: err}:
		case <-ctx.Done():
			exit = fmt.Errorf("range read at %d: %w", off, ctx.Err())
			return
		}
		if err != nil {
			exit = err
			return
		}
		off += want
	}
	exit = nil
}

// Read implements io.Reader.
func (r *RangeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.cur) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		c, ok := <-r.chunks
		if !ok {
			// The close happens after exit is recorded, so it is settled by now.
			r.mu.Lock()
			exit := r.exit
			r.mu.Unlock()
			if exit != nil {
				r.err = exit
			} else {
				r.err = io.EOF
			}
			continue
		}
		if c.err != nil {
			r.err = c.err
			continue
		}
		r.cur = c.data
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// Close stops any fetch in progress and waits for the prefetch goroutine to exit.
func (r *RangeReader) Close() error {
	r.cancel()
	<-r.done
	r.cur = nil
	return nil
}

// Stats reports bytes transferred and ranged GETs issued so far.
func (r *RangeReader) Stats() (bytes int64, requests int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetched, r.requests
}
