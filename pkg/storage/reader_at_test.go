package storage

import (
	"bytes"
	"context"
	"io"
	"testing"
)

// byteRanges serves ranges of one in-memory blob and counts what it serves.
type byteRanges struct {
	data     []byte
	requests int
	moved    int64
}

func (b *byteRanges) DownloadRange(_ context.Context, _ string, offset, count int64) ([]byte, error) {
	if offset >= int64(len(b.data)) {
		return nil, nil
	}
	end := offset + count
	if count == 0 || end > int64(len(b.data)) {
		end = int64(len(b.data))
	}
	b.requests++
	b.moved += end - offset
	return b.data[offset:end], nil
}

func patterned(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i % 251)
	}
	return out
}

// After Preload, reads inside the span cost nothing, small or large. A large read used
// to go straight to storage whatever the window held, which would have fetched the
// preloaded bytes a second time.
func TestPreloadServesLaterReadsFromMemory(t *testing.T) {
	data := patterned(300 << 10)
	src := &byteRanges{data: data}
	r, err := NewBlobReaderAt(context.Background(), src, "u", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Preload(0, 200<<10); err != nil {
		t.Fatalf("Preload: %v", err)
	}
	if src.requests != 1 || src.moved != 200<<10 {
		t.Fatalf("Preload made %d requests moving %d bytes, want 1 and %d", src.requests, src.moved, 200<<10)
	}

	for _, rd := range []struct{ off, n int64 }{{0, 30}, {1000, 100 << 10}, {(200 << 10) - 64, 64}} {
		p := make([]byte, rd.n)
		if _, err := r.ReadAt(p, rd.off); err != nil {
			t.Fatalf("ReadAt(%d, %d): %v", rd.off, rd.n, err)
		}
		if !bytes.Equal(p, data[rd.off:rd.off+rd.n]) {
			t.Fatalf("ReadAt(%d, %d) returned the wrong bytes", rd.off, rd.n)
		}
	}
	if src.requests != 1 {
		t.Fatalf("reads inside the preloaded span made %d extra requests", src.requests-1)
	}

	// A read past the span still works, and fetches.
	p := make([]byte, 10)
	if _, err := r.ReadAt(p, 250<<10); err != nil {
		t.Fatalf("ReadAt past span: %v", err)
	}
	if !bytes.Equal(p, data[250<<10:(250<<10)+10]) || src.requests != 2 {
		t.Fatalf("read past the span: requests = %d, want 2", src.requests)
	}
}

func TestPreloadClampsToTheBlob(t *testing.T) {
	data := patterned(1000)
	src := &byteRanges{data: data}
	r, _ := NewBlobReaderAt(context.Background(), src, "u", int64(len(data)))

	if err := r.Preload(900, 5000); err != nil {
		t.Fatalf("Preload: %v", err)
	}
	if src.moved != 100 {
		t.Fatalf("moved %d bytes, want the 100 left in the blob", src.moved)
	}
	p := make([]byte, 200)
	n, err := r.ReadAt(p, 900)
	if n != 100 || err != io.EOF {
		t.Fatalf("ReadAt at the tail = (%d, %v), want (100, EOF)", n, err)
	}
	if err := r.Preload(5000, 10); err != nil || src.requests != 1 {
		t.Fatalf("Preload past the end: err %v, requests %d; want no error and no request", err, src.requests)
	}
}

// A window already held at the start of the span is reused, not fetched again.
func TestPreloadKeepsTheWindowItAlreadyHolds(t *testing.T) {
	data := patterned(300 << 10)
	src := &byteRanges{data: data}
	r, _ := NewBlobReaderAt(context.Background(), src, "u", int64(len(data)))

	p := make([]byte, 30)
	if _, err := r.ReadAt(p, 0); err != nil { // anchors a 64 KiB window at 0
		t.Fatal(err)
	}
	before := src.moved
	if err := r.Preload(0, 200<<10); err != nil {
		t.Fatalf("Preload: %v", err)
	}
	if got := src.moved - before; got != (200<<10)-DefaultReadAheadBytes {
		t.Fatalf("Preload moved %d bytes, want only the %d not already held", got, (200<<10)-DefaultReadAheadBytes)
	}

	q := make([]byte, 150<<10)
	if _, err := r.ReadAt(q, 10); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(q, data[10:10+(150<<10)]) {
		t.Fatal("a read across the held and fetched parts returned the wrong bytes")
	}
	if src.requests != 2 {
		t.Fatalf("requests = %d, want 2 (the first window and the preload)", src.requests)
	}

	// A span the window already covers whole costs nothing.
	if err := r.Preload(100, 1000); err != nil || src.requests != 2 {
		t.Fatalf("Preload inside the window: err %v, requests %d", err, src.requests)
	}
}
