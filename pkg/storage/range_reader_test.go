package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type sliceRanges struct {
	data     []byte
	calls    atomic.Int64
	failAt   int64 // call number (1-based) to fail; 0 never
	shortAt  int64 // call number to answer short; 0 never
	blockFor time.Duration
}

func (s *sliceRanges) DownloadRange(ctx context.Context, _ string, offset, count int64) ([]byte, error) {
	n := s.calls.Add(1)
	if s.blockFor > 0 {
		select {
		case <-time.After(s.blockFor):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if n == s.failAt {
		return nil, errors.New("boom")
	}
	b := append([]byte(nil), s.data[offset:offset+count]...)
	if n == s.shortAt {
		b = b[:len(b)-1]
	}
	return b, nil
}

func TestRangeReaderReassemblesTheRangeExactly(t *testing.T) {
	data := make([]byte, 100_003)
	for i := range data {
		data[i] = byte(i * 7)
	}
	for _, tc := range []struct{ off, n, chunk int64 }{
		{0, int64(len(data)), 4096},
		{17, 50_000, 1000},
		{99_000, 1003, 1003},
		{5, 1, 64},
		{0, 0, 64},
	} {
		src := &sliceRanges{data: data}
		r, err := NewRangeReader(context.Background(), src, "u", tc.off, tc.n, tc.chunk)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatalf("%+v: %v", tc, err)
		}
		if !bytes.Equal(got, data[tc.off:tc.off+tc.n]) {
			t.Fatalf("%+v: bytes differ", tc)
		}
		wantReqs := (tc.n + tc.chunk - 1) / tc.chunk
		if moved, reqs := r.Stats(); reqs != wantReqs || moved != tc.n {
			t.Fatalf("%+v: stats %d bytes / %d requests, want %d / %d", tc, moved, reqs, tc.n, wantReqs)
		}
	}
}

// A short range must surface as an error, never as a silently truncated value.
func TestRangeReaderFailsOnAShortRange(t *testing.T) {
	src := &sliceRanges{data: make([]byte, 10_000), shortAt: 2}
	r, _ := NewRangeReader(context.Background(), src, "u", 0, 10_000, 4000)
	defer r.Close()
	if _, err := io.ReadAll(r); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestRangeReaderSurfacesADownloadError(t *testing.T) {
	src := &sliceRanges{data: make([]byte, 10_000), failAt: 3}
	r, _ := NewRangeReader(context.Background(), src, "u", 0, 10_000, 1000)
	defer r.Close()
	got, err := io.ReadAll(r)
	if err == nil {
		t.Fatal("expected an error")
	}
	if len(got) != 2000 {
		t.Fatalf("expected the two good chunks before the failure, got %d bytes", len(got))
	}
}

// Close must not hang when the consumer stops early, and must stop further fetching.
func TestRangeReaderCloseStopsAnAbandonedRead(t *testing.T) {
	src := &sliceRanges{data: make([]byte, 1_000_000), blockFor: 10 * time.Millisecond}
	r, _ := NewRangeReader(context.Background(), src, "u", 0, 1_000_000, 1000)
	buf := make([]byte, 10)
	if _, err := r.Read(buf); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return")
	}
	if calls := src.calls.Load(); calls > 4 {
		t.Fatalf("fetching continued after Close: %d calls", calls)
	}
}

func TestRangeReaderHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := &sliceRanges{data: make([]byte, 1_000_000), blockFor: 50 * time.Millisecond}
	r, _ := NewRangeReader(ctx, src, "u", 0, 1_000_000, 1000)
	defer r.Close()
	cancel()
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("expected an error after cancellation")
	}
}
