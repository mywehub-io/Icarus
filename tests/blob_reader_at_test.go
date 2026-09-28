package tests

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/storage"
)

// sliceRangeSource serves ranged reads from an in-memory blob, standing in for Azure
// so these tests need no storage account and no Azurite.
type sliceRangeSource struct {
	data  []byte
	calls int
	// ranges records every (offset, count) served, so tests can assert on the access
	// pattern and not merely the bytes returned.
	ranges []([2]int64)
	err    error
}

func (s *sliceRangeSource) DownloadRange(_ context.Context, _ string, offset, count int64) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.calls++
	s.ranges = append(s.ranges, [2]int64{offset, count})
	if offset >= int64(len(s.data)) {
		return nil, nil
	}
	end := offset + count
	if count == 0 || end > int64(len(s.data)) {
		end = int64(len(s.data))
	}
	out := make([]byte, end-offset)
	copy(out, s.data[offset:end])
	return out, nil
}

func newReader(t *testing.T, data []byte, chunk int64) (*storage.BlobReaderAt, *sliceRangeSource) {
	t.Helper()
	src := &sliceRangeSource{data: data}
	r, err := storage.NewBlobReaderAt(context.Background(), src, "https://acct.blob/c/x", int64(len(data)))
	if err != nil {
		t.Fatalf("NewBlobReaderAt: %v", err)
	}
	if chunk > 0 {
		r.WithReadAhead(chunk)
	}
	return r, src
}

func TestBlobReaderAtServesExactBytes(t *testing.T) {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	r, _ := newReader(t, data, 8)

	for _, tc := range []struct{ off, n int }{
		{0, 1}, {0, 8}, {3, 10}, {7, 2}, {8, 8}, {30, 6},
	} {
		got := make([]byte, tc.n)
		n, err := r.ReadAt(got, int64(tc.off))
		if err != nil {
			t.Fatalf("ReadAt(%d,%d): %v", tc.off, tc.n, err)
		}
		if n != tc.n {
			t.Fatalf("ReadAt(%d,%d): read %d bytes", tc.off, tc.n, n)
		}
		if want := data[tc.off : tc.off+tc.n]; !bytes.Equal(got, want) {
			t.Fatalf("ReadAt(%d,%d) = %q, want %q", tc.off, tc.n, got, want)
		}
	}
}

// A read running past the end must return the bytes it could and io.EOF, per the
// io.ReaderAt contract. archive/zip depends on this when it probes for the
// end-of-central-directory record near the tail.
func TestBlobReaderAtShortReadReturnsEOF(t *testing.T) {
	data := []byte("short")
	r, _ := newReader(t, data, 4)

	buf := make([]byte, 10)
	n, err := r.ReadAt(buf, 2)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 3 || !bytes.Equal(buf[:n], []byte("ort")) {
		t.Fatalf("got %q (n=%d), want %q", buf[:n], n, "ort")
	}

	if _, err := r.ReadAt(buf, int64(len(data))); !errors.Is(err, io.EOF) {
		t.Fatalf("read at EOF: expected io.EOF, got %v", err)
	}
}

// Sequential access inside one chunk must not re-fetch: that read-ahead is what keeps
// a central-directory scan to a single request instead of one per field.
func TestBlobReaderAtReadAheadCollapsesRequests(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 4096)
	r, src := newReader(t, data, 1024)

	buf := make([]byte, 16)
	for off := int64(0); off < 1024; off += 16 {
		if _, err := r.ReadAt(buf, off); err != nil {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
	}
	if src.calls != 1 {
		t.Fatalf("expected 1 ranged GET for 64 reads inside one chunk, got %d", src.calls)
	}

	fetched, requests := r.Stats()
	if requests != 1 || fetched != 1024 {
		t.Fatalf("Stats() = (%d bytes, %d requests), want (1024, 1)", fetched, requests)
	}
}

// A read at least as large as the chunk goes straight to storage. Buffering it would
// double the memory for a request read-ahead cannot help.
func TestBlobReaderAtLargeReadBypassesCache(t *testing.T) {
	data := bytes.Repeat([]byte("y"), 8192)
	r, src := newReader(t, data, 1024)

	buf := make([]byte, 4096)
	if _, err := r.ReadAt(buf, 100); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if src.calls != 1 {
		t.Fatalf("expected a single direct fetch, got %d", src.calls)
	}
	if got := src.ranges[0]; got[0] != 100 || got[1] != 4096 {
		t.Fatalf("expected exact range (100,4096), got (%d,%d)", got[0], got[1])
	}
}

func TestBlobReaderAtPropagatesError(t *testing.T) {
	src := &sliceRangeSource{data: []byte("abc"), err: errors.New("network down")}
	r, err := storage.NewBlobReaderAt(context.Background(), src, "u", 3)
	if err != nil {
		t.Fatalf("NewBlobReaderAt: %v", err)
	}
	if _, err := r.ReadAt(make([]byte, 2), 0); err == nil || !strings.Contains(err.Error(), "network down") {
		t.Fatalf("expected the underlying error to surface, got %v", err)
	}
}

func TestBlobReaderAtValidatesArguments(t *testing.T) {
	src := &sliceRangeSource{}
	if _, err := storage.NewBlobReaderAt(context.Background(), nil, "u", 1); err == nil {
		t.Fatal("expected error for nil downloader")
	}
	if _, err := storage.NewBlobReaderAt(context.Background(), src, "", 1); err == nil {
		t.Fatal("expected error for empty URL")
	}
	if _, err := storage.NewBlobReaderAt(context.Background(), src, "u", -1); err == nil {
		t.Fatal("expected error for negative size")
	}
}

// buildArchive writes a STORED zip whose entry names are real StandardUnitOutput flat
// keys, including the bracket and double-slash forms.
func buildArchive(t *testing.T, entries [][2]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: e[0], Method: zip.Store})
		if err != nil {
			t.Fatalf("CreateHeader(%q): %v", e[0], err)
		}
		if _, err := io.WriteString(w, e[1]); err != nil {
			t.Fatalf("write %q: %v", e[0], err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

// Entry names are flat keys verbatim. Some ZIP implementations normalise path
// separators, which would silently corrupt a key containing "//" — the array-path
// form. This asserts Go's own reader hands them back byte-identically.
func TestArchiveEntryNamesRoundTripVerbatim(t *testing.T) {
	names := []string{
		"abc-/name",
		"abc-/rows",
		"abc-/auth[0]",
		"abc-/auth[10]/sub[2]",
		"abc-/data//name",
		"abc-/a//b//c",
		"_wehub/manifest.json",
	}
	entries := make([][2]string, 0, len(names))
	for i, n := range names {
		entries = append(entries, [2]string{n, fmt.Sprintf("value-%d", i)})
	}
	raw := buildArchive(t, entries)

	r, _ := newReader(t, raw, 4096)
	zr, err := zip.NewReader(r, int64(len(raw)))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}

	if len(zr.File) != len(names) {
		t.Fatalf("got %d entries, want %d", len(zr.File), len(names))
	}
	for i, f := range zr.File {
		if f.Name != names[i] {
			t.Errorf("entry %d name = %q, want %q (byte-identical round trip required)", i, f.Name, names[i])
		}
		if f.Method != zip.Store {
			t.Errorf("entry %q stored with method %d, want STORED", f.Name, f.Method)
		}
	}
}

// The premise of the whole change: open an archive over ranged reads, pull one small
// entry out of a large one, and move a small fraction of the blob.
func TestArchivePointLookupMovesOnlyWhatItNeeds(t *testing.T) {
	big := strings.Repeat("A", 2<<20) // 2 MiB, standing in for a Shape A value
	raw := buildArchive(t, [][2]string{
		{"node-/error", `""`},
		{"node-/name", `"david"`},
		{"node-/rows", `"` + big + `"`},
	})

	r, _ := newReader(t, raw, storage.DefaultReadAheadBytes)
	zr, err := zip.NewReader(r, int64(len(raw)))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}

	var got string
	for _, f := range zr.File {
		if f.Name != "node-/name" {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open entry: %v", err)
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read entry: %v", err)
		}
		got = string(b)
	}
	if got != `"david"` {
		t.Fatalf("entry value = %q, want %q", got, `"david"`)
	}

	fetched, requests := r.Stats()
	if fetched >= int64(len(raw)) {
		t.Fatalf("fetched %d bytes of a %d byte archive — no better than a full download", fetched, len(raw))
	}
	// Directory scan plus one small entry should stay far below the payload. The
	// ratio here is the thesis of the change, so assert on it rather than eyeball it.
	if ratio := float64(fetched) / float64(len(raw)); ratio > 0.10 {
		t.Fatalf("moved %.1f%% of the archive (%d of %d bytes) in %d requests; expected well under 10%%",
			ratio*100, fetched, len(raw), requests)
	}
	t.Logf("point lookup moved %d of %d bytes (%.2f%%) in %d ranged GETs",
		fetched, len(raw), float64(fetched)/float64(len(raw))*100, requests)
}

// overReturningSource answers with more bytes than the range asked for, drawn from a
// blob that is larger than the size the reader was given. That pair is the reachable
// shape of the hazard: a stale BlobReference.SizeBytes (the plan's R4 case, where a blob
// was overwritten and grew) combined with any service or intermediary that does not
// honour Count exactly.
//
// With a correct size it cannot bite, because a read that is not clamped by end-of-blob
// has want == len(p) and the copy is bounded by the caller's buffer anyway. It bites only
// when the read is clamped short: then want < len(p), and an unclamped result copies past
// what the reader believes the blob holds and reports more bytes than it was asked for.
type overReturningSource struct {
	data  []byte
	extra int64
}

func (s *overReturningSource) DownloadRange(_ context.Context, _ string, offset, count int64) ([]byte, error) {
	if offset >= int64(len(s.data)) {
		return nil, nil
	}
	end := offset + count + s.extra
	if end > int64(len(s.data)) {
		end = int64(len(s.data))
	}
	out := make([]byte, end-offset)
	copy(out, s.data[offset:end])
	return out, nil
}

func TestBlobReaderAtClampsAnOverLongResponse(t *testing.T) {
	// 48 bytes on the wire, but the reader is told the blob is 36 — a stale size.
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKL")
	const declared = 36

	src := &overReturningSource{data: data, extra: 12}
	r, err := storage.NewBlobReaderAt(context.Background(), src, "https://acct.blob/c/x", declared)
	if err != nil {
		t.Fatalf("NewBlobReaderAt: %v", err)
	}
	r.WithReadAhead(4)

	// Read near the believed end with a buffer longer than what remains: want is 4,
	// len(p) is 10, and the source offers 16. Only the 4 bytes inside the declared size
	// may be reported.
	buf := make([]byte, 10)
	n, err := r.ReadAt(buf, 32)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF at the declared end, got %v", err)
	}
	if n != 4 {
		t.Fatalf("returned %d bytes for a 4 byte remainder — the over-long response leaked "+
			"%d bytes past the end of the blob the reader was told about", n, n-4)
	}
	if want := data[32:36]; !bytes.Equal(buf[:n], want) {
		t.Fatalf("got %q, want %q", buf[:n], want)
	}

	// An ordinary interior read must still be exact.
	mid := make([]byte, 8)
	if n, err := r.ReadAt(mid, 4); err != nil || n != 8 {
		t.Fatalf("interior ReadAt = (%d, %v), want (8, nil)", n, err)
	}
	if want := data[4:12]; !bytes.Equal(mid, want) {
		t.Fatalf("interior read = %q, want %q", mid, want)
	}
}

// A large entry must be consumable through a bounded buffer rather than materialised,
// which is the capability the follow-up's plugin work will depend on.
func TestArchiveLargeEntryStreamsInBoundedMemory(t *testing.T) {
	const size = 4 << 20
	big := strings.Repeat("B", size)
	raw := buildArchive(t, [][2]string{{"node-/payload", big}})

	r, _ := newReader(t, raw, 64*1024)
	zr, err := zip.NewReader(r, int64(len(raw)))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}

	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatalf("open entry: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, 32*1024)
	total := 0
	for {
		n, err := rc.Read(buf)
		total += n
		for i := 0; i < n; i++ {
			if buf[i] != 'B' {
				t.Fatalf("corrupt byte at %d", total-n+i)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if total != size {
		t.Fatalf("streamed %d bytes, want %d", total, size)
	}
}
