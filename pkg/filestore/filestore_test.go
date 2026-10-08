package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// memBackend stores blobs in memory and, like a block blob upload, commits a blob only when
// the upload's reader ends cleanly.
type memBackend struct {
	mu      sync.Mutex
	blobs   map[string][]byte
	types   map[string]string
	ranges  int
	deleted []string
}

func newMem() *memBackend {
	return &memBackend{blobs: map[string][]byte{}, types: map[string]string{}}
}

func (m *memBackend) UploadStream(ctx context.Context, p string, body io.Reader, ct string, _ map[string]string) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[p] = data
	m.types[p] = ct
	return "mem://" + p, nil
}

func (m *memBackend) DownloadRange(_ context.Context, u string, off, count int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ranges++
	data, ok := m.blobs[strings.TrimPrefix(u, "mem://")]
	if !ok {
		return nil, fmt.Errorf("not found: %s", u)
	}
	end := off + count
	if count == 0 || end > int64(len(data)) {
		end = int64(len(data))
	}
	return append([]byte(nil), data[off:end]...), nil
}

func (m *memBackend) URLFor(p string) string { return "mem://" + p }

func (m *memBackend) DeleteBlob(_ context.Context, p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, p)
	m.deleted = append(m.deleted, p)
	return nil
}

func TestCreateOpenRoundTrip(t *testing.T) {
	mem := newMem()
	s, err := New(mem, "wf", "run")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := fileref.PathFor("wf", "run", "n1", "payload", fileref.PathOptions{Ext: "csv"})
	w, err := s.Create(ctx, p, fileref.ContentTypeCSV, "export.csv")
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("a,b,c\n"), 100000)
	for i := 0; i < len(want); i += 4096 {
		end := i + 4096
		if end > len(want) {
			end = len(want)
		}
		if _, err := w.Write(want[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	if ref.Size != int64(len(want)) || ref.Path != p || ref.ContentType != fileref.ContentTypeCSV || ref.FileName != "export.csv" {
		t.Fatalf("bad ref %+v", ref)
	}
	if mem.types[p] != fileref.ContentTypeCSV {
		t.Fatalf("blob content type %q", mem.types[p])
	}

	old := ChunkBytes
	ChunkBytes = 64 * 1024
	defer func() { ChunkBytes = old }()
	rc, err := s.Open(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("read back mismatch: err=%v len=%d", err, len(got))
	}
	if mem.ranges < 2 {
		t.Fatalf("expected ranged reads, got %d", mem.ranges)
	}

	path, err := s.OpenToDisk(ctx, ref, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	disk, _ := os.ReadFile(path)
	if !bytes.Equal(disk, want) || !strings.HasSuffix(path, ".csv") {
		t.Fatalf("disk copy mismatch at %s", path)
	}
}

func TestZeroByteFile(t *testing.T) {
	mem := newMem()
	s, _ := New(mem, "wf", "run")
	ctx := context.Background()
	w, err := s.Create(ctx, "results/wf/run/n1/empty.txt", "", "")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	if ref.Size != 0 || ref.ContentType != fileref.ContentTypeText {
		t.Fatalf("bad ref %+v", ref)
	}
	if _, ok := mem.blobs[ref.Path]; !ok {
		t.Fatal("zero-byte blob not written")
	}
	rc, err := s.Open(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	if len(b) != 0 {
		t.Fatal("expected empty read")
	}
}

func TestAbortLeavesNoBlob(t *testing.T) {
	mem := newMem()
	s, _ := New(mem, "wf", "run")
	w, err := s.Create(context.Background(), "results/wf/run/n1/payload.bin", "", "")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("partial"))
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, ok := mem.blobs["results/wf/run/n1/payload.bin"]; ok {
		t.Fatal("aborted blob exists")
	}
	if _, err := w.Close(); err == nil {
		t.Fatal("Close after Abort must fail")
	}
}

func TestRefusesOtherRuns(t *testing.T) {
	mem := newMem()
	mem.blobs["results/wf/other/n1/payload.csv"] = []byte("secret")
	s, _ := New(mem, "wf", "run")
	ctx := context.Background()
	for _, p := range []string{
		"results/wf/other/n1/payload.csv",
		"results/wf/run/../other/n1/payload.csv",
		"monitoring/x.json",
	} {
		if _, err := s.Open(ctx, fileref.FileRef{Path: p, Size: 6}); !errors.Is(err, ErrNotInRun) {
			t.Errorf("Open(%q) err = %v, want ErrNotInRun", p, err)
		}
		if _, err := s.Create(ctx, p, "", ""); !errors.Is(err, ErrNotInRun) {
			t.Errorf("Create(%q) err = %v, want ErrNotInRun", p, err)
		}
	}
	if mem.ranges != 0 {
		t.Fatal("a refused open made a request")
	}
}

func TestUploadFailureSurfaces(t *testing.T) {
	s, _ := New(failingBackend{newMem()}, "wf", "run")
	w, err := s.Create(context.Background(), "results/wf/run/n1/p.bin", "", "")
	if err != nil {
		t.Fatal(err)
	}
	// Writes may fail once the upload has given up; either way Close must report it.
	_, _ = w.Write(bytes.Repeat([]byte("x"), 1<<20))
	if _, err := w.Close(); err == nil {
		t.Fatal("expected upload error from Close")
	}
}

type failingBackend struct{ *memBackend }

func (failingBackend) UploadStream(context.Context, string, io.Reader, string, map[string]string) (string, error) {
	return "", errors.New("boom")
}

// A store whose backend can copy copies server-side, sizes the result and refuses a path outside
// the run; a backend that cannot copy answers ok=false so the caller streams.
func TestStoreCopyFromURL(t *testing.T) {
	b := memfs.New()
	b.Put("source/blob.csv", []byte("a,b\n1,2\n"))
	st, err := New(b, "wf", "run")
	if err != nil {
		t.Fatal(err)
	}
	c, ok := st.(Copier)
	if !ok {
		t.Fatal("store must implement Copier")
	}
	path := fileref.RunPrefix("wf", "run") + "node/payload/files/0-blob.csv"
	ref, ok, err := c.CopyFromURL(context.Background(), path, "text/csv", "blob.csv", b.URLFor("source/blob.csv"), 8, 0)
	if err != nil || !ok || ref.Size != 8 || ref.Path != path || ref.ContentType != "text/csv" || ref.FileName != "blob.csv" {
		t.Fatalf("copy: %+v ok=%v err=%v", ref, ok, err)
	}
	if string(b.Blobs[path]) != "a,b\n1,2\n" || len(b.Copies) != 1 {
		t.Fatalf("destination = %q copies %v", b.Blobs[path], b.Copies)
	}
	// Unknown size is read back from the backend.
	ref, _, err = c.CopyFromURL(context.Background(), path+"2", "", "", b.URLFor("source/blob.csv"), -1, 0)
	if err != nil || ref.Size != 8 {
		t.Fatalf("unknown size: %+v %v", ref, err)
	}
	// Another run's path is refused, as for Create.
	if _, _, err := c.CopyFromURL(context.Background(), fileref.RunPrefix("wf", "other")+"x", "", "", b.URLFor("source/blob.csv"), 8, 0); !errors.Is(err, ErrNotInRun) {
		t.Fatalf("path outside the run: %v", err)
	}
	// A refused copy is the not-started error, which the caller answers by streaming.
	if _, _, err := c.CopyFromURL(context.Background(), path+"3", "", "", "https://elsewhere/blob", 8, 0); !errors.Is(err, storage.ErrCopyNotStarted) {
		t.Fatalf("unreadable source: %v", err)
	}
}
