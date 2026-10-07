package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
)

// filestore against Azurite (raw-payloads phase 1 step 4): a 0-byte file, a 100 MB file streamed
// both ways without holding it in memory, Abort leaving no blob, the blob's Content-Type, and a
// path outside the run refused before any request.

type patternReader struct{ remaining int64 }

func (p *patternReader) Read(b []byte) (int, error) {
	if p.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(b)
	if int64(n) > p.remaining {
		n = int(p.remaining)
	}
	for i := 0; i < n; i++ {
		b[i] = byte('a' + (p.remaining-int64(i))%26)
	}
	p.remaining -= int64(n)
	return n, nil
}

func TestFilestoreAzurite(t *testing.T) {
	client, _ := newAzuriteClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	store, err := filestore.New(client, "wf-1", "run-1")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("zero bytes", func(t *testing.T) {
		w, err := store.Create(ctx, "results/wf-1/run-1/n1/empty.txt", "", "empty.txt")
		if err != nil {
			t.Fatal(err)
		}
		ref, err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		if ref.Size != 0 {
			t.Fatalf("size %d", ref.Size)
		}
		if n, err := client.BlobSize(ctx, client.URLFor(ref.Path)); err != nil || n != 0 {
			t.Fatalf("blob size %d, err %v", n, err)
		}
		rc, err := store.Open(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(rc)
		rc.Close()
		if len(b) != 0 {
			t.Fatal("expected empty")
		}
	})

	t.Run("100 MB streamed", func(t *testing.T) {
		const size = 100 << 20
		var before runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)

		path := fileref.PathFor("wf-1", "run-1", "n1", "payload", fileref.PathOptions{Ext: "csv"})
		w, err := store.Create(ctx, path, fileref.ContentTypeCSV, "big.csv")
		if err != nil {
			t.Fatal(err)
		}
		wantHash := sha256.New()
		if _, err := io.Copy(io.MultiWriter(w, wantHash), &patternReader{remaining: size}); err != nil {
			t.Fatal(err)
		}
		ref, err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		if ref.Size != size {
			t.Fatalf("ref size %d", ref.Size)
		}
		props, err := client.BlobSize(ctx, client.URLFor(path))
		if err != nil || props != size {
			t.Fatalf("blob size %d err %v", props, err)
		}

		rc, err := store.Open(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		gotHash := sha256.New()
		n, err := io.Copy(gotHash, rc)
		rc.Close()
		if err != nil || n != size || !bytes.Equal(gotHash.Sum(nil), wantHash.Sum(nil)) {
			t.Fatalf("read back n=%d err=%v hash match=%v", n, err, bytes.Equal(gotHash.Sum(nil), wantHash.Sum(nil)))
		}

		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		// Streaming both ways holds a few chunks, never the file. TotalAlloc would count churn;
		// the live heap is what must stay small.
		if after.HeapInuse > before.HeapInuse+64<<20 {
			t.Fatalf("heap grew %d MiB for a 100 MiB file", (after.HeapInuse-before.HeapInuse)>>20)
		}

		dir := t.TempDir()
		local, err := store.OpenToDisk(ctx, ref, dir)
		if err != nil {
			t.Fatal(err)
		}
		if st, err := os.Stat(local); err != nil || st.Size() != size {
			t.Fatalf("disk copy %v %v", st, err)
		}
	})

	t.Run("abort leaves no blob", func(t *testing.T) {
		path := "results/wf-1/run-1/n2/payload.bin"
		w, err := store.Create(ctx, path, "", "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(w, &patternReader{remaining: 10 << 20}); err != nil {
			t.Fatal(err)
		}
		if err := w.Abort(); err != nil {
			t.Fatal(err)
		}
		if _, err := client.BlobSize(ctx, client.URLFor(path)); err == nil {
			t.Fatal("aborted blob exists")
		}
	})

	t.Run("other run refused", func(t *testing.T) {
		_, err := store.Open(ctx, fileref.FileRef{Path: "results/wf-1/run-2/n1/payload.csv", Size: 1})
		if !errors.Is(err, filestore.ErrNotInRun) {
			t.Fatalf("err = %v", err)
		}
	})
}
