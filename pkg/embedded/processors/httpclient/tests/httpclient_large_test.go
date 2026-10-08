package httpclient_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	embeddedrt "github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
)

const largeBytes = 100 << 20

// patternBackend serves any blob as largeBytes of a repeating pattern and discards uploads,
// counting them, so the test itself holds no file in memory.
type patternBackend struct{ uploaded atomic.Int64 }

func (b *patternBackend) UploadStream(_ context.Context, path string, body io.Reader, _ string, _ map[string]string) (string, error) {
	n, err := io.Copy(io.Discard, body)
	b.uploaded.Add(n)
	return "mem://" + path, err
}

func (b *patternBackend) DownloadRange(_ context.Context, _ string, off, count int64) ([]byte, error) {
	if off+count > largeBytes {
		count = largeBytes - off
	}
	out := make([]byte, count)
	for i := range out {
		out[i] = byte('a' + (off+int64(i))%26)
	}
	return out, nil
}

func (b *patternBackend) URLFor(path string) string                { return "mem://" + path }
func (b *patternBackend) DeleteBlob(context.Context, string) error { return nil }

// patternReader yields n bytes of the same pattern.
type patternReader struct{ off, n int64 }

func (r *patternReader) Read(p []byte) (int, error) {
	if r.off >= r.n {
		return 0, io.EOF
	}
	if rem := r.n - r.off; int64(len(p)) > rem {
		p = p[:rem]
	}
	for i := range p {
		p[i] = byte('a' + (r.off+int64(i))%26)
	}
	r.off += int64(len(p))
	return len(p), nil
}

// peakHeap samples the heap until stop is closed and returns the largest HeapInuse seen.
func peakHeap(stop <-chan struct{}) <-chan uint64 {
	out := make(chan uint64, 1)
	go func() {
		var peak uint64
		var ms runtime.MemStats
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse > peak {
				peak = ms.HeapInuse
			}
			select {
			case <-stop:
				out <- peak
				return
			case <-tick.C:
			}
		}
	}()
	return out
}

// HTTP Client posts a 100 MB payload file and saves a 100 MB response with a bounded heap.
func TestProcess_LargeFilesStreamBothWays(t *testing.T) {
	if os.Getenv("ICARUS_LARGE_TESTS") == "" {
		t.Skip("set ICARUS_LARGE_TESTS=1 to run")
	}
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(48 << 20))

	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		received.Store(n)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(largeBytes))
		_, _ = io.Copy(w, &patternReader{n: largeBytes})
	}))
	defer server.Close()

	backend := &patternBackend{}
	store, _ := filestore.New(backend, "w", "r")
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.bin", Size: largeBytes}
	rawCfg, _ := json.Marshal(map[string]interface{}{"label": "t", "connection": map[string]interface{}{"url": server.URL, "method": "POST"}})

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	stop := make(chan struct{})
	peak := peakHeap(stop)
	out := createTestNode(t, "node1").Process(embeddedrt.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1", Files: store, WorkflowID: "w", RunID: "r",
		ParentNodeID: "p", ItemIndex: -1,
		Data:       map[string]interface{}{"payload": fileref.Value(ref)},
		ByteFields: map[string]bool{"payload": true},
	})
	close(stop)
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	if received.Load() != largeBytes || backend.uploaded.Load() != largeBytes {
		t.Fatalf("sent %d, saved %d; want %d each", received.Load(), backend.uploaded.Load(), largeBytes)
	}
	if body, ok := fileref.Parse(out.Data["body"]); !ok || body.Size != largeBytes {
		t.Fatalf("body is not a %d byte file: %#v", largeBytes, out.Data["body"])
	}
	grown := int64(<-peak) - int64(base.HeapInuse)
	t.Logf("heap grew by at most %d MiB moving %d MiB each way", grown>>20, largeBytes>>20)
	if grown > 40<<20 {
		t.Fatalf("heap grew by %d MiB; the files are not streamed", grown>>20)
	}
}
