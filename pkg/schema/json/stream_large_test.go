package json

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

// generatedArray streams a JSON document without holding it: a prefix, then n items made by item,
// separated by commas, then a suffix.
type generatedArray struct {
	prefix, suffix string
	n              int
	item           func(i int) string
	i              int
	buf            strings.Builder
	pending        string
	done           bool
}

func (g *generatedArray) Read(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		if g.pending == "" {
			switch {
			case g.done:
				if written == 0 {
					return 0, io.EOF
				}
				return written, nil
			case g.i == 0 && g.prefix != "":
				g.pending, g.prefix = g.prefix, ""
				continue
			case g.i < g.n:
				g.buf.Reset()
				if g.i > 0 {
					g.buf.WriteByte(',')
				}
				g.buf.WriteString(g.item(g.i))
				g.pending = g.buf.String()
				g.i++
			default:
				g.pending, g.done = g.suffix, true
			}
		}
		c := copy(p[written:], g.pending)
		g.pending = g.pending[c:]
		written += c
	}
	return written, nil
}

func peakHeapInuse(stop <-chan struct{}) <-chan uint64 {
	out := make(chan uint64, 1)
	go func() {
		var peak uint64
		var ms runtime.MemStats
		tick := time.NewTicker(10 * time.Millisecond)
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

func largeTests(t *testing.T) {
	if os.Getenv("ICARUS_LARGE_TESTS") == "" {
		t.Skip("set ICARUS_LARGE_TESTS=1 to run")
	}
}

// A 500 MB root array is validated as a stream with a bounded heap, every item reaching OnItem.
func TestValidateStream_500MBRootArrayUnderAMemoryCeiling(t *testing.T) {
	largeTests(t)
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(64 << 20))

	pad := strings.Repeat("x", 200)
	const n = 2_000_000 // ~250 bytes each: ~500 MB
	src := &generatedArray{prefix: "[", suffix: "]", n: n, item: func(i int) string {
		return fmt.Sprintf(`{"id":%d,"email":"user%d@example.com","note":"%s"}`, i, i, pad)
	}}
	s := &Schema{Type: TypeArray, Items: &Property{Type: TypeObject, Properties: map[string]*Property{
		"id":    {Type: TypeNumber, Required: sPtrBool(true)},
		"email": {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("email")}},
		"note":  {Type: TypeString, Validation: &ValidationRules{MaxLength: sPtrFloat(500)}},
	}}}

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	stop := make(chan struct{})
	peak := peakHeapInuse(stop)
	var items int64
	res, err := NewValidator().ValidateStream(json.NewDecoder(src), s, StreamOptions{
		OnItem: func(interface{}, int64) error { items++; return nil },
	})
	close(stop)
	if err != nil || !res.Valid {
		t.Fatalf("err %v, errors %v", err, res.Errors)
	}
	if items != n {
		t.Fatalf("OnItem saw %d items, want %d", items, n)
	}
	grown := int64(<-peak) - int64(base.HeapInuse)
	t.Logf("heap grew by at most %d MiB validating ~500 MB", grown>>20)
	if grown > 48<<20 {
		t.Fatalf("heap grew by %d MiB; the array is not streamed", grown>>20)
	}
}

// uniqueItems catches a duplicate one million items after its first occurrence.
func TestValidateStream_UniqueItemsOneMillionApart(t *testing.T) {
	largeTests(t)
	const n = 1_000_002
	src := &generatedArray{prefix: `{"ids":[`, suffix: "]}", n: n, item: func(i int) string {
		if i == 0 || i == n-1 {
			return `"duplicate"`
		}
		return fmt.Sprintf(`"id-%d"`, i)
	}}
	s := &Schema{Type: TypeObject, Properties: map[string]*Property{
		"ids": {Type: TypeArray, Validation: &ValidationRules{UniqueItems: sPtrBool(true)}, Items: &Property{Type: TypeString}},
	}}
	res, err := NewValidator().ValidateStream(json.NewDecoder(src), s, StreamOptions{CollectAllErrors: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid {
		t.Fatal("a duplicate one million items apart was not caught")
	}
	t.Logf("errors: %v", res.Errors)
}
