package records

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
)

type mem struct{ blobs map[string][]byte }

func (m *mem) UploadStream(_ context.Context, p string, body io.Reader, _ string, _ map[string]string) (string, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	m.blobs[p] = b
	return p, nil
}
func (m *mem) DownloadRange(_ context.Context, u string, off, n int64) ([]byte, error) {
	b := m.blobs[u]
	end := off + n
	if n == 0 || end > int64(len(b)) {
		end = int64(len(b))
	}
	return b[off:end], nil
}
func (m *mem) URLFor(p string) string                       { return p }
func (m *mem) DeleteBlob(_ context.Context, p string) error { delete(m.blobs, p); return nil }

func TestRoundTrip(t *testing.T) {
	m := &mem{blobs: map[string][]byte{}}
	store, _ := filestore.New(m, "wf", "run")
	ctx := context.Background()
	w, err := Create(ctx, store, "results/wf/run/n1/data.ndjson", "data.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	in := []interface{}{
		map[string]interface{}{"name": "a\nb", "n": float64(1)},
		map[string]interface{}{"name": "<tag>", "big": int64(9007199254740993)},
		"scalar",
	}
	for _, r := range in {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	if ref.Records == nil || *ref.Records != 3 || !ref.IsRecords() {
		t.Fatalf("ref %+v", ref)
	}
	raw := m.blobs[ref.Path]
	if bytes.Count(raw, []byte("\n")) != 3 || !strings.Contains(string(raw), "<tag>") {
		t.Fatalf("not one line per record, or HTML escaped: %q", raw)
	}

	out, err := ReadAll(ctx, store, ref, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 || out[0].(map[string]interface{})["name"] != "a\nb" ||
		out[1].(map[string]interface{})["big"] != int64(9007199254740993) || out[2] != "scalar" {
		t.Fatalf("round trip %#v", out)
	}
}

func TestReadAllRefusesBeforeReading(t *testing.T) {
	m := &mem{blobs: map[string][]byte{}}
	store, _ := filestore.New(m, "wf", "run")
	ref := fileref.FileRef{Path: "results/wf/run/n1/data.ndjson", Size: 1000, ContentType: fileref.ContentTypeNDJSON}
	_, err := ReadAll(context.Background(), store, ref, 999)
	if !errors.Is(err, ErrTooLargeToMaterialise) {
		t.Fatalf("err = %v", err)
	}
}

func TestReaderSkipsBlankLinesAndReportsBadLine(t *testing.T) {
	r := NewReader(io.NopCloser(strings.NewReader("{\"a\":1}\n\n{\"a\":2}\n{bad\n")))
	for i := 0; i < 2; i++ {
		if _, err := r.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Next(); err == nil || !strings.Contains(err.Error(), "line 4") {
		t.Fatalf("want a line 4 error, got %v", err)
	}
}

func TestSliceReader(t *testing.T) {
	r := SliceReader([]interface{}{1, 2})
	n := 0
	for {
		_, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		n++
	}
	if n != 2 {
		t.Fatal(n)
	}
}
