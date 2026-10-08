package jsonops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
)

// recordsFileInput delivers an .ndjson file (records) or a JSON array file to the field "data".
func fileInput(t *testing.T, b *memfs.Backend, body, contentType string, records int64) runtime.ProcessInput {
	t.Helper()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/in", Size: int64(len(body)), ContentType: contentType}
	if records >= 0 {
		ref.Records = &records
	}
	b.Put(ref.Path, []byte(body))
	store, _ := filestore.New(b, "w", "r")
	return runtime.ProcessInput{
		Ctx: context.Background(), Files: store, WorkflowID: "w", RunID: "r", ParentNodeID: "p", NodeId: "j1", ItemIndex: -1,
		Data:       map[string]interface{}{"data": fileref.Value(ref)},
		ByteFields: map[string]bool{"data": true},
	}
}

func ndjson(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"email":"u%d@example.com"}`+"\n", i)
	}
	return sb.String()
}

// Every node that consumes JSON takes .ndjson: JSON Parser validates each line as one item of the
// root array, and gives the items a JSON array of the same objects gives (raw payloads Q3).
func TestParseReadsAnNDJSONFileAsTheRootArray(t *testing.T) {
	n := &JsonOpsNode{}
	cfg := &Config{Action: "parse", Schema: json.RawMessage(rootArraySchema)}

	lines := ndjson(40)
	asArray := "[" + strings.Join(strings.Split(strings.TrimSpace(lines), "\n"), ",") + "]"
	fromLines := n.executeParse(fileInput(t, memfs.New(), lines, fileref.ContentTypeNDJSON, 40), cfg)
	fromArray := n.executeParse(fileInput(t, memfs.New(), asArray, fileref.ContentTypeJSON, -1), cfg)
	if fromLines.Error != nil || fromArray.Error != nil {
		t.Fatal(fromLines.Error, fromArray.Error)
	}
	a, _ := json.Marshal(fromLines.Data)
	b, _ := json.Marshal(fromArray.Data)
	if !bytes.Equal(a, b) {
		t.Fatalf("an .ndjson input differs from the same array:\n%s\n%s", a, b)
	}

	// A line that fails the schema fails the parse at its position.
	bad := strings.Replace(lines, `{"email":"u7@example.com"}`, `{"email":"nope"}`, 1)
	if out := n.executeParse(fileInput(t, memfs.New(), bad, fileref.ContentTypeNDJSON, 40), cfg); out.Error == nil || !strings.Contains(fmt.Sprint(out.Error), "[7]") {
		t.Fatalf("want an error at item 7, got %v", out.Error)
	}
	// A line that is not JSON fails.
	if out := n.executeParse(fileInput(t, memfs.New(), "{\"email\":\"a@b.co\"}\nnot json\n", fileref.ContentTypeNDJSON, 2), cfg); out.Error == nil {
		t.Fatal("a line that is not JSON must fail")
	}
	// An object root cannot take records.
	objCfg := &Config{Action: "parse", Schema: json.RawMessage(`{"type":"OBJECT","properties":{"email":{"type":"STRING"}}}`)}
	if out := n.executeParse(fileInput(t, memfs.New(), lines, fileref.ContentTypeNDJSON, 40), objCfg); out.Error == nil {
		t.Fatal("an .ndjson input needs a schema whose root is ARRAY")
	}
}

// JSON Producer reads its array from a file, an .ndjson records file or a JSON array, and writes the
// output document without holding either.
func TestProduceReadsTheArrayFromAFile(t *testing.T) {
	const sch = `{"type":"ARRAY","items":{"type":"OBJECT","properties":{"email":{"type":"STRING","required":true},
		"status":{"type":"STRING","default":"active"}}}}`
	n := &JsonOpsNode{}
	cfg := &Config{Action: "produce", Schema: json.RawMessage(sch)}

	lines := ndjson(25)
	asArray := "[" + strings.Join(strings.Split(strings.TrimSpace(lines), "\n"), ",") + "]"
	var outputs []string
	for _, in := range []struct{ body, ct string }{{lines, fileref.ContentTypeNDJSON}, {asArray, fileref.ContentTypeJSON}} {
		b := memfs.New()
		out := n.executeProduce(fileInput(t, b, in.body, in.ct, -1), cfg)
		if out.Error != nil {
			t.Fatal(out.Error)
		}
		ref, ok := fileref.Parse(out.Data["encoded"])
		if !ok {
			t.Fatalf("encoded is not a file: %#v", out.Data)
		}
		outputs = append(outputs, string(b.Blobs[ref.Path]))
	}
	if outputs[0] != outputs[1] {
		t.Fatalf("records and array inputs must give the same document:\n%s\n%s", outputs[0], outputs[1])
	}
	var doc []map[string]interface{}
	if err := json.Unmarshal([]byte(outputs[0]), &doc); err != nil || len(doc) != 25 || doc[3]["status"] != "active" {
		t.Fatalf("document %.200s (%v)", outputs[0], err)
	}

	// An invalid item fails the produce and leaves no file.
	b := memfs.New()
	bad := strings.Replace(lines, `{"email":"u3@example.com"}`, `{"status":"x"}`, 1)
	if out := n.executeProduce(fileInput(t, b, bad, fileref.ContentTypeNDJSON, 25), cfg); out.Error == nil {
		t.Fatal("an item missing a required field must fail")
	}
	for path := range b.Blobs {
		if strings.Contains(path, "encoded") {
			t.Fatalf("a failed produce left %s", path)
		}
	}
}

// A BYTE property that takes its default gets a file: the schema keeps the default as base64, and
// the node decodes it and writes it, so the property carries a file reference (raw payloads Q4).
func TestBYTEDefaultIsWrittenAsAFile(t *testing.T) {
	const sch = `{"type":"OBJECT","properties":{"name":{"type":"STRING"},
		"logo":{"type":"BYTE","default":"aGVsbG8=","validation":{"minLength":3,"maxLength":10}}}}`
	n := &JsonOpsNode{}
	cfg := &Config{Action: "parse", Schema: json.RawMessage(sch)}
	doc := `{"name":"alice"}`

	b := memfs.New()
	in := fileInput(t, b, doc, fileref.ContentTypeJSON, -1)
	out := n.executeParse(in, cfg)
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	ref, ok := fileref.Parse(out.Data["logo"])
	if !ok || ref.Size != 5 || string(b.Blobs[ref.Path]) != "hello" {
		t.Fatalf("logo = %#v (blobs %v)", out.Data["logo"], b.Blobs)
	}

	// A value that is present is not a default and stays as it is.
	in = fileInput(t, memfs.New(), `{"name":"bob","logo":"c29tZQ=="}`, fileref.ContentTypeJSON, -1)
	if out := n.executeParse(in, cfg); out.Error != nil || out.Data["logo"] != "c29tZQ==" {
		t.Fatalf("a supplied value must be left alone: %#v %v", out.Data["logo"], out.Error)
	}

	// Without a file store the default stays the base64 string, as before.
	in = fileInput(t, memfs.New(), doc, fileref.ContentTypeJSON, -1)
	in.Files = nil
	in.ByteFields = nil
	in.Data = map[string]interface{}{"data": json.RawMessage(doc)}
	if out := n.executeParse(in, cfg); out.Error != nil || out.Data["logo"] != "aGVsbG8=" {
		t.Fatalf("without a store the default stays base64: %#v %v", out.Data["logo"], out.Error)
	}

	// The length rules apply to the file's size: a default of 5 bytes fails maxLength 3.
	tight := &Config{Action: "parse", Schema: json.RawMessage(strings.Replace(sch, `"maxLength":10`, `"maxLength":3`, 1))}
	if out := n.executeParse(fileInput(t, memfs.New(), doc, fileref.ContentTypeJSON, -1), tight); out.Error == nil {
		t.Fatal("a default file above maxLength must fail validation")
	}
}
