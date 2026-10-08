package jsonops

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
)

// A JSON document delivered as a file parses as it would as base64 (raw payloads).
func TestParseReadsATrustedFile(t *testing.T) {
	doc := `{"name":"alice","age":30}`
	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.json", Size: int64(len(doc))}
	b.Put(ref.Path, []byte(doc))
	store, _ := filestore.New(b, "w", "r")
	schema := json.RawMessage(`{"type":"OBJECT","properties":{"name":{"type":"STRING"},"age":{"type":"NUMBER"}}}`)
	n := &JsonOpsNode{}
	out := n.executeParse(runtime.ProcessInput{
		Ctx: context.Background(), Files: store,
		Data:       map[string]interface{}{"data": fileref.Value(ref)},
		ByteFields: map[string]bool{"data": true},
	}, &Config{Action: "parse", Schema: schema})
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	if out.Data["name"] != "alice" {
		t.Fatalf("parsed %#v", out.Data)
	}
}

// JSON Producer writes /encoded as a file when its port is in FileOutputs, base64 otherwise.
func TestProduceWritesAFileWhenReady(t *testing.T) {
	b := memfs.New()
	store, _ := filestore.New(b, "w", "r")
	n := &JsonOpsNode{}
	in := runtime.ProcessInput{Ctx: context.Background(), Files: store, WorkflowID: "w", RunID: "r", ParentNodeID: "p", NodeId: "j1",
		ItemIndex: -1, Data: map[string]interface{}{"name": "alice"}, FileOutputs: map[string]bool{"/encoded": true}}
	cfg := &Config{Action: "produce", Schema: json.RawMessage(`{"type":"OBJECT","properties":{"name":{"type":"STRING"}}}`)}
	out := n.executeProduce(in, cfg)
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	ref, ok := fileref.Parse(out.Data["encoded"])
	if !ok || ref.Path != "results/w/r/p/j1/encoded.json" {
		t.Fatalf("encoded is not a file: %#v", out.Data["encoded"])
	}
	in.FileOutputs = nil
	if out := n.executeProduce(in, cfg); out.Error != nil || fileref.IsRef(out.Data["encoded"]) {
		t.Fatalf("without file_outputs the output stays base64: %#v", out.Data)
	}
}
