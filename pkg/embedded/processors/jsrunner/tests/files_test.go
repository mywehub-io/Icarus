package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
)

// A script reads a file input with file.text/json and writes a file output with file.write
// (raw payloads, phase 5 step 5).
func TestScriptReadsAndWritesFiles(t *testing.T) {
	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.json", Size: 17}
	b.Put(ref.Path, []byte(`{"name":"alice"}` + "\n"))
	store, _ := filestore.New(b, "w", "r")

	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{NodeId: "js1", PluginType: "plugin-js", Embeddable: true})
	if err != nil {
		t.Fatal(err)
	}
	script := `var doc = file.json(input.payload);
var out = file.write("greeting.txt", "text/plain", "hello " + doc.name);
return { name: doc.name, text: file.text(input.payload).trim(), out: out };`
	raw, _ := json.Marshal(map[string]interface{}{"script": script})
	result := node.Process(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: raw, NodeId: "js1", ItemIndex: -1,
		Data: map[string]interface{}{"payload": fileref.Value(ref)}, Files: store,
		WorkflowID: "w", RunID: "r", ParentNodeID: "p",
	})
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	if result.Data["name"] != "alice" || result.Data["text"] != `{"name":"alice"}` {
		t.Fatalf("read wrong: %#v", result.Data)
	}
	out, ok := fileref.Parse(result.Data["out"])
	if !ok || string(b.Blobs[out.Path]) != "hello alice" {
		t.Fatalf("file.write wrong: %#v", result.Data["out"])
	}
	if !fileref.InRun(out, "w", "r") {
		t.Fatalf("written file outside the run: %s", out.Path)
	}
}

// A script refuses a file above SetMaxFileBytes, both to read and to write.
func TestScriptFileLimitIsConfigurable(t *testing.T) {
	jsrunner.SetMaxFileBytes(8)
	defer jsrunner.SetMaxFileBytes(0)

	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.txt", Size: 12}
	b.Put(ref.Path, []byte("hello world!"))
	store, _ := filestore.New(b, "w", "r")
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{NodeId: "js1", PluginType: "plugin-js", Embeddable: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, script := range []string{
		`return { t: file.text(input.payload) };`,
		`return { out: file.write("x.txt", "text/plain", "123456789") };`,
	} {
		raw, _ := json.Marshal(map[string]interface{}{"script": script})
		result := node.Process(runtime.ProcessInput{
			Ctx: context.Background(), RawConfig: raw, NodeId: "js1", ItemIndex: -1,
			Data: map[string]interface{}{"payload": fileref.Value(ref)}, Files: store,
			WorkflowID: "w", RunID: "r", ParentNodeID: "p",
		})
		if result.Error == nil {
			t.Fatalf("%s: want the limit refused", script)
		}
	}
}
