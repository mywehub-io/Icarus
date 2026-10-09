package jsonops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
	"github.com/wehubfusion/Icarus/pkg/schema"
)

// A JSON document delivered as a file parses.
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

const rootArraySchema = `{"type":"ARRAY","items":{"type":"OBJECT","properties":{
	"email":{"type":"STRING","required":true,"validation":{"format":"email"}},
	"tags":{"type":"ARRAY","validation":{"maxItems":2},"items":{"type":"STRING"}},
	"status":{"type":"STRING","default":"active"}}}}`

func rootArrayInput(t *testing.T, doc string) runtime.ProcessInput {
	t.Helper()
	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.json", Size: int64(len(doc))}
	b.Put(ref.Path, []byte(doc))
	store, _ := filestore.New(b, "w", "r")
	return runtime.ProcessInput{
		Ctx: context.Background(), Files: store,
		Data:       map[string]interface{}{"data": fileref.Value(ref)},
		ByteFields: map[string]bool{"data": true},
	}
}

// A root array streams item by item and gives the items the whole-document path gives.
func TestParseRootArrayMatchesTheWholeDocument(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("[")
	for i := 0; i < 50; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		if i%2 == 0 {
			fmt.Fprintf(&sb, `{"email":"u%d@example.com"}`, i)
		} else {
			fmt.Fprintf(&sb, `{"email":"u%d@example.com","status":"inactive","extra":%d}`, i, i)
		}
	}
	sb.WriteString("]")
	doc := sb.String()

	n := &JsonOpsNode{}
	cfg := &Config{Action: "parse", Schema: json.RawMessage(rootArraySchema)}
	out := n.executeParse(rootArrayInput(t, doc), cfg)
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	whole, err := schema.Shared().ProcessWithSchema([]byte(doc), cfg.Schema, schema.ProcessOptions{
		ApplyDefaults: cfg.GetApplyDefaults(), StructureData: cfg.GetStructureData()})
	if err != nil || !whole.Valid {
		t.Fatalf("whole-document path: %v %#v", err, whole)
	}
	var want interface{}
	_ = json.Unmarshal(whole.Data, &want)
	got, _ := json.Marshal(out.Data[runtime.RootArrayKey])
	var gotV interface{}
	_ = json.Unmarshal(got, &gotV)
	if !reflect.DeepEqual(gotV, want) {
		t.Fatalf("streamed items differ from the whole-document path:\n got %s\nwant %s", got, whole.Data)
	}

	// Non-strict mode keeps invalid items and gives the same transform.
	off := false
	cfg.StrictValidation = &off
	if out := n.executeParse(rootArrayInput(t, `[{"email":5},{}]`), cfg); out.Error != nil {
		t.Fatal(out.Error)
	} else if items := out.Data[runtime.RootArrayKey].([]interface{}); len(items) != 2 || items[1].(map[string]interface{})["status"] != "active" {
		t.Fatalf("non-strict items %#v", items)
	}
}

// Strict validation of a streamed root array reports the item's path and the array rules.
func TestParseRootArrayValidationErrors(t *testing.T) {
	n := &JsonOpsNode{}
	cfg := &Config{Action: "parse", Schema: json.RawMessage(rootArraySchema)}

	items := make([]string, 45)
	for i := range items {
		items[i] = fmt.Sprintf(`{"email":"u%d@example.com"}`, i)
	}
	items[41] = `{"email":"not-an-email"}`
	out := n.executeParse(rootArrayInput(t, "["+strings.Join(items, ",")+"]"), cfg)
	if out.Error == nil || !strings.Contains(out.Error.Error()+fmt.Sprint(out.Error), "[41]") {
		t.Fatalf("want an error at item 41, got %v", out.Error)
	}

	if out := n.executeParse(rootArrayInput(t, `[{"email":"a@b.co","tags":["x","y","z"]}]`), cfg); out.Error == nil {
		t.Fatal("maxItems inside an item not enforced")
	}
	for _, bad := range []string{`{"email":"a@b.co"}`, `[{"email":"a@b.co"}] x`, `[{"email":`} {
		if out := n.executeParse(rootArrayInput(t, bad), cfg); out.Error == nil {
			t.Fatalf("%q: want an error", bad)
		}
	}
}

// A root-array JSON Producer streams its file, byte for byte what the whole-document path encodes.
func TestProduceArrayFileMatchesTheWholeDocument(t *testing.T) {
	items := make([]interface{}, 0, 30)
	for i := 0; i < 30; i++ {
		items = append(items, map[string]interface{}{"email": fmt.Sprintf("u%d@example.com", i), "n": float64(i), "drop": i})
	}
	sch := `{"type":"ARRAY","items":{"type":"OBJECT","properties":{"email":{"type":"STRING","required":true},
		"n":{"type":"NUMBER"},"status":{"type":"STRING","default":"active"}}}}`
	n := &JsonOpsNode{}
	for _, pretty := range []bool{false, true} {
		for _, data := range [][]interface{}{items, {}} {
			cfg := &Config{Action: "produce", Schema: json.RawMessage(sch), Pretty: pretty}
			b := memfs.New()
			store, _ := filestore.New(b, "w", "r")
			in := runtime.ProcessInput{Ctx: context.Background(), Files: store, WorkflowID: "w", RunID: "r",
				ParentNodeID: "p", NodeId: "j1", ItemIndex: -1, Data: map[string]interface{}{"rows": data}}
			out := n.executeProduce(in, cfg)
			if out.Error != nil {
				t.Fatal(out.Error)
			}
			ref, ok := fileref.Parse(out.Data["encoded"])
			if !ok {
				t.Fatalf("encoded is not a file: %#v", out.Data)
			}
			// The document the whole-document path writes: the items with the schema's default.
			full := make([]interface{}, len(data))
			for i, it := range data {
				m := it.(map[string]interface{})
				full[i] = map[string]interface{}{"email": m["email"], "n": m["n"], "status": "active"}
			}
			var want []byte
			if pretty {
				want, _ = json.MarshalIndent(full, "", "  ")
			} else {
				want, _ = json.Marshal(full)
			}
			if got := b.Blobs[ref.Path]; !bytes.Equal(got, want) || ref.Size != int64(len(want)) {
				t.Fatalf("pretty=%v rows=%d:\n got %s\nwant %s", pretty, len(data), got, want)
			}
		}
	}

	bad := append([]interface{}{}, items...)
	bad[7] = map[string]interface{}{"n": 1}
	b := memfs.New()
	store, _ := filestore.New(b, "w", "r")
	out := n.executeProduce(runtime.ProcessInput{Ctx: context.Background(), Files: store, WorkflowID: "w", RunID: "r",
		ParentNodeID: "p", NodeId: "j1", ItemIndex: -1, Data: map[string]interface{}{"rows": bad}},
		&Config{Action: "produce", Schema: json.RawMessage(sch)})
	if out.Error == nil || len(b.Blobs) != 0 {
		t.Fatalf("an invalid item must fail and leave no file: %v %v", out.Error, b.Blobs)
	}
}

// JSON Producer writes /encoded as a file; without a file store the unit fails.
func TestProduceWritesAFileWhenReady(t *testing.T) {
	b := memfs.New()
	store, _ := filestore.New(b, "w", "r")
	n := &JsonOpsNode{}
	in := runtime.ProcessInput{Ctx: context.Background(), Files: store, WorkflowID: "w", RunID: "r", ParentNodeID: "p", NodeId: "j1",
		ItemIndex: -1, Data: map[string]interface{}{"name": "alice"}}
	cfg := &Config{Action: "produce", Schema: json.RawMessage(`{"type":"OBJECT","properties":{"name":{"type":"STRING"}}}`)}
	out := n.executeProduce(in, cfg)
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	ref, ok := fileref.Parse(out.Data["encoded"])
	if !ok || ref.Path != "results/w/r/p/j1/encoded.json" {
		t.Fatalf("encoded is not a file: %#v", out.Data["encoded"])
	}
	in.Files = nil
	if out := n.executeProduce(in, cfg); out.Error == nil {
		t.Fatalf("without a file store the unit must fail: %#v", out.Data)
	}
}
