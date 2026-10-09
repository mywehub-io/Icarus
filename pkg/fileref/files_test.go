package fileref

import (
	"encoding/json"
	"testing"
)

func TestFilesItemRoundTrip(t *testing.T) {
	ref := FileRef{Path: "results/w/r/t/payload/files/0-a.csv", Size: 3, ContentType: "text/csv", FileName: "a.csv"}
	b, err := json.Marshal(FilesItem(ref, "upload"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded interface{}
	_ = json.Unmarshal(b, &decoded)
	got, key, ok := ParseFilesItem(decoded)
	if !ok || key != "upload" || got.Path != ref.Path || got.Size != 3 || got.FileName != "a.csv" {
		t.Fatalf("round trip: %v %q %+v from %s", ok, key, got, b)
	}
	if _, _, ok := ParseFilesItem(json.RawMessage(b)); !ok {
		t.Fatal("raw JSON item not read")
	}
	// An item is not a reference: a single-reference reader never opens it.
	if IsRef(decoded) {
		t.Fatal("a list item must not parse as a reference")
	}
	// A plain reference is a valid item with no key.
	if _, key, ok := ParseFilesItem(Value(ref)); !ok || key != "" {
		t.Fatal("a bare reference is an item without a key")
	}
}

func TestParseFilesItemRejects(t *testing.T) {
	for name, v := range map[string]interface{}{
		"extra field":   map[string]interface{}{Key: map[string]interface{}{"path": "p", "size": 1}, FilesItemKey: "k", "x": 1},
		"key not text":  map[string]interface{}{Key: map[string]interface{}{"path": "p", "size": 1}, FilesItemKey: 3},
		"no reference":  map[string]interface{}{FilesItemKey: "k"},
		"not an object": "x",
	} {
		if _, _, ok := ParseFilesItem(v); ok {
			t.Errorf("%s: accepted", name)
		}
	}
}
