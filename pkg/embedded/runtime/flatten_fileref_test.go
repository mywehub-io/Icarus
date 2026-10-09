package runtime

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/fileref"
)

// A file reference is a single value: flattening keeps it whole under its own key, so a byte
// port is looked up by exact key and never split into path/size/contentType leaves.
func TestFlattenKeepsFileRefWhole(t *testing.T) {
	ref := fileref.Value(fileref.FileRef{Path: "wf/run/node/payload.csv", Size: 12, ContentType: "text/csv", FileName: "payload.csv"})
	data := map[string]interface{}{"payload": ref, "meta": map[string]interface{}{"n": 1}}

	for name, flat := range map[string]map[string]interface{}{
		"FlattenMap":          FlattenMap(data, "n1", ""),
		"FlattenMapWithIndex": FlattenMapWithIndex(data, "n1", "", 0),
	} {
		key := "n1-/payload"
		if name == "FlattenMapWithIndex" {
			key = "n1-/0/payload"
		}
		got, ok := flat[key]
		if !ok || !fileref.IsRef(got) {
			t.Fatalf("%s: %s is not the file reference: %#v", name, key, flat)
		}
		for k := range flat {
			if len(k) > len(key) && k[:len(key)+1] == key+"/" {
				t.Fatalf("%s: file reference was descended into: %s", name, k)
			}
		}
	}
	if _, ok := FlattenMap(data, "n1", "")["n1-/meta/n"]; !ok {
		t.Fatal("an ordinary object must still be flattened")
	}
}
