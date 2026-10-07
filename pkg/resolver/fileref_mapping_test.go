package resolver

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func refValue() map[string]interface{} {
	return fileref.Value(fileref.FileRef{
		Path: "results/wf/run/n1/payload.csv", Size: 42,
		ContentType: fileref.ContentTypeCSV, FileName: "export.csv",
	})
}

func buildOne(t *testing.T, m message.FieldMapping, fields map[string]interface{}) map[string]interface{} {
	t.Helper()
	out, err := buildInputFromMappings(BuildInputParams{
		UnitNodeID:    "target",
		FieldMappings: []message.FieldMapping{m},
		SourceResults: map[string]*SourceResult{
			"src": {NodeID: "src", Status: "success", ProjectedFields: map[string]map[string]interface{}{"src": fields}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(out, &data); err != nil {
		t.Fatal(err)
	}
	return data
}

// A byte mapping copies the reference to its destination as it is, top level or nested.
func TestByteMappingCopiesTheReference(t *testing.T) {
	for _, dest := range []string{"/payload", "/request/body"} {
		data := buildOne(t, message.FieldMapping{
			SourceNodeID: "src", SourceEndpoint: "/payload", DestinationEndpoints: []string{dest},
			DataType: "FIELD", ValueType: message.ValueTypeByte,
		}, map[string]interface{}{"payload": refValue()})

		got := extractFromPath(data, dest)
		ref, ok := fileref.Parse(normalise(t, got))
		if !ok || ref.Path != "results/wf/run/n1/payload.csv" || ref.Size != 42 || ref.FileName != "export.csv" {
			t.Fatalf("dest %s: reference not passed through: %#v", dest, got)
		}
	}
}

// A byte mapping to the root does not spill the reference's "$file" key into the input: it lands
// under the source port's name.
func TestByteMappingToRootKeepsTheReferenceWhole(t *testing.T) {
	data := buildOne(t, message.FieldMapping{
		SourceNodeID: "src", SourceEndpoint: "/payload", DestinationEndpoints: []string{"/"},
		DataType: "FIELD", ValueType: message.ValueTypeByte,
	}, map[string]interface{}{"payload": refValue()})
	if _, spilled := data[fileref.Key]; spilled {
		t.Fatalf("reference merged into the root: %#v", data)
	}
	if _, ok := fileref.Parse(normalise(t, data["payload"])); !ok {
		t.Fatalf("reference missing under payload: %#v", data)
	}
}

// A "$file" object under a data mapping is data: copied like any object, and the mapping does not
// mark it as a file. Trust comes from ValueType, never from the key.
func TestFileKeyInDataIsJustData(t *testing.T) {
	m := message.FieldMapping{
		SourceNodeID: "src", SourceEndpoint: "/body", DestinationEndpoints: []string{"/body"}, DataType: "FIELD",
	}
	if m.IsFile() {
		t.Fatal("a data mapping must not be a file mapping")
	}
	crafted := map[string]interface{}{fileref.Key: map[string]interface{}{"path": "results/other/run/x", "size": float64(1)}}
	data := buildOne(t, m, map[string]interface{}{"body": crafted})
	if !reflect.DeepEqual(normalise(t, data["body"]), normalise(t, crafted)) {
		t.Fatalf("data changed: %#v", data["body"])
	}
}

// Bridge until the cut: a byte mapping whose value is still a base64 string takes the old path.
func TestByteMappingWithLegacyStringStillWorks(t *testing.T) {
	data := buildOne(t, message.FieldMapping{
		SourceNodeID: "src", SourceEndpoint: "/payload", DestinationEndpoints: []string{"/payload"},
		DataType: "FIELD", ValueType: message.ValueTypeByte,
	}, map[string]interface{}{"payload": "aGVsbG8="})
	if data["payload"] != "aGVsbG8=" {
		t.Fatalf("legacy value lost: %#v", data)
	}
}

func normalise(t *testing.T, v interface{}) interface{} {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
