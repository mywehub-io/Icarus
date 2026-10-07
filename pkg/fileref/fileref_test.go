package fileref

import (
	"encoding/json"
	"testing"
)

func intp(i int) *int { return &i }

func TestPathFor(t *testing.T) {
	cases := []struct {
		name string
		port string
		node string
		opts PathOptions
		want string
	}{
		{"unit", "payload", "n1", PathOptions{Ext: "csv"}, "results/wf/run/n1/payload.csv"},
		{"leading slash port", "/payload", "n1", PathOptions{Ext: ".hl7"}, "results/wf/run/n1/payload.hl7"},
		{"no ext", "body", "n1", PathOptions{}, "results/wf/run/n1/body.bin"},
		{"iterated", "data", "n1", PathOptions{Ext: "json", Index: intp(7)}, "results/wf/run/n1/data/7.json"},
		{"embedded", "encoded", "e1", PathOptions{Ext: "json", ParentNodeID: "p1"}, "results/wf/run/p1/e1/encoded.json"},
		{"embedded iterated", "encoded", "e1", PathOptions{Ext: "json", ParentNodeID: "p1", Index: intp(2)}, "results/wf/run/p1/e1/encoded/2.json"},
		{"files list", "output", "n1", PathOptions{FileName: "export.csv", Index: intp(3)}, "results/wf/run/n1/output/files/3-export.csv"},
		{"files list sanitised", "output", "n1", PathOptions{FileName: "../../etc/passwd"}, "results/wf/run/n1/output/files/0-..-..-etc-passwd"},
		{"dot node", "payload", "..", PathOptions{Ext: "csv"}, "results/wf/run/node/payload.csv"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := PathFor("wf", "run", c.node, c.port, c.opts)
			if got != c.want {
				t.Fatalf("PathFor = %q, want %q", got, c.want)
			}
			if !InRun(FileRef{Path: got}, "wf", "run") {
				t.Fatalf("PathFor produced a path InRun refuses: %q", got)
			}
		})
	}
}

func TestInRun(t *testing.T) {
	ok := []string{
		"results/wf/run/n1/payload.csv",
		"results/wf/run/n1.zip",
	}
	bad := []string{
		"",
		"results/wf/run/",
		"results/wf/run",
		"results/wf/other/n1/payload.csv",
		"results/other/run/n1/payload.csv",
		"results/wf/run/../other/n1/payload.csv",
		"results/wf/run/./n1/payload.csv",
		"results/wf/run//n1/payload.csv",
		"/results/wf/run/n1/payload.csv",
		"results\\wf\\run\\n1",
		"monitoring/c/p/wf/run/n1.json",
		"results/wf/runX/n1/payload.csv",
	}
	for _, p := range ok {
		if !InRun(FileRef{Path: p}, "wf", "run") {
			t.Errorf("InRun(%q) = false, want true", p)
		}
	}
	for _, p := range bad {
		if InRun(FileRef{Path: p}, "wf", "run") {
			t.Errorf("InRun(%q) = true, want false", p)
		}
	}
	if InRun(FileRef{Path: "results/wf/run/n1/x"}, "", "run") || InRun(FileRef{Path: "results/wf/run/n1/x"}, "wf", "") {
		t.Error("InRun must refuse an empty run identity")
	}
}

func TestParseRoundTrip(t *testing.T) {
	n := int64(12)
	ref := FileRef{Path: "results/wf/run/n1/data.ndjson", Size: 345, ContentType: ContentTypeNDJSON, FileName: "data.ndjson", Records: &n}
	raw, err := MarshalValue(ref)
	if err != nil {
		t.Fatal(err)
	}
	var decoded interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	got, ok := Parse(decoded)
	if !ok {
		t.Fatalf("Parse failed on %s", raw)
	}
	if got.Path != ref.Path || got.Size != ref.Size || got.ContentType != ref.ContentType || got.FileName != ref.FileName || got.Records == nil || *got.Records != 12 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if !got.IsRecords() {
		t.Error("IsRecords false for ndjson")
	}

	// Value form decodes the same.
	viaValue, _ := json.Marshal(Value(ref))
	if string(viaValue) == "" {
		t.Fatal("empty Value")
	}
	var decoded2 interface{}
	_ = json.Unmarshal(viaValue, &decoded2)
	if g2, ok := Parse(decoded2); !ok || g2.Path != ref.Path {
		t.Fatalf("Value form did not parse: %s", viaValue)
	}
	if _, ok := Parse(json.RawMessage(raw)); !ok {
		t.Error("Parse of RawMessage failed")
	}
}

func TestParseRejects(t *testing.T) {
	bad := []interface{}{
		nil,
		"results/wf/run/x",
		map[string]interface{}{},
		map[string]interface{}{"$file": "x"},
		map[string]interface{}{"$file": map[string]interface{}{"size": float64(1)}},
		map[string]interface{}{"$file": map[string]interface{}{"path": "p", "size": float64(-1)}},
		map[string]interface{}{"$file": map[string]interface{}{"path": "p", "size": 1.5}},
		map[string]interface{}{"$file": map[string]interface{}{"path": "p"}},
		// A second key means it is data that happens to contain "$file".
		map[string]interface{}{"$file": map[string]interface{}{"path": "p", "size": float64(1)}, "other": 1},
	}
	for i, v := range bad {
		if _, ok := Parse(v); ok {
			t.Errorf("case %d: Parse accepted %#v", i, v)
		}
	}
}

func TestContentTypes(t *testing.T) {
	cases := map[string]string{
		"text/csv":                 "csv",
		"text/csv; charset=utf-8":  "csv",
		"Application/JSON":         "json",
		"application/x-ndjson":     "ndjson",
		"x-application/hl7-v2+er7": "hl7",
		"application/vnd.duckdb":   "duckdb",
		ContentTypeXLSX:            "xlsx",
		"application/fhir+json":    "json",
		"application/foo+xml":      "xml",
		"":                         "bin",
		"application/unknown-zzz":  "bin",
	}
	for ct, want := range cases {
		if got := ExtensionFor(ct); got != want {
			t.Errorf("ExtensionFor(%q) = %q, want %q", ct, got, want)
		}
	}
	back := map[string]string{
		"csv":         ContentTypeCSV,
		".ndjson":     ContentTypeNDJSON,
		"export.xlsx": ContentTypeXLSX,
		"db.duckdb":   ContentTypeDuckDB,
		"m.hl7":       ContentTypeHL7,
		"GO_1.DAT":    ContentTypeESR,
		"noext":       ContentTypeOctetStream,
		"":            ContentTypeOctetStream,
	}
	for in, want := range back {
		if got := ContentTypeFor(in); got != want {
			t.Errorf("ContentTypeFor(%q) = %q, want %q", in, got, want)
		}
	}
}
