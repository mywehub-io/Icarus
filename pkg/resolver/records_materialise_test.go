package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func recordsFixture(t *testing.T) (context.Context, map[string]interface{}) {
	t.Helper()
	b := memfs.New()
	body := `{"name":"alice","age":30}` + "\n" + `{"name":"bob","age":41}` + "\n"
	n := int64(2)
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "csv/data.ndjson", Size: int64(len(body)),
		ContentType: fileref.ContentTypeNDJSON, Records: &n}
	b.Put(ref.Path, []byte(body))
	store, _ := filestore.New(b, "w", "r")
	return filestore.WithStore(context.Background(), store), fileref.Value(ref)
}

var recordsMapping = []message.FieldMapping{{SourceNodeID: "csv", SourceEndpoint: "/data//name",
	DestinationEndpoints: []string{"/names"}, Iterate: true, ValueType: message.ValueTypeRecords}}

// A records reference becomes its array, and a mapping into the records extracts as before.
func TestMaterialisedRecordsMapLikeAnArray(t *testing.T) {
	ctx, refVal := recordsFixture(t)
	results := map[string]*SourceResult{"csv": {NodeID: "csv", Status: "success",
		RawFlatKeys: map[string]interface{}{"csv-/data": refVal}}}
	s := NewService(nil)
	if err := s.materialiseRecords(ctx, recordsMapping, results); err != nil {
		t.Fatal(err)
	}
	if arr, ok := results["csv"].RawFlatKeys["csv-/data"].([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("not materialised: %#v", results["csv"].RawFlatKeys)
	}
	out, err := buildInputFromMappings(BuildInputParams{
		FieldMappings: []message.FieldMapping{{SourceNodeID: "csv", SourceEndpoint: "/data//name", DestinationEndpoints: []string{"/names"}, Iterate: true}},
		SourceResults: results,
	})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]interface{}
	_ = json.Unmarshal(out, &doc)
	names, _ := json.Marshal(doc["names"])
	if string(names) != `["alice","bob"]` {
		t.Fatalf("mapping into records gave %s (doc %s)", names, out)
	}
}

func TestRecordsAboveTheCapAreRefused(t *testing.T) {
	ctx, refVal := recordsFixture(t)
	results := map[string]*SourceResult{"csv": {RawFlatKeys: map[string]interface{}{"csv-/data": refVal}}}
	err := NewService(nil).WithRecordsMaterialiseMax(10).materialiseRecords(ctx, recordsMapping, results)
	if !errors.Is(err, ErrRecordsTooLarge) {
		t.Fatalf("want RECORDS_TOO_LARGE_TO_MATERIALISE, got %v", err)
	}
}

func TestNoStoreLeavesReferences(t *testing.T) {
	_, refVal := recordsFixture(t)
	results := map[string]*SourceResult{"csv": {RawFlatKeys: map[string]interface{}{"csv-/data": refVal}}}
	if err := NewService(nil).materialiseRecords(context.Background(), recordsMapping, results); err != nil {
		t.Fatal(err)
	}
	if !fileref.IsRef(results["csv"].RawFlatKeys["csv-/data"]) {
		t.Fatal("without a run store the reference must stay")
	}
}

// D11: a records reference a mapping does not type as RECORDS is data and is never opened.
func TestUntypedMappingLeavesTheReference(t *testing.T) {
	ctx, refVal := recordsFixture(t)
	results := map[string]*SourceResult{"csv": {RawFlatKeys: map[string]interface{}{"csv-/data": refVal},
		ProjectedFields: map[string]map[string]interface{}{"csv": {"data": refVal}}}}
	untyped := []message.FieldMapping{{SourceNodeID: "csv", SourceEndpoint: "/data", DestinationEndpoints: []string{"/rows"}}}
	if err := NewService(nil).materialiseRecords(ctx, untyped, results); err != nil {
		t.Fatal(err)
	}
	if !fileref.IsRef(results["csv"].RawFlatKeys["csv-/data"]) || !fileref.IsRef(results["csv"].ProjectedFields["csv"]["data"]) {
		t.Fatal("an untyped mapping must not open the records file")
	}
}

func TestRecordsPort(t *testing.T) {
	for in, want := range map[string]string{"/data": "/data", "/data//name": "/data", "data//": "/data", "/": ""} {
		if got := recordsPort(in); got != want {
			t.Errorf("recordsPort(%q) = %q, want %q", in, got, want)
		}
	}
}

// A consumer that streams /data keeps the reference for a whole-port mapping into /data, and the
// built input carries it whole; a mapping into the items is still materialised.
func TestStreamingConsumerKeepsTheReference(t *testing.T) {
	ctx, refVal := recordsFixture(t)
	ctx = WithRecordsStreaming(ctx, "/data")
	whole := []message.FieldMapping{{SourceNodeID: "csv", SourceEndpoint: "/data", DestinationEndpoints: []string{"/data"},
		ValueType: message.ValueTypeRecords}}
	results := map[string]*SourceResult{"csv": {NodeID: "csv", Status: "success",
		RawFlatKeys: map[string]interface{}{"csv-/data": refVal}}}
	s := NewService(nil)
	if err := s.materialiseRecords(ctx, whole, results); err != nil {
		t.Fatal(err)
	}
	if !fileref.IsRef(results["csv"].RawFlatKeys["csv-/data"]) {
		t.Fatal("a streaming consumer must keep the reference")
	}
	out, err := buildInputFromMappings(BuildInputParams{FieldMappings: whole, SourceResults: results})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]interface{}
	_ = json.Unmarshal(out, &doc)
	if ref, ok := fileref.Parse(doc["data"]); !ok || !ref.IsRecords() {
		t.Fatalf("the input must carry the reference whole: %s", out)
	}

	other := []message.FieldMapping{{SourceNodeID: "csv", SourceEndpoint: "/data", DestinationEndpoints: []string{"/rows"},
		ValueType: message.ValueTypeRecords}}
	if err := s.materialiseRecords(ctx, other, results); err != nil {
		t.Fatal(err)
	}
	if _, ok := results["csv"].RawFlatKeys["csv-/data"].([]interface{}); !ok {
		t.Fatal("a destination the consumer does not stream must be materialised")
	}
}
