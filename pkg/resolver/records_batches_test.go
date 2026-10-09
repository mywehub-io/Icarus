package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func bigRecords(t *testing.T, n int) (context.Context, map[string]interface{}) {
	t.Helper()
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"file":"f%04d.csv","n":%d}`+"\n", i, i)
	}
	cnt := int64(n)
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "csv/data.ndjson", Size: int64(sb.Len()),
		ContentType: fileref.ContentTypeNDJSON, Records: &cnt}
	b := memfs.New()
	b.Put(ref.Path, []byte(sb.String()))
	store, _ := filestore.New(b, "w", "r")
	return filestore.WithStore(context.Background(), store), fileref.Value(ref)
}

var iterFile = []message.FieldMapping{{SourceNodeID: "csv", SourceEndpoint: "/data//file", DestinationEndpoints: []string{"/path"},
	Iterate: true, ValueType: message.ValueTypeRecords, DataType: "FIELD"}}

func results(ref interface{}) map[string]*SourceResult {
	return map[string]*SourceResult{"csv": {NodeID: "csv", Status: "success", RawFlatKeys: map[string]interface{}{"csv-/data": ref}}}
}

// Batches of an over-cap iterated records input, put together, are exactly the whole input.
func TestRecordsBatchesMatchTheWholeInput(t *testing.T) {
	ctx, ref := bigRecords(t, 2503)
	whole, err := NewService(nil).materialiseAndBuild(ctx, BuildInputParams{FieldMappings: iterFile, SourceResults: results(ref)})
	if err != nil {
		t.Fatal(err)
	}
	var want interface{}
	if err := json.Unmarshal(whole, &want); err != nil {
		t.Fatal(err)
	}

	s := NewService(nil).WithRecordsMaterialiseMax(1024)
	_, err = s.materialiseAndBuild(WithRecordsBatching(ctx, 1000), BuildInputParams{FieldMappings: iterFile, SourceResults: results(ref)})
	var batches *RecordsBatches
	if !errors.As(err, &batches) {
		t.Fatalf("want batches, got %v", err)
	}
	defer batches.Close()
	var got interface{}
	sizes := []int{}
	for {
		in, err := batches.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var part interface{}
		if err := json.Unmarshal(in, &part); err != nil {
			t.Fatal(err)
		}
		n, merged := concatInputs(got, part)
		sizes = append(sizes, n)
		got = merged
	}
	if fmt.Sprint(sizes) != "[1000 1000 503]" {
		t.Fatalf("batch sizes %v", sizes)
	}
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(want)
	if string(a) != string(b) {
		t.Fatal("batches differ from the whole input")
	}
}

// Without batching on ctx, or for a whole-array reader, over the cap is still refused.
func TestRecordsOverTheCapStillRefusedWhereBatchingCannotServe(t *testing.T) {
	ctx, ref := bigRecords(t, 50)
	s := NewService(nil).WithRecordsMaterialiseMax(64)
	if _, err := s.materialiseAndBuild(ctx, BuildInputParams{FieldMappings: iterFile, SourceResults: results(ref)}); !errors.Is(err, ErrRecordsTooLarge) {
		t.Fatalf("no batching on ctx: want too large, got %v", err)
	}
	whole := []message.FieldMapping{{SourceNodeID: "csv", SourceEndpoint: "/data", DestinationEndpoints: []string{"/rows"},
		ValueType: message.ValueTypeRecords, DataType: "FIELD"}}
	if _, err := s.materialiseAndBuild(WithRecordsBatching(ctx, 10), BuildInputParams{FieldMappings: whole, SourceResults: results(ref)}); !errors.Is(err, ErrRecordsTooLarge) {
		t.Fatalf("whole-array reader: want too large, got %v", err)
	}
}

// concatInputs appends a batch's input to the inputs so far: a root array is appended, and an
// object's array fields are appended key by key. It returns the batch's item count.
func concatInputs(acc, part interface{}) (int, interface{}) {
	switch p := part.(type) {
	case []interface{}:
		a, _ := acc.([]interface{})
		return len(p), append(a, p...)
	case map[string]interface{}:
		a, _ := acc.(map[string]interface{})
		if a == nil {
			a = map[string]interface{}{}
		}
		n := 0
		for k, v := range p {
			arr, ok := v.([]interface{})
			if !ok {
				a[k] = v
				continue
			}
			prev, _ := a[k].([]interface{})
			a[k] = append(prev, arr...)
			n = len(arr)
		}
		return n, a
	}
	return 0, acc
}

// An iterated records input above the batching threshold is read in batches although it is far
// below the materialise cap; below the threshold, or for a whole-array reader, it is built whole.
func TestIteratedRecordsAboveTheBatchThresholdAreBatched(t *testing.T) {
	ctx, ref := bigRecords(t, 500) // about 20 KB
	s := NewService(nil).WithRecordsBatchAbove(4 << 10)
	bctx := WithRecordsBatching(ctx, 100)

	_, err := s.materialiseAndBuild(bctx, BuildInputParams{FieldMappings: iterFile, SourceResults: results(ref)})
	var batches *RecordsBatches
	if !errors.As(err, &batches) {
		t.Fatalf("an iterated input above the threshold must be batched, got %v", err)
	}
	batches.Close()

	// Below the threshold it is built whole.
	small := NewService(nil).WithRecordsBatchAbove(1 << 20)
	if _, err := small.materialiseAndBuild(bctx, BuildInputParams{FieldMappings: iterFile, SourceResults: results(ref)}); err != nil {
		t.Fatalf("below the threshold nothing is batched: %v", err)
	}

	// Without batching on the context nothing is batched whatever the size.
	if _, err := s.materialiseAndBuild(ctx, BuildInputParams{FieldMappings: iterFile, SourceResults: results(ref)}); err != nil {
		t.Fatalf("no batching on ctx: %v", err)
	}

	// A whole-array reader of the same port needs the array, so the input is built whole.
	whole := append([]message.FieldMapping{}, iterFile...)
	whole = append(whole, message.FieldMapping{SourceNodeID: "csv", SourceEndpoint: "/data", DestinationEndpoints: []string{"/all"},
		ValueType: message.ValueTypeRecords, DataType: "FIELD"})
	if _, err := s.materialiseAndBuild(bctx, BuildInputParams{FieldMappings: whole, SourceResults: results(ref)}); err != nil {
		t.Fatalf("a whole-array reader rules batching out, and the file fits the cap: %v", err)
	}
}
