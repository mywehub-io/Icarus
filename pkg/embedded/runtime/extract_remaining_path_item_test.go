package runtime

import (
	"reflect"
	"testing"
)

// TestExtractRemainingPath_ItemPlaceholder verifies that "$item" as a terminal
// path segment behaves the same as a bare trailing "//" - it returns the
// current element itself rather than doing a literal key lookup.
func TestExtractRemainingPath_ItemPlaceholder(t *testing.T) {
	sp := &SubflowProcessor{}

	t.Run("primitive element wrapped in $value", func(t *testing.T) {
		data := map[string]interface{}{"$value": "JOHN DOE"}
		segments := ParseNestedArrayPath("$item")

		got := sp.extractRemainingPath(data, segments)
		if got != "JOHN DOE" {
			t.Fatalf("expected %q, got %v", "JOHN DOE", got)
		}
	})

	t.Run("object element returned as-is, not looked up by literal key", func(t *testing.T) {
		data := map[string]interface{}{"id": "1", "name": "Alice"}
		segments := ParseNestedArrayPath("$item")

		got := sp.extractRemainingPath(data, segments)
		if !reflect.DeepEqual(got, data) {
			t.Fatalf("expected whole element %v, got %v", data, got)
		}
	})

	t.Run("nested array of primitives terminated by $item", func(t *testing.T) {
		data := map[string]interface{}{
			"assignments": []interface{}{"a", "b", "c"},
		}
		segments := ParseNestedArrayPath("assignments//$item")

		got := sp.extractRemainingPath(data, segments)
		want := []interface{}{"a", "b", "c"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
	})

	t.Run("full field mapping shape: /names//$item source resolves per item", func(t *testing.T) {
		// Mirrors buildItemInput's use: remaining segments after the "names"
		// array segment has already been consumed by the caller.
		segments := ParseNestedArrayPath("/names//$item")
		if len(segments) != 2 || segments[0].Path != "names" || segments[1].Path != ItemPlaceholderKey {
			t.Fatalf("unexpected segments: %+v", segments)
		}

		item := map[string]interface{}{"$value": "John Doe"}
		got := sp.extractRemainingPath(item, segments[1:])
		if got != "John Doe" {
			t.Fatalf("expected %q, got %v", "John Doe", got)
		}
	})
}

// TestBuildItemInput_SourceEndpoint_NamesDoubleSlashItem is the regression test for
// the reported bug: a JS Runner node mapping /names//$item from the flat array
// parser's output over {"names": ["John Doe", "Jane Doe", ...]} must resolve each
// element into the destination field, one per iteration index - not silently nil.
func TestBuildItemInput_SourceEndpoint_NamesDoubleSlashItem(t *testing.T) {
	const iterSourceNodeID = "flat-array-json-parser"
	const jsRunnerNodeID = "upper-js"

	sp := &SubflowProcessor{logger: &NoOpLogger{}}

	names := []interface{}{"John Doe", "Jane Doe", "John Smith", "Jane Smith"}
	iter := IterationState{
		IsActive:     true,
		SourceNodeId: iterSourceNodeID,
		ArrayPath:    "names",
		TotalItems:   len(names),
		Items:        names,
	}

	jsRunnerCfg := EmbeddedNodeConfig{
		NodeId: jsRunnerNodeID,
		FieldMappings: []FieldMapping{
			{
				SourceNodeId:         iterSourceNodeID,
				SourceEndpoint:       "/names//$item",
				DestinationEndpoints: []string{"/name"},
				DataType:             "FIELD",
				Iterate:              true,
			},
		},
	}

	for i, want := range names {
		itemStore := NewNodeOutputStore()
		itemStore.SetCurrentIterationItem(iterSourceNodeID, sp.extractItemDataAsMap(names[i]), i)

		input, shouldSkip := sp.buildItemInput(jsRunnerCfg, itemStore, iter, i)
		if shouldSkip {
			t.Fatalf("item %d: expected shouldSkip=false, got true", i)
		}
		if got := input["name"]; got != want {
			t.Errorf("item %d: expected input[\"name\"]=%q, got %v (full input: %v)", i, want, got, input)
		}
	}
}
