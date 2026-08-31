package runtime

import (
	"testing"
)

// TestAnalyzeDestinationStructure_IterateTrueNoSlashSlash_StillTriggersArray pins graph-authored-cut
// phases/05-elysium-icarus-cutover.md item 8's acceptance test: a mapping with Iterate:true and a
// destination path containing no "//" must still be treated as array-shaped input, not silently
// fall through to buildSingleInput because the destination string happens to carry no notation.
func TestAnalyzeDestinationStructure_IterateTrueNoSlashSlash_StillTriggersArray(t *testing.T) {
	r := NewOutputResolver()
	mappings := []FieldMapping{
		{
			SourceNodeId:         "n1",
			SourceEndpoint:       "/entries",
			DestinationEndpoints: []string{"/path"}, // no "//" at all
			DataType:             "FIELD",
			Iterate:              true,
		},
	}

	ds := r.analyzeDestinationStructure(mappings)
	if !ds.HasArrayDest {
		t.Fatal("HasArrayDest = false, want true: Iterate:true must trigger array input regardless of destination path shape")
	}
	if ds.ArrayPath != "" {
		t.Fatalf("ArrayPath = %q, want empty (no // to derive it from; buildArrayInput's own default applies)", ds.ArrayPath)
	}
}

// TestAnalyzeDestinationStructure_SlashSlashDestination_StillDerivesArrayPath pins that the
// existing "//"-in-destination path-shape behaviour is unchanged: it still wins (and still
// supplies a precise ArrayPath) when present, regardless of what Iterate says.
func TestAnalyzeDestinationStructure_SlashSlashDestination_StillDerivesArrayPath(t *testing.T) {
	r := NewOutputResolver()
	mappings := []FieldMapping{
		{
			SourceNodeId:         "n1",
			SourceEndpoint:       "/entries",
			DestinationEndpoints: []string{"/rows//name"},
			DataType:             "FIELD",
			Iterate:              true,
		},
	}

	ds := r.analyzeDestinationStructure(mappings)
	if !ds.HasArrayDest {
		t.Fatal("HasArrayDest = false, want true")
	}
	if ds.ArrayPath != "rows" {
		t.Fatalf("ArrayPath = %q, want %q", ds.ArrayPath, "rows")
	}
}

// TestAnalyzeDestinationStructure_IterateFalseNoSlashSlash_NoArray pins "absent means false": no
// mapping has Iterate, and no destination carries "//" — input stays a single object.
func TestAnalyzeDestinationStructure_IterateFalseNoSlashSlash_NoArray(t *testing.T) {
	r := NewOutputResolver()
	mappings := []FieldMapping{
		{
			SourceNodeId:         "n1",
			SourceEndpoint:       "/name",
			DestinationEndpoints: []string{"/name"},
			DataType:             "FIELD",
			Iterate:              false,
		},
	}

	ds := r.analyzeDestinationStructure(mappings)
	if ds.HasArrayDest {
		t.Fatal("HasArrayDest = true, want false")
	}
}

// TestAnalyzeDestinationStructure_EventMappingIterateIgnored pins that an event-trigger mapping's
// Iterate flag (if ever set) does not drive array input — events carry a signal, not iterable data.
func TestAnalyzeDestinationStructure_EventMappingIterateIgnored(t *testing.T) {
	r := NewOutputResolver()
	mappings := []FieldMapping{
		{
			SourceNodeId:         "n1",
			SourceEndpoint:       "/true",
			DestinationEndpoints: []string{""},
			DataType:             "EVENT",
			IsEventTrigger:       true,
			Iterate:              true,
		},
	}

	ds := r.analyzeDestinationStructure(mappings)
	if ds.HasArrayDest {
		t.Fatal("HasArrayDest = true, want false: an event mapping's Iterate must not drive array input")
	}
}

// TestBuildInputForUnit_IterateTrueNoSlashSlash_FansOutIntoDataDefault exercises the full path
// through BuildInputForUnit for the same no-"//"-but-Iterate:true shape, confirming the whole
// resolver (not just analyzeDestinationStructure in isolation) produces array-shaped input under
// the "data" default array path when the destination gives it no more specific one.
func TestBuildInputForUnit_IterateTrueNoSlashSlash_FansOutIntoDataDefault(t *testing.T) {
	r := NewOutputResolver()
	unit := ExecutionUnit{
		NodeId: "n2",
		FieldMappings: []FieldMapping{
			{
				SourceNodeId:         "n1",
				SourceEndpoint:       "/entries",
				DestinationEndpoints: []string{"/path"},
				DataType:             "FIELD",
				Iterate:              true,
			},
		},
	}

	// A single (non-indexed) output — buildArrayInput's length==1-and-not-HasIteration branch
	// wraps buildSingleInput's result into a one-element array under the default "data" path.
	output := StandardUnitOutput{
		"n1-/entries": "value-a",
	}

	got, err := r.BuildInputForUnit(output, unit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := got["data"].([]interface{})
	if !ok {
		t.Fatalf("expected input wrapped under the default \"data\" array path, got %#v", got)
	}
	if len(arr) != 1 {
		t.Fatalf("expected 1 element, got %d", len(arr))
	}
}
