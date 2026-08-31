package resolver

import (
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// The cross-unit half of the array-element grammar matrix. The embedded half lives in
// pkg/embedded/runtime/topology_iterate_test.go; see its header for the three authored spellings and
// why Zeus canonicalises two of them to "<array>//" before they reach the runtime.
//
// Both topologies here resolve through buildInputFromMappings — the live mechanism for a unit whose
// input comes from a prior unit's output. The only difference between them is whether the source node
// id names a prior unit's parent (parent -> parent) or one of its embedded nodes (embedded -> parent);
// the projected-field namespace is per node id either way, so one helper serves both.
//
// The two authored spellings do not fail the same way on this path as they do on the embedded one,
// which is itself the argument for canonicalising at the producer:
//
//	"/names//"      fans out — the canonical form, asserted below
//	"/names/"       fans out here, but delivers nil on the embedded path
//	"/names/$item"  collapses to {"name": ["mona", "amir"]} — one run, with the whole array where a
//	                single element belongs — and never runs at all on the embedded path
//
// A grammar that means three different things in three different readers is not a grammar. After
// canonicalisation only the first spelling reaches any of them.

const (
	canonicalElement    = "/names//"
	authoredPlaceholder = "/names/"
	authoredItem        = "/names/$item"
)

// resolveFromPriorUnit builds a unit's input from a prior unit's flat output, the way a standalone
// plugin's input is built between units.
func resolveFromPriorUnit(t *testing.T, sourceNodeID, endpoint string) map[string]interface{} {
	t.Helper()

	out, err := buildInputFromMappings(BuildInputParams{
		UnitNodeID: "unit-consumer",
		FieldMappings: []message.FieldMapping{{
			SourceNodeID:         sourceNodeID,
			SourceEndpoint:       endpoint,
			SourceSectionId:      "default",
			DestinationEndpoints: []string{"/name"},
			DestinationSectionId: "default",
			DataType:             "FIELD",
			Iterate:              true,
		}},
		SourceResults: map[string]*SourceResult{
			sourceNodeID: {
				NodeID: sourceNodeID,
				Status: "success",
				ProjectedFields: map[string]map[string]interface{}{
					sourceNodeID: {"names": []interface{}{"mona", "amir"}},
				},
				IterationMetadata: map[string]*IterationContext{
					sourceNodeID: {IsArray: true, ArrayLength: 2, ArrayPath: "names"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("buildInputFromMappings: %v", err)
	}

	var decoded interface{}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("resolved input is not valid JSON (%q): %v", string(out), err)
	}
	return map[string]interface{}{"raw": string(out), "decoded": decoded}
}

// assertFansOutTo checks the resolved input is the array-of-objects shape a fanned-out unit runs over,
// carrying one element per array item at the destination path.
func assertFansOutTo(t *testing.T, resolved map[string]interface{}, want ...string) {
	t.Helper()

	rows, ok := resolved["decoded"].([]interface{})
	if !ok {
		t.Fatalf("resolved input is %s, want a JSON array — a fanned-out unit runs over one row per element",
			resolved["raw"])
	}
	if len(rows) != len(want) {
		t.Fatalf("resolved %d rows from %s, want %d", len(rows), resolved["raw"], len(want))
	}

	seen := map[string]int{}
	for _, row := range rows {
		obj, ok := row.(map[string]interface{})
		if !ok {
			t.Fatalf("row is %#v, want an object with the destination field", row)
		}
		s, ok := obj["name"].(string)
		if !ok {
			t.Fatalf("row %#v has no string at /name — the element itself should land there, not a wrapper", obj)
		}
		seen[s]++
	}
	for _, w := range want {
		if seen[w] == 0 {
			t.Fatalf("value %q was never delivered; resolved %s", w, resolved["raw"])
		}
		seen[w]--
	}
}

func TestTopologyEmbeddedToParent_Iterate(t *testing.T) {
	// The source is an embedded node of a prior unit — the shape the failing workflow would take if
	// the JS Runner were its own unit instead of embedded.
	const sourceNodeID = "node-parser-embedded"

	t.Run("canonical element fans out", func(t *testing.T) {
		assertFansOutTo(t, resolveFromPriorUnit(t, sourceNodeID, canonicalElement), "mona", "amir")
	})

	t.Run("authored $item does not reach the runtime", func(t *testing.T) {
		resolved := resolveFromPriorUnit(t, sourceNodeID, authoredItem)
		if _, ok := resolved["decoded"].([]interface{}); ok {
			t.Fatalf("%q fanned out; if the runtime learned this spelling, Zeus's canonicalisation is "+
				"redundant and this test should be rewritten deliberately", authoredItem)
		}
	})

	t.Run("authored $item collapses the array onto one run", func(t *testing.T) {
		resolved := resolveFromPriorUnit(t, sourceNodeID, authoredItem)
		obj, ok := resolved["decoded"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected a single object, got %s", resolved["raw"])
		}
		if _, isArray := obj["name"].([]interface{}); !isArray {
			t.Fatalf("expected the whole array parked at /name, got %s", resolved["raw"])
		}
		// One run, receiving ["mona", "amir"] where the port's element type is STRING. Apollo
		// validated this link as STRING iterate -> STRING scalar; this is the runtime not honouring
		// it, and canonicalisation is what makes it honour it.
	})
}

func TestTopologyParentToParent_Iterate(t *testing.T) {
	// The source is a prior unit's own parent node.
	const sourceNodeID = "node-prior-unit-parent"

	t.Run("canonical element fans out", func(t *testing.T) {
		assertFansOutTo(t, resolveFromPriorUnit(t, sourceNodeID, canonicalElement), "mona", "amir")
	})

	t.Run("authored $item does not reach the runtime", func(t *testing.T) {
		resolved := resolveFromPriorUnit(t, sourceNodeID, authoredItem)
		if _, ok := resolved["decoded"].([]interface{}); ok {
			t.Fatalf("%q fanned out; if the runtime learned this spelling, Zeus's canonicalisation is "+
				"redundant and this test should be rewritten deliberately", authoredItem)
		}
	})
}
