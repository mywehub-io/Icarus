package resolver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// The completeness rule is the single correctness constraint the read path rests on, and
// breaking it raises no error: the extraction code downstream sizes arrays from the
// highest index it can see, so an under-fetch silently yields a shorter array. These tests
// assert the planner's verdict directly, one row of the rule's table at a time.

func archiveOf(t *testing.T, doc map[string]interface{}) *archive.Reader {
	t.Helper()
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	raw, _, err := archive.Build(b)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := archive.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return r
}

func mapping(node, endpoint string) message.FieldMapping {
	return message.FieldMapping{
		SourceNodeID:         node,
		SourceEndpoint:       endpoint,
		DestinationEndpoints: []string{"/out"},
	}
}

func planKeys(t *testing.T, a *archive.Reader, nodes []string, ms ...message.FieldMapping) ([]string, bool) {
	t.Helper()
	p := planArchiveFetch(a, nodes, ms)
	keys := append([]string(nil), p.keys...)
	sort.Strings(keys)
	return keys, p.wholeFile
}

// Shape A: a scalar or object leaf, and a "//" whose collection entry holds an array, are
// the cases where selective fetch is safe and where the whole win lives.
func TestPlannerFetchesOneKeyForExactLookups(t *testing.T) {
	// Padding keeps the selection well under the whole-file ratio so the plan stays
	// selective; the ratio itself is covered separately below.
	doc := map[string]interface{}{
		"n-/name":    "david",
		"n-/obj":     map[string]interface{}{"a": 1},
		"n-/rows":    []interface{}{map[string]interface{}{"name": "x"}},
		"n-/filler1": largeString(4000),
		"n-/filler2": largeString(4000),
	}
	a := archiveOf(t, doc)

	for _, tc := range []struct {
		name     string
		endpoint string
		want     string
	}{
		{"scalar leaf", "/name", "n-/name"},
		{"object leaf", "/obj", "n-/obj"},
		{"collection over an array-valued entry", "/rows//name", "n-/rows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, whole := planKeys(t, a, []string{"n"}, mapping("n", tc.endpoint))
			if whole {
				t.Fatalf("planner chose a whole-file read for an exact-key lookup")
			}
			if !reflect.DeepEqual(keys, []string{tc.want}) {
				t.Fatalf("planner selected %v, want exactly [%s]", keys, tc.want)
			}
		})
	}
}

// Every route into a prefix scan must widen the fetch to the whole node. Two of the four
// here are invisible in the endpoint string, which is why classification is made against
// the archive and the mapping rather than the path alone.
func TestPlannerFetchesEveryKeyOfANodeWhenExtractionWillScan(t *testing.T) {
	doc := map[string]interface{}{
		"n-/auth[0]/id":   1,
		"n-/auth[1]/id":   2,
		"n-/name":         "david",
		"n-/objCollected": map[string]interface{}{"a": 1},
		"n-/filler":       largeString(20000),
	}
	nodeKeys := []string{"n-/auth[0]/id", "n-/auth[1]/id", "n-/filler", "n-/name", "n-/objCollected"}
	a := archiveOf(t, doc)

	iterated := mapping("n", "/name")
	iterated.Iterate = true

	for _, tc := range []struct {
		name string
		m    message.FieldMapping
		why  string
	}{
		{"root handle", mapping("n", ""), "the whole node is reconstructed from its leaf keys"},
		{"root handle as slash", mapping("n", "/"), "same as the empty endpoint"},
		{"iterate flag set", iterated, "iterate turns extraction into a prefix scan whatever the path says"},
		{"collection over a non-array entry", mapping("n", "/objCollected//a"), "an object falls through into a prefix scan"},
		{"collection over an absent entry", mapping("n", "/missing//a"), "the lookup misses and falls through"},
		{"absent exact key", mapping("n", "/nope"), "structural fallback reconstructs the node"},
		{"indexed family named directly", mapping("n", "/auth"), "no such entry; the indexed keys are scanned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, whole := planKeys(t, a, []string{"n"}, tc.m)
			if whole {
				return // a whole-file read is a superset, so it satisfies the rule
			}
			if !reflect.DeepEqual(keys, nodeKeys) {
				t.Fatalf("planner selected %v, want every key of the node %v — %s",
					keys, nodeKeys, tc.why)
			}
		})
	}
}

// A conservative verdict for one node must not drag another node in the same file up with
// it. One blob holds several nodes because embedded nodes are merged into the parent's
// output before the result is written.
func TestPlannerAppliesTheRulePerSourceNode(t *testing.T) {
	// child-/other is large and unselected, which keeps the selection a small enough share
	// of the payload that the planner stays with ranged reads. Without it the whole-file
	// branch takes over and the test proves nothing about per-node scoping.
	doc := map[string]interface{}{
		"parent-/auth[0]/id": 1,
		"parent-/auth[1]/id": 2,
		"parent-/small":      "p",
		"child-/name":        "david",
		"child-/other":       largeString(60000),
	}
	a := archiveOf(t, doc)

	keys, whole := planKeys(t, a, []string{"parent", "child"},
		mapping("parent", ""),     // forces the whole of parent
		mapping("child", "/name"), // exact key on child
	)
	if whole {
		t.Fatal("planner chose a whole-file read; the fixture no longer exercises per-node scoping")
	}

	want := []string{"parent-/auth[0]/id", "parent-/auth[1]/id", "parent-/small", "child-/name"}
	sort.Strings(want)
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("planner selected %v, want %v — child must not be widened by parent's verdict", keys, want)
	}
}

// A plugin-error mapping is *expected* to miss when the upstream node completed without
// writing /error, and is answered with a synthesised default before any structural
// extraction runs. It must not widen the fetch.
func TestPlannerDoesNotWidenForAnExpectedPluginErrorMiss(t *testing.T) {
	doc := map[string]interface{}{
		"n-/name":   "david",
		"n-/filler": largeString(8000),
	}
	a := archiveOf(t, doc)

	miss := mapping("n", "/"+runtime.ErrorOutputKeyError)
	miss.SourceSectionId = runtime.SectionPluginError

	keys, whole := planKeys(t, a, []string{"n"}, mapping("n", "/name"), miss)
	if whole {
		t.Fatal("an expected plugin-error miss triggered a whole-file read")
	}
	if !reflect.DeepEqual(keys, []string{"n-/name"}) {
		t.Fatalf("planner selected %v, want only [n-/name]", keys)
	}
}

// Mappings naming nodes that are not in this file are not this file's business.
func TestPlannerIgnoresMappingsForOtherFilesAndTriggers(t *testing.T) {
	doc := map[string]interface{}{"n-/name": "david", "n-/filler": largeString(8000)}
	a := archiveOf(t, doc)

	trigger := mapping("n", "")
	trigger.IsEventTrigger = true

	keys, whole := planKeys(t, a, []string{"n"},
		mapping("n", "/name"),
		mapping("elsewhere", ""), // different file: must not widen this one
		trigger,                  // event trigger: not a blob read at all
	)
	if whole {
		t.Fatal("unrelated mappings caused a whole-file read")
	}
	if !reflect.DeepEqual(keys, []string{"n-/name"}) {
		t.Fatalf("planner selected %v, want only [n-/name]", keys)
	}
}

// Once the selection approaches the whole payload, one large request beats many small
// ones. The sizes come from the central directory, so deciding costs no extra fetch.
func TestPlannerPrefersOneRequestWhenNearlyEverythingIsNeeded(t *testing.T) {
	doc := map[string]interface{}{
		"n-/big":   largeString(40000),
		"n-/small": "x",
	}
	a := archiveOf(t, doc)

	if _, whole := planKeys(t, a, []string{"n"}, mapping("n", "/big")); !whole {
		t.Fatal("selecting nearly the whole payload should have chosen a single whole-file read")
	}
	if _, whole := planKeys(t, a, []string{"n"}, mapping("n", "/small")); whole {
		t.Fatal("selecting a tiny fraction should have stayed with ranged reads")
	}
}

func largeString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func TestNodeIDFromFlatKey(t *testing.T) {
	for _, tc := range []struct {
		key  string
		node string
		ok   bool
	}{
		{"abc-/name", "abc", true},
		{"abc-/a//b", "abc", true},
		{"abc-/auth[0]/id", "abc", true},
		{"-/leading", "", false},
		{"nodash", "", false},
		{"", "", false},
	} {
		got, ok := nodeIDFromFlatKey(tc.key)
		if got != tc.node || ok != tc.ok {
			t.Errorf("nodeIDFromFlatKey(%q) = (%q,%v), want (%q,%v)", tc.key, got, ok, tc.node, tc.ok)
		}
	}
}

var _ = fmt.Sprintf
