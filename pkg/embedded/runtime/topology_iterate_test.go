package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

// The authored graph spells an array element three ways (Apollo's schemaio/portindex.go): "//" inside
// items, a trailing "/" for an array-of-objects element placeholder, and a trailing "/$item" for a
// primitive array's element. Only the first is a spelling this package's path readers recognise as an
// array crossing, so Zeus canonicalises the other two to "<array>//" at the authored -> wire boundary
// (zeus/internal/domain/graphdata/wirepath.go).
//
// These tests pin what the runtime does with each spelling, for both embedded-node topologies:
// parent -> embedded (EmbeddedProcessor.analyzeIterationContext) and embedded -> embedded
// (SubflowProcessor's mid-flow iteration). The canonical form must fan out; the two authored
// spellings must be understood to not, which is the whole reason the canonicalisation exists.
//
// The cross-unit topologies (embedded -> parent, parent -> parent) are covered by the matching test
// in pkg/resolver.

const (
	canonicalElement    = "/names//"
	authoredPlaceholder = "/names/"
	authoredItem        = "/names/$item"
)

// recordingNode echoes its input and records every execution, so a test can assert both how many
// times a node ran and what it received.
type recordingNode struct {
	id     string
	plugin string
	rec    *executionRecorder
}

func (n *recordingNode) NodeId() string     { return n.id }
func (n *recordingNode) PluginType() string { return n.plugin }

func (n *recordingNode) Process(in ProcessInput) ProcessOutput {
	n.rec.record(n.id, in.Data)
	if out, ok := n.rec.canned[n.id]; ok {
		return ProcessOutput{Data: out}
	}
	// Default: echo the input, so the assertion reads the delivered value.
	echoed := make(map[string]interface{}, len(in.Data))
	for k, v := range in.Data {
		echoed[k] = v
	}
	return ProcessOutput{Data: echoed}
}

type executionRecorder struct {
	mu     sync.Mutex
	runs   map[string][]map[string]interface{}
	canned map[string]map[string]interface{}
}

func newExecutionRecorder() *executionRecorder {
	return &executionRecorder{
		runs:   map[string][]map[string]interface{}{},
		canned: map[string]map[string]interface{}{},
	}
}

func (r *executionRecorder) record(nodeID string, data map[string]interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make(map[string]interface{}, len(data))
	for k, v := range data {
		copied[k] = v
	}
	r.runs[nodeID] = append(r.runs[nodeID], copied)
}

func (r *executionRecorder) count(nodeID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.runs[nodeID])
}

// values returns the value delivered at key for each execution of nodeID, in index order. Iteration
// runs concurrently, so callers compare as a set.
func (r *executionRecorder) values(nodeID, key string) []interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]interface{}, 0, len(r.runs[nodeID]))
	for _, run := range r.runs[nodeID] {
		out = append(out, run[key])
	}
	return out
}

type recordingFactory struct{ rec *executionRecorder }

func (f *recordingFactory) Create(c EmbeddedNodeConfig) (EmbeddedNode, error) {
	return &recordingNode{id: c.NodeId, plugin: c.PluginType, rec: f.rec}, nil
}
func (f *recordingFactory) Register(string, NodeCreator) {}
func (f *recordingFactory) HasCreator(string) bool       { return true }
func (f *recordingFactory) RegisteredTypes() []string    { return nil }

func assertSameStrings(t *testing.T, got []interface{}, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("delivered %d values %#v, want %d %#v", len(got), got, len(want), want)
	}
	seen := map[string]int{}
	for _, v := range got {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("delivered a %T (%#v), want a string — the element itself, not a wrapper", v, v)
		}
		seen[s]++
	}
	for _, w := range want {
		if seen[w] == 0 {
			t.Fatalf("value %q was never delivered; got %#v", w, got)
		}
		seen[w]--
	}
}

// TestTopologyParentToEmbedded_Iterate covers a mapping straight from the unit's parent output into
// an embedded node — EmbeddedProcessor.analyzeIterationContext decides the fan-out.
func TestTopologyParentToEmbedded_Iterate(t *testing.T) {
	const (
		parent   = "node-parent"
		consumer = "node-consumer"
	)

	run := func(t *testing.T, endpoint string) *executionRecorder {
		t.Helper()
		rec := newExecutionRecorder()
		proc := NewEmbeddedProcessorWithDefaults(&recordingFactory{rec: rec})

		unit := ExecutionUnit{
			NodeId:     parent,
			Label:      "trigger",
			PluginType: "plugin-triggers",
			EmbeddedNodes: []EmbeddedNodeConfig{{
				NodeId: consumer, Label: "consumer", PluginType: "plugin-js",
				Embeddable: true, Depth: 0, ExecutionOrder: 0,
				FieldMappings: []FieldMapping{{
					SourceNodeId: parent, SourceEndpoint: endpoint,
					DestinationEndpoints: []string{"/name"},
					SourceSectionId:      SectionDefault, DestinationSectionId: SectionDefault,
					DataType: "FIELD", Iterate: true,
				}},
				NodeConfig: NodeConfig{NodeId: consumer, Config: json.RawMessage(`{}`)},
			}},
		}

		parentOutput := map[string]interface{}{"names": []interface{}{"mona", "amir"}}
		if _, _, err := proc.ProcessEmbeddedNodes(context.Background(), parentOutput, unit, nil); err != nil {
			t.Fatalf("ProcessEmbeddedNodes: %v", err)
		}
		return rec
	}

	t.Run("canonical element fans out", func(t *testing.T) {
		rec := run(t, canonicalElement)
		if got := rec.count(consumer); got != 2 {
			t.Fatalf("consumer ran %d times, want 2 (one per element)", got)
		}
		assertSameStrings(t, rec.values(consumer, "name"), "mona", "amir")
	})

	t.Run("authored $item does not reach the runtime", func(t *testing.T) {
		rec := run(t, authoredItem)
		if got := rec.count(consumer); got == 2 {
			t.Fatalf("%q fanned out; if the runtime learned this spelling, Zeus's canonicalisation "+
				"is redundant and this test should be rewritten deliberately", authoredItem)
		}
	})

	t.Run("authored placeholder does not reach the runtime", func(t *testing.T) {
		rec := run(t, authoredPlaceholder)
		delivered := rec.values(consumer, "name")
		for _, v := range delivered {
			if s, ok := v.(string); ok && (s == "mona" || s == "amir") {
				t.Fatalf("%q delivered element values; if the runtime learned this spelling, "+
					"Zeus's canonicalisation is redundant and this test should be rewritten "+
					"deliberately", authoredPlaceholder)
			}
		}
	})
}

// TestTopologyEmbeddedToEmbedded_Iterate covers the failing run: HTTP Trigger -> JSON Parser -> JS
// Runner, where the parser and the JS node are both embedded in the trigger's unit and the parser
// produces the array. SubflowProcessor's mid-flow iteration decides the fan-out.
func TestTopologyEmbeddedToEmbedded_Iterate(t *testing.T) {
	const (
		parent = "node-parent"
		parser = "node-parser"
		js     = "node-js"
	)

	run := func(t *testing.T, endpoint string) *executionRecorder {
		t.Helper()
		rec := newExecutionRecorder()
		rec.canned[parser] = map[string]interface{}{"names": []interface{}{"mona", "amir"}}
		proc := NewEmbeddedProcessorWithDefaults(&recordingFactory{rec: rec})

		unit := ExecutionUnit{
			NodeId:     parent,
			Label:      "Get FHIR message",
			PluginType: "plugin-triggers",
			EmbeddedNodes: []EmbeddedNodeConfig{
				{
					NodeId: parser, Label: "parser", PluginType: "plugin-json-operations",
					Embeddable: true, Depth: 0, ExecutionOrder: 0,
					FieldMappings: []FieldMapping{{
						SourceNodeId: parent, SourceEndpoint: "/payload",
						DestinationEndpoints: []string{"/data"},
						SourceSectionId:      "head_body", DestinationSectionId: SectionDefault,
						DataType: "FIELD",
					}},
					NodeConfig: NodeConfig{NodeId: parser, Config: json.RawMessage(`{}`)},
				},
				{
					NodeId: js, Label: "make names upper", PluginType: "plugin-js",
					Embeddable: true, Depth: 1, ExecutionOrder: 1,
					FieldMappings: []FieldMapping{{
						SourceNodeId: parser, SourceEndpoint: endpoint,
						DestinationEndpoints: []string{"/name"},
						SourceSectionId:      SectionDefault, DestinationSectionId: SectionDefault,
						DataType: "FIELD", Iterate: true,
					}},
					NodeConfig: NodeConfig{NodeId: js, Config: json.RawMessage(`{}`)},
				},
			},
		}

		parentOutput := map[string]interface{}{"payload": `{"names":["mona","amir"]}`}
		if _, _, err := proc.ProcessEmbeddedNodes(context.Background(), parentOutput, unit, nil); err != nil {
			t.Fatalf("ProcessEmbeddedNodes: %v", err)
		}
		return rec
	}

	t.Run("canonical element fans out", func(t *testing.T) {
		rec := run(t, canonicalElement)
		if got := rec.count(parser); got != 1 {
			t.Fatalf("parser ran %d times, want 1", got)
		}
		if got := rec.count(js); got != 2 {
			t.Fatalf("js ran %d times, want 2 (one per element) — this is the run that failed", got)
		}
		assertSameStrings(t, rec.values(js, "name"), "mona", "amir")
	})

	t.Run("authored $item does not reach the runtime", func(t *testing.T) {
		rec := run(t, authoredItem)
		if got := rec.count(js); got != 0 {
			t.Fatalf("%q ran the node %d times; if the runtime learned this spelling, Zeus's "+
				"canonicalisation is redundant and this test should be rewritten deliberately",
				authoredItem, got)
		}
	})

	t.Run("authored placeholder does not reach the runtime", func(t *testing.T) {
		rec := run(t, authoredPlaceholder)
		delivered := rec.values(js, "name")
		for _, v := range delivered {
			if s, ok := v.(string); ok && (s == "mona" || s == "amir") {
				t.Fatalf("%q delivered element values; if the runtime learned this spelling, "+
					"Zeus's canonicalisation is redundant and this test should be rewritten "+
					"deliberately", authoredPlaceholder)
			}
		}
	})
}
