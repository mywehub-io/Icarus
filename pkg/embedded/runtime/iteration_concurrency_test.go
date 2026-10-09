package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Mid-flow iteration run with workers (NestedIteration.IterationConcurrency) must give exactly the
// output the one-at-a-time run gives: same keys, same values, items in input order. Run with -race.
func TestConcurrentMidFlowIterationMatchesSequential(t *testing.T) {
	const (
		parent = "node-parent"
		parser = "node-parser"
		js     = "node-js"
		after  = "node-after"
	)
	names := make([]interface{}, 25)
	for i := range names {
		names[i] = fmt.Sprintf("person-%02d", i)
	}
	unit := ExecutionUnit{
		NodeId: parent, Label: "trigger", PluginType: "plugin-triggers",
		EmbeddedNodes: []EmbeddedNodeConfig{
			{
				NodeId: parser, Label: "parser", PluginType: "plugin-json-operations",
				Embeddable: true, Depth: 0, ExecutionOrder: 0,
				FieldMappings: []FieldMapping{{SourceNodeId: parent, SourceEndpoint: "/payload",
					DestinationEndpoints: []string{"/data"}, SourceSectionId: "head_body",
					DestinationSectionId: SectionDefault, DataType: "FIELD"}},
				NodeConfig: NodeConfig{NodeId: parser, Config: json.RawMessage(`{}`)},
			},
			{
				NodeId: js, Label: "per item", PluginType: "plugin-js",
				Embeddable: true, Depth: 1, ExecutionOrder: 1,
				FieldMappings: []FieldMapping{{SourceNodeId: parser, SourceEndpoint: "/names//",
					DestinationEndpoints: []string{"/name"}, SourceSectionId: SectionDefault,
					DestinationSectionId: SectionDefault, DataType: "FIELD", Iterate: true}},
				NodeConfig: NodeConfig{NodeId: js, Config: json.RawMessage(`{}`)},
			},
			{
				NodeId: after, Label: "after", PluginType: "plugin-js",
				Embeddable: true, Depth: 2, ExecutionOrder: 2,
				FieldMappings: []FieldMapping{{SourceNodeId: js, SourceEndpoint: "/name",
					DestinationEndpoints: []string{"/again"}, SourceSectionId: SectionDefault,
					DestinationSectionId: SectionDefault, DataType: "FIELD"}},
				NodeConfig: NodeConfig{NodeId: after, Config: json.RawMessage(`{}`)},
			},
		},
	}
	run := func(concurrency int) (string, string, *executionRecorder) {
		rec := newExecutionRecorder()
		rec.canned[parser] = map[string]interface{}{"names": names}
		cfg := DefaultProcessorConfig()
		cfg.NestedIteration.IterationConcurrency = concurrency
		proc := NewEmbeddedProcessor(&recordingFactory{rec: rec}, cfg)
		out, events, err := proc.ProcessEmbeddedNodes(context.Background(),
			map[string]interface{}{"payload": `{}`}, unit, nil)
		if err != nil {
			t.Fatalf("concurrency %d: %v", concurrency, err)
		}
		o, _ := json.Marshal(out)
		e, _ := json.Marshal(events)
		return string(o), string(e), rec
	}

	seqOut, seqEvents, seqRec := run(0)
	if seqRec.count(js) != len(names) {
		t.Fatalf("sequential run: js ran %d times, want %d", seqRec.count(js), len(names))
	}
	if !strings.Contains(seqOut, "person-00") || !strings.Contains(seqOut, "person-24") {
		t.Fatalf("the unit output must carry the per-item values: %.400s", seqOut)
	}
	for i := 0; i < 5; i++ { // a few rounds, so scheduling varies
		parOut, parEvents, parRec := run(8)
		if parRec.count(js) != len(names) || parRec.count(after) != seqRec.count(after) {
			t.Fatalf("concurrent run: js %d, after %d; sequential js %d, after %d",
				parRec.count(js), parRec.count(after), len(names), seqRec.count(after))
		}
		if parOut != seqOut {
			t.Fatalf("round %d: concurrent output differs from sequential\nseq: %.600s\npar: %.600s", i, seqOut, parOut)
		}
		if parEvents != seqEvents {
			t.Fatalf("round %d: concurrent events differ\nseq: %s\npar: %s", i, seqEvents, parEvents)
		}
	}
}
