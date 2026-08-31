package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// capturingLogger keeps warnings so a test can assert what the runtime reported.
type capturingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *capturingLogger) Debug(string, ...Field) {}
func (l *capturingLogger) Info(string, ...Field)  {}
func (l *capturingLogger) Error(string, ...Field) {}
func (l *capturingLogger) Warn(msg string, fields ...Field) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	b.WriteString(msg)
	for _, f := range fields {
		b.WriteString(" ")
		b.WriteString(f.Key)
		b.WriteString("=")
		if s, ok := f.Value.(string); ok {
			b.WriteString(s)
		}
	}
	l.warns = append(l.warns, b.String())
}

func (l *capturingLogger) warnsMentioning(needle string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, w := range l.warns {
		if strings.Contains(w, needle) {
			out = append(out, w)
		}
	}
	return out
}

// TestUnexecutedNodeIsReported: a node that never runs must not leave the unit silently. This is the
// failing run's shape — an authored "$item" path the runtime cannot parse as an array crossing.
func TestUnexecutedNodeIsReported(t *testing.T) {
	const (
		parent = "node-parent"
		parser = "node-parser"
		js     = "node-js"
	)

	rec := newExecutionRecorder()
	rec.canned[parser] = map[string]interface{}{"names": []interface{}{"mona", "amir"}}
	logger := &capturingLogger{}
	cfg := DefaultProcessorConfig()
	cfg.Logger = logger
	proc := NewEmbeddedProcessor(&recordingFactory{rec: rec}, cfg)

	unit := ExecutionUnit{
		NodeId: parent, Label: "trigger", PluginType: "plugin-triggers",
		EmbeddedNodes: []EmbeddedNodeConfig{
			{
				NodeId: parser, Label: "parser", PluginType: "plugin-json-operations",
				Embeddable: true, Depth: 0, ExecutionOrder: 0,
				FieldMappings: []FieldMapping{{
					SourceNodeId: parent, SourceEndpoint: "/payload",
					DestinationEndpoints: []string{"/data"},
					SourceSectionId:      SectionDefault, DestinationSectionId: SectionDefault,
					DataType: "FIELD",
				}},
				NodeConfig: NodeConfig{NodeId: parser, Config: json.RawMessage(`{}`)},
			},
			{
				NodeId: js, Label: "make names upper", PluginType: "plugin-js",
				Embeddable: true, Depth: 1, ExecutionOrder: 1,
				FieldMappings: []FieldMapping{{
					SourceNodeId: parser, SourceEndpoint: authoredItem,
					DestinationEndpoints: []string{"/name"},
					SourceSectionId:      SectionDefault, DestinationSectionId: SectionDefault,
					DataType: "FIELD", Iterate: true,
				}},
				NodeConfig: NodeConfig{NodeId: js, Config: json.RawMessage(`{}`)},
			},
		},
	}

	if _, _, err := proc.ProcessEmbeddedNodes(context.Background(),
		map[string]interface{}{"payload": `{"names":["mona","amir"]}`}, unit, nil); err != nil {
		t.Fatalf("ProcessEmbeddedNodes: %v", err)
	}

	if rec.count(js) != 0 {
		t.Fatalf("precondition failed: the node was expected not to run, but ran %d times", rec.count(js))
	}
	reported := logger.warnsMentioning(js)
	if len(reported) == 0 {
		t.Fatalf("the node never ran and nothing was reported — this is the silence that let a "+
			"dropped node finalise a run as completed. Warnings seen: %#v", logger.warns)
	}
	if !strings.Contains(reported[0], "embedded node never executed") {
		t.Errorf("warning does not name the condition: %q", reported[0])
	}
}

// TestGatedNodeIsNotReported is the other half, and the one that matters for noise: a node whose
// event gate evaluated false was deliberately not run. Reporting it would make the warning useless.
func TestGatedNodeIsNotReported(t *testing.T) {
	const (
		parent   = "node-parent"
		producer = "node-producer"
		gated    = "node-gated"
	)

	rec := newExecutionRecorder()
	rec.canned[producer] = map[string]interface{}{"gate": false}
	logger := &capturingLogger{}
	cfg := DefaultProcessorConfig()
	cfg.Logger = logger
	proc := NewEmbeddedProcessor(&recordingFactory{rec: rec}, cfg)

	unit := ExecutionUnit{
		NodeId: parent, Label: "trigger", PluginType: "plugin-triggers",
		EmbeddedNodes: []EmbeddedNodeConfig{
			{
				NodeId: producer, Label: "condition", PluginType: "plugin-simple-condition",
				Embeddable: true, Depth: 0, ExecutionOrder: 0,
				FieldMappings: []FieldMapping{{
					SourceNodeId: parent, SourceEndpoint: "/payload",
					DestinationEndpoints: []string{"/data"},
					SourceSectionId:      SectionDefault, DestinationSectionId: SectionDefault,
					DataType: "FIELD",
				}},
				NodeConfig: NodeConfig{NodeId: producer, Config: json.RawMessage(`{}`)},
			},
			{
				NodeId: gated, Label: "only on true", PluginType: "plugin-js",
				Embeddable: true, Depth: 1, ExecutionOrder: 1,
				FieldMappings: []FieldMapping{{
					SourceNodeId: producer, SourceEndpoint: "/gate",
					DestinationEndpoints: []string{"/trigger"},
					SourceSectionId:      SectionDefault, DestinationSectionId: SectionDefault,
					DataType: "EVENT", IsEventTrigger: true,
				}},
				NodeConfig: NodeConfig{NodeId: gated, Config: json.RawMessage(`{}`)},
			},
		},
	}

	if _, _, err := proc.ProcessEmbeddedNodes(context.Background(),
		map[string]interface{}{"payload": "x"}, unit, nil); err != nil {
		t.Fatalf("ProcessEmbeddedNodes: %v", err)
	}

	if rec.count(gated) != 0 {
		t.Fatalf("precondition failed: the gated node should not have run, ran %d times", rec.count(gated))
	}
	if reported := logger.warnsMentioning(gated); len(reported) > 0 {
		t.Errorf("a deliberately gated node was reported as never executed, which would make the "+
			"warning noise: %#v", reported)
	}
}
