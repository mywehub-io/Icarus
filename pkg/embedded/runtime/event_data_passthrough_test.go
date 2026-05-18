package runtime

import (
	"context"
	"testing"
)

// GetFieldMappings must include EVENT mappings that aren't gating triggers, so embedded
// nodes can read event values (like /error from a parent's pluginError section) as
// regular input data without being skipped.
func TestGetFieldMappings_IncludesNonTriggerEventMappings(t *testing.T) {
	cfg := EmbeddedNodeConfig{
		FieldMappings: []FieldMapping{
			{SourceNodeId: "p", SourceEndpoint: "/payload", DataType: "FIELD"},
			{SourceNodeId: "p", SourceEndpoint: "/error", SourceSectionId: SectionPluginError, DataType: "EVENT"},
			{SourceNodeId: "c", SourceEndpoint: "/false", DataType: "EVENT", IsEventTrigger: true},
		},
	}

	fields := cfg.GetFieldMappings()
	if len(fields) != 2 {
		t.Fatalf("expected 2 field mappings (FIELD + non-trigger EVENT); got %d: %#v", len(fields), fields)
	}

	hasField, hasNonTriggerEvent := false, false
	for _, m := range fields {
		if m.DataType == "FIELD" && m.SourceEndpoint == "/payload" {
			hasField = true
		}
		if m.DataType == "EVENT" && m.SourceEndpoint == "/error" && !m.IsEventTrigger {
			hasNonTriggerEvent = true
		}
	}
	if !hasField {
		t.Error("expected FIELD mapping in GetFieldMappings result")
	}
	if !hasNonTriggerEvent {
		t.Error("expected non-trigger EVENT mapping in GetFieldMappings result")
	}
}

// Event triggers must NOT appear in GetFieldMappings — they gate execution and are
// returned by GetEventMappings instead.
func TestGetFieldMappings_ExcludesEventTriggers(t *testing.T) {
	cfg := EmbeddedNodeConfig{
		FieldMappings: []FieldMapping{
			{SourceNodeId: "c", SourceEndpoint: "/false", DataType: "EVENT", IsEventTrigger: true},
		},
	}
	if got := cfg.GetFieldMappings(); len(got) != 0 {
		t.Fatalf("expected 0 field mappings (only an event trigger); got %d", len(got))
	}
}

func TestGetEventMappings_OnlyTriggers(t *testing.T) {
	cfg := EmbeddedNodeConfig{
		FieldMappings: []FieldMapping{
			{SourceNodeId: "p", SourceEndpoint: "/error", DataType: "EVENT"}, // non-trigger
			{SourceNodeId: "c", SourceEndpoint: "/false", DataType: "EVENT", IsEventTrigger: true},
		},
	}
	events := cfg.GetEventMappings()
	if len(events) != 1 {
		t.Fatalf("expected only 1 event trigger; got %d", len(events))
	}
	if events[0].SourceEndpoint != "/false" {
		t.Errorf("unexpected event mapping returned: %#v", events[0])
	}
}

// On the parent's success path, embedded nodes that consume /error from a pluginError
// section must see error=false. withImplicitNoErrorDefaults guarantees this without
// touching outputs that already carry an error flag (the failure path).
func TestWithImplicitNoErrorDefaults_AddsKeysWhenMissing(t *testing.T) {
	in := map[string]interface{}{"payload": "abc"}
	out := withImplicitNoErrorDefaults(in)
	if v, ok := out[ErrorOutputKeyError].(bool); !ok || v {
		t.Errorf("expected error=false; got %#v", out[ErrorOutputKeyError])
	}
	if v, ok := out[ErrorOutputKeyDescription].(string); !ok || v != "" {
		t.Errorf("expected errorDescription=''; got %#v", out[ErrorOutputKeyDescription])
	}
	if got := out["payload"]; got != "abc" {
		t.Errorf("expected payload preserved; got %#v", got)
	}
	// Original map must not be mutated (helper returns a shallow copy).
	if _, ok := in[ErrorOutputKeyError]; ok {
		t.Error("input map was mutated; expected withImplicitNoErrorDefaults to return a copy")
	}
}

func TestWithImplicitNoErrorDefaults_PreservesExistingError(t *testing.T) {
	in := map[string]interface{}{
		ErrorOutputKeyError:       true,
		ErrorOutputKeyDescription: "boom",
	}
	out := withImplicitNoErrorDefaults(in)
	if v, _ := out[ErrorOutputKeyError].(bool); !v {
		t.Errorf("expected error=true preserved; got %#v", out[ErrorOutputKeyError])
	}
	if v, _ := out[ErrorOutputKeyDescription].(string); v != "boom" {
		t.Errorf("expected errorDescription='boom' preserved; got %#v", out[ErrorOutputKeyDescription])
	}
}

func TestWithImplicitNoErrorDefaults_NilInput(t *testing.T) {
	out := withImplicitNoErrorDefaults(nil)
	if v, _ := out[ErrorOutputKeyError].(bool); v {
		t.Errorf("expected error=false on nil input; got %#v", out[ErrorOutputKeyError])
	}
}

// --- buildSingleNodeInput pluginError enrichment tests ---
//
// These tests cover the three scenarios where a node's output is stored without
// an "error" key and a downstream embedded node reads from its pluginError section:
//
//   Scenario 2: embedded sibling (intra-unit, non-iteration)
//   Scenario 3: embedded sibling (intra-unit, iteration) — see buildItemInput tests
//   Scenario 4: prior-unit node (cross-unit, seeded via priorUnitOutputs)
//
// Scenario 1 (parent → embedded) already works because processSingleObject enriches
// the parent output before seeding the store.

// TestBuildSingleNodeInput_PluginError_EmbeddedSiblingSuccess verifies that when an
// embedded sibling succeeds with no "error" key in its output (e.g. JSON Producer
// returning {"encoded": "..."}), a downstream node that maps from the sibling's
// pluginError /error section receives error=false rather than an empty input map.
// This is scenario 2 (intra-unit embedded → embedded, non-iteration).
func TestBuildSingleNodeInput_PluginError_EmbeddedSiblingSuccess(t *testing.T) {
	const siblingNodeID = "sibling-node"
	const consumerNodeID = "consumer-node"

	sp := &SubflowProcessor{
		parentNodeId: "parent-node",
		logger:       &NoOpLogger{},
	}

	store := NewNodeOutputStore()
	store.SetSingleOutput(siblingNodeID, map[string]interface{}{"encoded": "abc"})

	consumerCfg := EmbeddedNodeConfig{
		NodeId: consumerNodeID,
		FieldMappings: []FieldMapping{
			{
				SourceNodeId:         siblingNodeID,
				SourceEndpoint:       "/error",
				SourceSectionId:      SectionPluginError,
				DestinationEndpoints: []string{"/error"},
				DataType:             "EVENT",
			},
		},
	}

	input := sp.buildSingleNodeInput(consumerCfg, store)

	errVal, ok := input[ErrorOutputKeyError]
	if !ok {
		t.Fatalf("expected 'error' key in consumer input; got %v", input)
	}
	if v, _ := errVal.(bool); v {
		t.Errorf("expected error=false (sibling succeeded); got %v", errVal)
	}
}

// TestBuildSingleNodeInput_PluginError_EmbeddedSiblingError verifies that when an
// embedded sibling fails and stores {error: true, errorDescription: "boom"}, a
// downstream pluginError consumer receives the real error values unmodified.
// This is the regression guard — failure payloads must not be overwritten.
func TestBuildSingleNodeInput_PluginError_EmbeddedSiblingError(t *testing.T) {
	const siblingNodeID = "sibling-node"
	const consumerNodeID = "consumer-node"

	sp := &SubflowProcessor{
		parentNodeId: "parent-node",
		logger:       &NoOpLogger{},
	}

	store := NewNodeOutputStore()
	store.SetSingleOutput(siblingNodeID, map[string]interface{}{
		ErrorOutputKeyError:       true,
		ErrorOutputKeyDescription: "boom",
	})

	consumerCfg := EmbeddedNodeConfig{
		NodeId: consumerNodeID,
		FieldMappings: []FieldMapping{
			{
				SourceNodeId:         siblingNodeID,
				SourceEndpoint:       "/error",
				SourceSectionId:      SectionPluginError,
				DestinationEndpoints: []string{"/error"},
				DataType:             "EVENT",
			},
			{
				SourceNodeId:         siblingNodeID,
				SourceEndpoint:       "/errorDescription",
				SourceSectionId:      SectionPluginError,
				DestinationEndpoints: []string{"/errorDescription"},
				DataType:             "FIELD",
			},
		},
	}

	input := sp.buildSingleNodeInput(consumerCfg, store)

	if v, _ := input[ErrorOutputKeyError].(bool); !v {
		t.Errorf("expected error=true preserved from failure payload; got %v", input[ErrorOutputKeyError])
	}
	if v, _ := input[ErrorOutputKeyDescription].(string); v != "boom" {
		t.Errorf("expected errorDescription='boom' preserved; got %v", input[ErrorOutputKeyDescription])
	}
}

// TestBuildSingleNodeInput_PluginError_PriorUnitEmbedded verifies the cross-unit
// scenario (scenario 4): a prior unit's embedded node output is seeded into the store
// via priorUnitOutputs with no "error" key, and a current-unit consumer that maps
// from its pluginError /error receives error=false.
func TestBuildSingleNodeInput_PluginError_PriorUnitEmbedded(t *testing.T) {
	const priorEmbeddedNodeID = "prior-unit-json-producer"
	const consumerNodeID = "consumer-node"

	sp := &SubflowProcessor{
		parentNodeId: "parent-node",
		logger:       &NoOpLogger{},
	}

	// Simulate how ProcessItem seeds priorUnitOutputs into the store raw (no enrichment
	// at write time — the gap this fix closes at read time).
	store := NewNodeOutputStore()
	store.SetSingleOutput(priorEmbeddedNodeID, map[string]interface{}{"encoded": "xyz"})

	consumerCfg := EmbeddedNodeConfig{
		NodeId: consumerNodeID,
		FieldMappings: []FieldMapping{
			{
				SourceNodeId:         priorEmbeddedNodeID,
				SourceEndpoint:       "/error",
				SourceSectionId:      SectionPluginError,
				DestinationEndpoints: []string{"/error"},
				DataType:             "EVENT",
			},
		},
	}

	input := sp.buildSingleNodeInput(consumerCfg, store)

	errVal, ok := input[ErrorOutputKeyError]
	if !ok {
		t.Fatalf("expected 'error' key in consumer input for cross-unit scenario; got %v", input)
	}
	if v, _ := errVal.(bool); v {
		t.Errorf("expected error=false (prior-unit node succeeded); got %v", errVal)
	}
}

// --- buildItemInput pluginError enrichment test ---

// TestBuildItemInput_PluginError_EmbeddedSiblingSuccess verifies scenario 3: when
// processing an iteration item, a consumer that maps from a sibling's pluginError
// /error section via the itemStore path receives error=false on success.
func TestBuildItemInput_PluginError_EmbeddedSiblingSuccess(t *testing.T) {
	const iterSourceNodeID = "iter-source"
	const siblingNodeID = "sibling-node"
	const consumerNodeID = "consumer-node"

	sp := &SubflowProcessor{
		parentNodeId: "parent-node",
		logger:       &NoOpLogger{},
	}

	// itemStore holds outputs of nodes already executed for this iteration item.
	// The sibling succeeded with no "error" key — this is what triggers the bug.
	itemStore := NewNodeOutputStore()
	itemStore.SetSingleOutput(siblingNodeID, map[string]interface{}{"encoded": "abc"})

	// The iteration source is a different node; this forces the else-if branch in
	// buildItemInput (SourceNodeId != iter.SourceNodeId).
	iter := IterationState{
		IsActive:     true,
		SourceNodeId: iterSourceNodeID,
		ArrayPath:    "items",
		TotalItems:   1,
		Items:        []interface{}{map[string]interface{}{"val": "x"}},
	}

	consumerCfg := EmbeddedNodeConfig{
		NodeId: consumerNodeID,
		FieldMappings: []FieldMapping{
			{
				SourceNodeId:         siblingNodeID,
				SourceEndpoint:       "/error",
				SourceSectionId:      SectionPluginError,
				DestinationEndpoints: []string{"/error"},
				DataType:             "EVENT",
			},
		},
	}

	input, shouldSkip := sp.buildItemInput(consumerCfg, itemStore, iter, 0)

	if shouldSkip {
		t.Fatal("expected shouldSkip=false; no mappings required deeper iteration")
	}
	errVal, ok := input[ErrorOutputKeyError]
	if !ok {
		t.Fatalf("expected 'error' key in consumer input (iteration path); got %v", input)
	}
	if v, _ := errVal.(bool); v {
		t.Errorf("expected error=false (sibling succeeded, iteration path); got %v", errVal)
	}
}

// --- end-to-end integration: pluginError consumer via SubflowProcessor.ProcessItem ---

// TestProcessItem_PluginError_EmbeddedSiblingSuccess runs a two-node embedded chain
// through the full SubflowProcessor.ProcessItem path: a "producer" node that emits
// only {"encoded": "abc"} (no error key), followed by a SimpleCondition-like
// "consumer" node that reads /error from the producer's pluginError section.
// Before the fix the consumer received empty input and emitted a warning.
// After the fix it should receive error=false.
func TestProcessItem_PluginError_EmbeddedSiblingSuccess(t *testing.T) {
	const parentNodeID = "parent"
	const producerNodeID = "producer"
	const consumerNodeID = "consumer"

	capturedInputs := make(map[string]map[string]interface{})

	// producer: always returns {"encoded": "abc"} (simulates a successful JSON Producer).
	producerCfg := EmbeddedNodeConfig{
		NodeId:         producerNodeID,
		Label:          "producer",
		PluginType:     "plugin-stub-producer",
		Embeddable:     true,
		Depth:          0,
		ExecutionOrder: 1,
		FieldMappings:  []FieldMapping{},
		NodeConfig:     NodeConfig{NodeId: producerNodeID},
	}
	// consumer: reads /error from producer's pluginError section, stores in /gotError.
	consumerCfg := EmbeddedNodeConfig{
		NodeId:         consumerNodeID,
		Label:          "consumer",
		PluginType:     "plugin-stub-consumer",
		Embeddable:     true,
		Depth:          1,
		ExecutionOrder: 2,
		FieldMappings: []FieldMapping{
			{
				SourceNodeId:         producerNodeID,
				SourceEndpoint:       "/error",
				SourceSectionId:      SectionPluginError,
				DestinationEndpoints: []string{"/gotError"},
				DataType:             "EVENT",
			},
		},
		NodeConfig: NodeConfig{NodeId: consumerNodeID},
	}

	factory := NewDefaultNodeFactory()
	factory.Register("plugin-stub-producer", func(cfg EmbeddedNodeConfig) (EmbeddedNode, error) {
		return &stubNode{nodeID: cfg.NodeId, pluginType: cfg.PluginType, output: map[string]interface{}{"encoded": "abc"}, captured: capturedInputs}, nil
	})
	factory.Register("plugin-stub-consumer", func(cfg EmbeddedNodeConfig) (EmbeddedNode, error) {
		return &stubNode{nodeID: cfg.NodeId, pluginType: cfg.PluginType, output: map[string]interface{}{"result": "ok"}, captured: capturedInputs}, nil
	})

	sp, err := NewSubflowProcessor(SubflowConfig{
		ParentNodeId: parentNodeID,
		NodeConfigs:  []EmbeddedNodeConfig{producerCfg, consumerCfg},
		Factory:      factory,
	})
	if err != nil {
		t.Fatalf("NewSubflowProcessor: %v", err)
	}

	parentOutput := map[string]interface{}{"payload": "data"}
	result := sp.ProcessItem(context.Background(), BatchItem{Index: 0, Data: parentOutput})
	if result.Error != nil {
		t.Fatalf("ProcessItem failed: %v", result.Error)
	}

	gotError, ok := capturedInputs[consumerNodeID]["gotError"]
	if !ok {
		t.Fatalf("consumer did not receive 'gotError' in input; got %v", capturedInputs[consumerNodeID])
	}
	if v, _ := gotError.(bool); v {
		t.Errorf("expected consumer gotError=false (producer succeeded); got %v", gotError)
	}
}

// stubNode is a minimal EmbeddedNode implementation for tests.
// It records the input it receives and returns a pre-configured output.
type stubNode struct {
	nodeID     string
	pluginType string
	output     map[string]interface{}
	captured   map[string]map[string]interface{}
}

func (n *stubNode) NodeId() string     { return n.nodeID }
func (n *stubNode) PluginType() string { return n.pluginType }

func (n *stubNode) Process(input ProcessInput) ProcessOutput {
	snapshot := make(map[string]interface{}, len(input.Data))
	for k, v := range input.Data {
		snapshot[k] = v
	}
	n.captured[n.nodeID] = snapshot
	return SuccessOutput(n.output)
}
