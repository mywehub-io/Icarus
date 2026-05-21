package runtime

import (
	"testing"
)

func testActiveIterationStack(sourceNodeID string) *IterationStack {
	stack := NewIterationStack()
	stack.Push(&NestedIterationContext{
		SourceNodeId: sourceNodeID,
		ArrayPath:    "data",
		CurrentIndex: 0,
		TotalItems:   1,
		Items:        []interface{}{map[string]interface{}{"value": "x"}},
	})
	return stack
}

func TestFilterNodesByIterationDepth_IncludeIterateConsumerFromContextOutput(t *testing.T) {
	const (
		iterationSourceNode = "pid-node"
		producerNode        = "js-node"
		consumerNode        = "http-node"
	)

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: producerNode,
				FieldMappings: []FieldMapping{
					{SourceNodeId: iterationSourceNode, SourceEndpoint: "/PID/PID-3//PID-3-1", DataType: "FIELD", Iterate: true},
				},
			},
			{
				NodeId: consumerNode,
				FieldMappings: []FieldMapping{
					{SourceNodeId: producerNode, SourceEndpoint: "/url", DataType: "FIELD", Iterate: true},
				},
			},
		},
	}

	iterStack := testActiveIterationStack(iterationSourceNode)
	store := NewNodeOutputStore()
	store.SetOutputAtContext(producerNode, map[string]interface{}{"url": "https://example.org"}, iterStack)

	filtered := sp.filterNodesByIterationDepth([]int{1}, 1, iterStack, store)
	if len(filtered) != 1 || filtered[0] != 1 {
		t.Fatalf("expected consumer node to be included, got %v", filtered)
	}
}

func TestFilterNodesByIterationDepth_ExcludeIterateConsumerWithoutContextOutput(t *testing.T) {
	const (
		iterationSourceNode = "pid-node"
		producerNode        = "js-node"
		consumerNode        = "http-node"
	)

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: consumerNode,
				FieldMappings: []FieldMapping{
					{SourceNodeId: producerNode, SourceEndpoint: "/url", DataType: "FIELD", Iterate: true},
				},
			},
		},
	}

	iterStack := testActiveIterationStack(iterationSourceNode)
	store := NewNodeOutputStore()

	filtered := sp.filterNodesByIterationDepth([]int{0}, 1, iterStack, store)
	if len(filtered) != 0 {
		t.Fatalf("expected consumer node to be excluded without source output, got %v", filtered)
	}
}

func TestFilterNodesByIterationDepth_KeepDirectSourceConsumerFallback(t *testing.T) {
	const iterationSourceNode = "pid-node"

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: "direct-consumer",
				FieldMappings: []FieldMapping{
					{SourceNodeId: iterationSourceNode, SourceEndpoint: "/PID/PID-3", DataType: "FIELD", Iterate: false},
				},
			},
		},
	}

	iterStack := testActiveIterationStack(iterationSourceNode)
	store := NewNodeOutputStore()

	filtered := sp.filterNodesByIterationDepth([]int{0}, 1, iterStack, store)
	if len(filtered) != 1 || filtered[0] != 0 {
		t.Fatalf("expected direct source consumer fallback to remain active, got %v", filtered)
	}
}

func TestFilterNodesByIterationDepth_RootExcludesIterateOnlyEmbeddedConsumer(t *testing.T) {
	sp := &SubflowProcessor{
		parentNodeId: "parent-node",
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: "http-node",
				FieldMappings: []FieldMapping{
					{SourceNodeId: "js-node", SourceEndpoint: "/url", DataType: "FIELD", Iterate: true},
				},
			},
		},
	}

	filtered := sp.filterNodesByIterationDepth([]int{0}, 0, nil, NewNodeOutputStore())
	if len(filtered) != 0 {
		t.Fatalf("expected iterate-only embedded consumer to be skipped at root, got %v", filtered)
	}
}

func TestFilterNodesByIterationDepth_RootAllowsNodeWithNonIterateMapping(t *testing.T) {
	sp := &SubflowProcessor{
		parentNodeId: "parent-node",
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: "mixed-node",
				FieldMappings: []FieldMapping{
					{SourceNodeId: "js-node", SourceEndpoint: "/url", DataType: "FIELD", Iterate: true},
					{SourceNodeId: "js-node", SourceEndpoint: "/status", DataType: "FIELD", Iterate: false},
				},
			},
		},
	}

	filtered := sp.filterNodesByIterationDepth([]int{0}, 0, nil, NewNodeOutputStore())
	if len(filtered) != 1 || filtered[0] != 0 {
		t.Fatalf("expected mixed iterate/non-iterate node to be allowed at root, got %v", filtered)
	}
}

func TestFilterNodesByIterationDepth_RootAllowsParentSourcedIterateNode(t *testing.T) {
	sp := &SubflowProcessor{
		parentNodeId: "parent-node",
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: "parent-consumer",
				FieldMappings: []FieldMapping{
					{SourceNodeId: "parent-node", SourceEndpoint: "/data//id", DataType: "FIELD", Iterate: true},
				},
			},
		},
	}

	iterStack := testActiveIterationStack("parent-node")
	filtered := sp.filterNodesByIterationDepth([]int{0}, 1, iterStack, NewNodeOutputStore())
	if len(filtered) != 1 || filtered[0] != 0 {
		t.Fatalf("expected parent-sourced iterate node to remain eligible in parent iteration context, got %v", filtered)
	}
}

// TestIsConsumedByIteration_ConditionNodeConsumedByEntryIteration verifies that a condition
// node whose sole iterate mapping sources from the PDS JSON parser via "/entry//..." is
// detected as "consumed by" an iteration context that iterates the "entry" array.
// This prevents the condition from running at the outer PID-3 stack depth (Bug 1).
func TestIsConsumedByIteration_ConditionNodeConsumedByEntryIteration(t *testing.T) {
	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: "condition-node",
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:   "pds-json-parser",
						SourceEndpoint: "/entry//resource/managingOrganization/reference",
						DataType:       "FIELD",
						Iterate:        true,
					},
				},
			},
		},
	}

	iterCtx := &NestedIterationContext{
		SourceNodeId: "pds-json-parser",
		ArrayPath:    "entry",
	}

	if !sp.isConsumedByIteration(sp.nodeConfigs[0], iterCtx) {
		t.Fatal("expected condition node to be consumed by the entry iteration")
	}
}

// TestIsConsumedByIteration_NHSCheckerNotConsumedByEntryIteration verifies that the NHS
// num type checker (sourced from PID Num, not PDS JSON parser) is NOT consumed by an
// entry iteration context — it should still run at the outer PID-3 stack depth.
func TestIsConsumedByIteration_NHSCheckerNotConsumedByEntryIteration(t *testing.T) {
	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: "nhs-checker",
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:   "pid-num",
						SourceEndpoint: "/PID/PID-3//PID-3-4",
						DataType:       "FIELD",
						Iterate:        true,
					},
				},
			},
		},
	}

	iterCtx := &NestedIterationContext{
		SourceNodeId: "pds-json-parser",
		ArrayPath:    "entry",
	}

	if sp.isConsumedByIteration(sp.nodeConfigs[0], iterCtx) {
		t.Fatal("expected NHS checker to NOT be consumed by the entry iteration")
	}
}

// TestIsConsumedByIteration_MixedSourceNodeNotConsumed verifies that a node with iterate
// mappings from two different sources is not flagged as consumed — it still needs to run
// at the outer depth for the non-entry-iteration source.
func TestIsConsumedByIteration_MixedSourceNodeNotConsumed(t *testing.T) {
	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: "mixed-node",
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:   "pds-json-parser",
						SourceEndpoint: "/entry//resource/id",
						DataType:       "FIELD",
						Iterate:        true,
					},
					{
						SourceNodeId:   "pid-num",
						SourceEndpoint: "/PID/PID-3//PID-3-1",
						DataType:       "FIELD",
						Iterate:        true,
					},
				},
			},
		},
	}

	iterCtx := &NestedIterationContext{
		SourceNodeId: "pds-json-parser",
		ArrayPath:    "entry",
	}

	if sp.isConsumedByIteration(sp.nodeConfigs[0], iterCtx) {
		t.Fatal("expected mixed-source node to NOT be consumed by the entry iteration")
	}
}

// TestShouldSkipNodeForItem_SkipsWhenAllSourcesAbsent verifies that a node whose only
// default-section source has no stored output (it was skipped upstream) is itself skipped.
// This prevents conditions from running with empty input when the upstream PDS query
// never ran (e.g. because the NHS type-check gate filtered out this iteration item).
func TestShouldSkipNodeForItem_SkipsWhenAllSourcesAbsent(t *testing.T) {
	const (
		sourceNode = "pds-json-parser"
		condNode   = "condition-node"
	)

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: condNode,
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:    sourceNode,
						SourceEndpoint:  "/entry//resource/managingOrganization/reference",
						SourceSectionId: SectionDefault,
						DataType:        "FIELD",
						Iterate:         true,
					},
				},
			},
		},
	}

	config := sp.nodeConfigs[0]
	store := NewNodeOutputStore() // pds-json-parser has no output in this store
	iter := IterationState{IsActive: true, SourceNodeId: "pid-num", ArrayPath: "PID/PID-3"}

	if !sp.shouldSkipNodeForItem(config, store, iter, 0) {
		t.Fatal("expected condition node to be skipped when its source has no output")
	}
}

// TestShouldSkipNodeForItem_DoesNotSkipWhenSourceHasValidOutput verifies that a node is
// NOT skipped when at least one default-section source has valid (non-error) output.
func TestShouldSkipNodeForItem_DoesNotSkipWhenSourceHasValidOutput(t *testing.T) {
	const (
		sourceNode = "pds-json-parser"
		condNode   = "condition-node"
	)

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: condNode,
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:    sourceNode,
						SourceEndpoint:  "/entry//resource/managingOrganization/reference",
						SourceSectionId: SectionDefault,
						DataType:        "FIELD",
						Iterate:         true,
					},
				},
			},
		},
	}

	config := sp.nodeConfigs[0]
	store := NewNodeOutputStore()
	store.SetSingleOutput(sourceNode, map[string]interface{}{
		"entry": []interface{}{
			map[string]interface{}{
				"resource": map[string]interface{}{
					"managingOrganization": map[string]interface{}{
						"reference": "Organization/F86045",
					},
				},
			},
		},
	})
	iter := IterationState{IsActive: true, SourceNodeId: "pid-num", ArrayPath: "PID/PID-3"}

	if sp.shouldSkipNodeForItem(config, store, iter, 0) {
		t.Fatal("expected condition node to NOT be skipped when its source has valid output")
	}
}

// TestShouldSkipNode_SkipsWhenAllSourcesAbsent mirrors
// TestShouldSkipNodeForItem_SkipsWhenAllSourcesAbsent for the non-iteration path:
// a node whose only default-section source has no stored output (the upstream was
// itself skipped) must be skipped instead of running with an empty input map.
func TestShouldSkipNode_SkipsWhenAllSourcesAbsent(t *testing.T) {
	const (
		sourceNode   = "array-error-producer"
		consumerNode = "error-gate-client"
	)

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: consumerNode,
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:    sourceNode,
						SourceEndpoint:  "/encoded",
						SourceSectionId: SectionDefault,
						DataType:        "FIELD",
					},
				},
			},
		},
	}

	config := sp.nodeConfigs[0]
	store := NewNodeOutputStore() // sourceNode has no output in this store

	if !sp.shouldSkipNode(config, store) {
		t.Fatal("expected consumer node to be skipped when its source has no output")
	}
}

// TestShouldSkipNode_DoesNotSkipWhenSourceHasValidOutput mirrors
// TestShouldSkipNodeForItem_DoesNotSkipWhenSourceHasValidOutput for the non-iteration
// path: the consumer must run when its default-section source has produced a valid,
// non-error output.
func TestShouldSkipNode_DoesNotSkipWhenSourceHasValidOutput(t *testing.T) {
	const (
		sourceNode   = "array-error-producer"
		consumerNode = "error-gate-client"
	)

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: consumerNode,
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:    sourceNode,
						SourceEndpoint:  "/encoded",
						SourceSectionId: SectionDefault,
						DataType:        "FIELD",
					},
				},
			},
		},
	}

	config := sp.nodeConfigs[0]
	store := NewNodeOutputStore()
	store.SetSingleOutput(sourceNode, map[string]interface{}{
		"encoded": "PID|1||...",
	})

	if sp.shouldSkipNode(config, store) {
		t.Fatal("expected consumer node to NOT be skipped when its source has valid output")
	}
}

// TestShouldSkipNode_HL7ValidatorReproducer captures the original production
// scenario:
//
//	Hl7 Is Valid (Simple Condition) — emits {true: <hl7>, false: nil}
//	-> Array Error Producer (event-triggered on /false; skipped when /false is nil)
//	-> Error Gate Client (FIELD-mapped from Array Error Producer /encoded only;
//	                      no event mapping; must skip when its only upstream
//	                      source was skipped and therefore has no stored output)
//
// Before the fix in shouldSkipNode, Error Gate Client ran with an empty input
// map and was reported as status:success rather than being absent from the
// sync-completion response.
func TestShouldSkipNode_HL7ValidatorReproducer(t *testing.T) {
	const (
		condNode   = "hl7-is-valid"
		producer   = "array-error-producer"
		gateClient = "error-gate-client"
	)

	sp := &SubflowProcessor{
		nodeConfigs: []EmbeddedNodeConfig{
			{
				NodeId: gateClient,
				FieldMappings: []FieldMapping{
					{
						SourceNodeId:    producer,
						SourceEndpoint:  "/encoded",
						SourceSectionId: SectionDefault,
						DataType:        "FIELD",
					},
				},
			},
		},
	}

	config := sp.nodeConfigs[0]
	store := NewNodeOutputStore()
	// The Simple Condition ran and emitted {true: <hl7>, false: nil}; Array Error
	// Producer was triggered off /false and was skipped, so it is absent from the
	// store — exactly the production scenario.
	store.SetSingleOutput(condNode, map[string]interface{}{
		"true":  "MSH|^~\\&|...",
		"false": nil,
	})

	if !sp.shouldSkipNode(config, store) {
		t.Fatal("expected Error Gate Client to be skipped when Array Error Producer was filtered out upstream")
	}
}
