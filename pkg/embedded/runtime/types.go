// Package runtime provides the core types and interfaces for embedded node processing.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// FieldMapping represents the mapping configuration between nodes.
// It defines how data flows from a source node to a destination node.
type FieldMapping struct {
	// SourceNodeId is the ID of the node providing the data
	SourceNodeId string `json:"sourceNodeId"`
	// SourceNodeLabel is the human-readable label of the source node
	SourceNodeLabel string `json:"sourceNodeLabel"`
	// SourceEndpoint is the path to the data in the source node's output
	// Uses "/" for simple paths and "//" for array iteration (e.g., "/data//name")
	SourceEndpoint string `json:"sourceEndpoint"`
	// SourceSectionId identifies which section of the source output to use
	SourceSectionId string `json:"sourceSectionId"`
	// DestinationEndpoints are the paths where data should be placed in the destination
	DestinationEndpoints []string `json:"destinationEndpoints"`
	// DestinationSectionId identifies which section of the destination to populate
	DestinationSectionId string `json:"destinationSectionId"`
	// DataType indicates the type of mapping (e.g., "FIELD", "EVENT")
	DataType string `json:"dataType"`
	// IsEventTrigger indicates if this mapping triggers conditional execution
	IsEventTrigger bool `json:"isEventTrigger,omitempty"`
	// Iterate indicates if this mapping should iterate over array items
	Iterate bool `json:"iterate"`
}

// IsEvent returns true if this mapping is an event trigger
func (f FieldMapping) IsEvent() bool {
	return f.DataType == "EVENT"
}

// HasArrayNotation returns true if the source endpoint contains //
func (f FieldMapping) HasArrayNotation() bool {
	return strings.Contains(f.SourceEndpoint, "//")
}

// Output section names and error-output keys (aligned with execution plan sourceSectionId / endpoints).
const (
	SectionDefault            = "default"          // success output section
	SectionPluginError        = "pluginError"      // error output section
	ErrorOutputKeyError       = "error"            // boolean, true when output is error-only
	ErrorOutputKeyDescription = "errorDescription" // string, error message
)

// IsErrorOnlyOutput returns true if output represents error-only payload (node failed and emitted error output).
func IsErrorOnlyOutput(output map[string]interface{}) bool {
	if output == nil {
		return false
	}
	v, ok := output[ErrorOutputKeyError].(bool)
	return ok && v
}

// EmbeddedNodeConfig represents the configuration for an embedded node in the execution plan.
type EmbeddedNodeConfig struct {
	// NodeId is the unique identifier for this node
	NodeId string `json:"nodeId"`
	// Label is the human-readable name for this node
	Label string `json:"label"`
	// PluginType identifies which processor handles this node
	PluginType string `json:"pluginType"`
	// Action is the node schema action (e.g. "JSON Parser", "CSV Producer"); used to derive action for processors.
	Action string `json:"action,omitempty"`
	// Embeddable indicates if this node can be embedded in a unit
	Embeddable bool `json:"embeddable"`
	// Depth represents the node's position in the execution graph
	Depth int `json:"depth"`
	// ExecutionOrder determines the sequence of execution within a unit
	ExecutionOrder int `json:"executionOrder"`
	// FieldMappings define how data flows into this node
	FieldMappings []FieldMapping `json:"fieldMappings"`
	// NodeConfig contains the node-specific configuration
	NodeConfig NodeConfig `json:"nodeConfig"`
}

// HasIterateMappings returns true if any field mapping has iterate:true
func (e EmbeddedNodeConfig) HasIterateMappings() bool {
	for _, m := range e.FieldMappings {
		if m.Iterate && !m.IsEvent() {
			return true
		}
	}
	return false
}

// GetIterateMappings returns mappings with iterate:true (excluding events)
func (e EmbeddedNodeConfig) GetIterateMappings() []FieldMapping {
	var mappings []FieldMapping
	for _, m := range e.FieldMappings {
		if m.Iterate && !m.IsEvent() {
			mappings = append(mappings, m)
		}
	}
	return mappings
}

// GetEventMappings returns event trigger mappings
func (e EmbeddedNodeConfig) GetEventMappings() []FieldMapping {
	var events []FieldMapping
	for _, m := range e.FieldMappings {
		if m.IsEvent() && m.IsEventTrigger {
			events = append(events, m)
		}
	}
	return events
}

// GetFieldMappings returns mappings that should be resolved as input data.
// This includes ordinary FIELD mappings AND EVENT mappings that are not gating
// triggers (IsEventTrigger=false). Event triggers are returned by GetEventMappings
// and gate execution; non-trigger event mappings carry event values into input data
// (e.g. routing /error from a parent's pluginError section into a downstream node's
// input so it can branch on the value without being skipped).
func (e EmbeddedNodeConfig) GetFieldMappings() []FieldMapping {
	var fields []FieldMapping
	for _, m := range e.FieldMappings {
		if !m.IsEvent() || !m.IsEventTrigger {
			fields = append(fields, m)
		}
	}
	return fields
}

// GetDefaultSectionFieldMappings returns field mappings that read from the source's default (success) section.
func (e EmbeddedNodeConfig) GetDefaultSectionFieldMappings() []FieldMapping {
	var mappings []FieldMapping
	for _, m := range e.FieldMappings {
		if !m.IsEvent() && m.SourceSectionId == SectionDefault {
			mappings = append(mappings, m)
		}
	}
	return mappings
}

// NodeConfig contains the detailed configuration for a node.
type NodeConfig struct {
	// NodeId is the unique identifier (matches parent EmbeddedNodeConfig.NodeId)
	NodeId string `json:"node_id"`
	// WorkflowId is the ID of the workflow this node belongs to
	WorkflowId string `json:"workflow_id"`
	// NodeSchemaId is the ID of the schema defining this node's structure
	NodeSchemaId string `json:"node_schema_id"`
	// Config contains the node-specific configuration as raw JSON
	Config json.RawMessage `json:"config"`
	// CreatedAt is the creation timestamp
	CreatedAt string `json:"created_at,omitempty"`
	// UpdatedAt is the last update timestamp
	UpdatedAt string `json:"updated_at,omitempty"`
}

// ExecutionUnit represents a group of nodes to be executed together.
// It contains a parent node and its embedded nodes.
type ExecutionUnit struct {
	// Index is the position of this unit in the execution plan
	Index int `json:"index"`
	// NodeId is the ID of the parent node
	NodeId string `json:"nodeId"`
	// WorkflowId is the ID of the workflow this execution unit belongs to
	WorkflowId string `json:"workflowId,omitempty"`
	// RunId is the ID of the workflow run
	RunId string `json:"runId,omitempty"`
	// ClientId is the tenant/client identifier for this execution unit
	ClientId string `json:"clientId,omitempty"`
	// ProjectId is the project identifier for observation and multi-tenant isolation
	ProjectId string `json:"projectId,omitempty"`
	// EnvironmentId is the tenant environment identifier (empty = default)
	EnvironmentId string `json:"environmentId,omitempty"`
	// Label is the human-readable name of the parent node
	Label string `json:"label"`
	// Type is the node type (e.g., "plugin")
	Type string `json:"type"`
	// PluginType identifies which processor handles the parent node
	PluginType string `json:"pluginType"`
	// Action is the node schema action (e.g. "HTTP Trigger", "ESR Parser"); used to derive action where applicable.
	Action string `json:"action,omitempty"`
	// Depth represents the unit's position in the execution graph
	Depth int `json:"depth"`
	// Dependencies lists the node IDs this unit depends on
	Dependencies []string `json:"dependencies"`
	// FieldMappings define how data flows into this unit
	FieldMappings []FieldMapping `json:"fieldMappings"`
	// EmbeddedNodes are the nodes to be processed within this unit
	EmbeddedNodes []EmbeddedNodeConfig `json:"embeddedNodes"`
	// NodeConfig contains the parent node's configuration
	NodeConfig NodeConfig `json:"nodeConfig"`
	// IsTrigger indicates if this unit is a workflow trigger
	IsTrigger bool `json:"isTrigger,omitempty"`
}

// EmbeddedNodeLifecycleEmitter emits lifecycle events for embedded nodes and their parent.
// This keeps the embedded runtime decoupled from any specific observation system
// (such as Argus) while still allowing services like Elysium to hook into
// embedded node execution.
type EmbeddedNodeLifecycleEmitter interface {
	// EmitNodeStartEvent is called immediately before an embedded node processes.
	// Emits lifecycle input on node.started.
	EmitNodeStartEvent(ctx context.Context, info EmbeddedNodeIOInfo) error

	// EmitNodeEndEvent is called when a node output is available.
	// It is used for both parent and embedded node terminal events.
	// Emits lifecycle output on node.ended.
	EmitNodeEndEvent(ctx context.Context, info EmbeddedNodeIOInfo) error
}

// EmbeddedNodeStartInfo carries contextual information about an embedded node
// that is about to start execution.
type EmbeddedNodeStartInfo struct {
	WorkflowID     string
	RunID          string
	ClientID       string
	ProjectID       string
	EnvironmentID   string
	ParentNodeID    string
	EmbeddedNodeID  string
	Label           string
}

// EmbeddedNodeEndInfo carries lifecycle information for an embedded node completion.
type EmbeddedNodeEndInfo struct {
	WorkflowID     string
	RunID          string
	ClientID       string
	ProjectID       string
	EnvironmentID   string
	ParentNodeID    string
	EmbeddedNodeID  string
	Label           string
	HasError        bool
	ErrorMessage    string
}

// ParentNodeEndInfo carries contextual information about a parent node whose
// plugin execution has completed and is about to have its embedded nodes
// processed (if any).
type ParentNodeEndInfo struct {
	WorkflowID   string
	RunID        string
	ClientID     string
	ProjectID       string
	EnvironmentID   string
	ParentNodeID    string
	// Label is the human-readable node label from the execution plan (for observation events).
	Label string
	// HasEmbeddedNodes indicates whether this parent has embedded nodes configured.
	HasEmbeddedNodes bool
}

// ParentNodeOutputInfo carries the parent node's output for lifecycle output emission.
type ParentNodeOutputInfo struct {
	WorkflowID   string
	RunID        string
	ClientID     string
	ProjectID       string
	EnvironmentID   string
	ParentNodeID    string
	Label           string
	Output          map[string]interface{}
	HasError        bool
	ErrorMessage    string
}

// EmbeddedNodeIOInfo carries input or output data for an embedded node emission.
type EmbeddedNodeIOInfo struct {
	WorkflowID     string
	RunID          string
	ClientID       string
	ProjectID       string
	EnvironmentID   string
	ParentNodeID    string
	EmbeddedNodeID  string
	Label           string
	Data            map[string]interface{}
	HasError        bool
	ErrorMessage    string
}

// StandardUnitOutput represents the flattened output of a unit as a flat map.
// Keys are formatted as:
//   - Non-iterated: "nodeId-/path" (e.g., "abc-/name")
//   - Single-level iteration: "nodeId-/path[0]" (e.g., "abc-/name[0]", "abc-/name[1]")
//   - Nested iteration: "nodeId-/path[0][1][2]" (e.g., "abc-/chapter[0][0][0]")
type StandardUnitOutput map[string]interface{}

// EventOutputs represents a lightweight subset of StandardUnitOutput used by
// workflow schedulers to evaluate event-trigger conditions without requiring
// full inline payload materialization.
type EventOutputs map[string]interface{}

// NewSingleOutput creates output for non-iterated processing
func NewSingleOutput(data map[string]interface{}) StandardUnitOutput {
	if data == nil {
		data = make(map[string]interface{})
	}
	return StandardUnitOutput(data)
}

// NewIteratedOutput creates output for iterated processing by merging items with index suffixes
func NewIteratedOutput(shared map[string]interface{}, items []map[string]interface{}) StandardUnitOutput {
	result := make(StandardUnitOutput)

	// Add shared (non-iterated) data
	for k, v := range shared {
		result[k] = v
	}

	// Add indexed items
	for i, item := range items {
		for k, v := range item {
			indexedKey := fmt.Sprintf("%s[%d]", k, i)
			result[indexedKey] = v
		}
	}

	return result
}

// NewEmptyOutput creates an empty output
func NewEmptyOutput() StandardUnitOutput {
	return make(StandardUnitOutput)
}

// HasIteration returns true if iteration occurred (keys contain '[' character)
func (o StandardUnitOutput) HasIteration() bool {
	for k := range o {
		if strings.Contains(k, "[") {
			return true
		}
	}
	return false
}

// Len returns the number of items (max index + 1, or 1 if no iteration)
func (o StandardUnitOutput) Len() int {
	maxIndex := -1
	for k := range o {
		if idx := strings.Index(k, "["); idx > 0 {
			// Parse first index from key
			endIdx := strings.Index(k[idx:], "]")
			if endIdx > 1 {
				var index int
				if _, err := fmt.Sscanf(k[idx+1:idx+endIdx], "%d", &index); err == nil {
					if index > maxIndex {
						maxIndex = index
					}
				}
			}
		}
	}
	if maxIndex >= 0 {
		return maxIndex + 1
	}
	return 1
}

// GetValue retrieves a value by key, checking indexed key first then falling back to non-indexed
func (o StandardUnitOutput) GetValue(key string, index int) (interface{}, bool) {
	// Try indexed key first
	indexedKey := fmt.Sprintf("%s[%d]", key, index)
	if val, ok := o[indexedKey]; ok {
		return val, true
	}
	// Fallback to non-indexed key
	if val, ok := o[key]; ok {
		return val, true
	}
	return nil, false
}

// GetItem returns all values at the specified index (strips index from keys)
func (o StandardUnitOutput) GetItem(index int) map[string]interface{} {
	result := make(map[string]interface{})
	indexSuffix := fmt.Sprintf("[%d]", index)

	// Add indexed values
	for k, v := range o {
		if strings.Contains(k, indexSuffix) {
			// Extract base key by removing index suffix
			if idx := strings.Index(k, indexSuffix); idx > 0 {
				baseKey := k[:idx]
				result[baseKey] = v
			}
		}
	}

	// Add non-indexed values if no indexed values found
	if len(result) == 0 {
		for k, v := range o {
			if !strings.Contains(k, "[") {
				result[k] = v
			}
		}
	}

	return result
}

// GetAllItems returns all items grouped by index
func (o StandardUnitOutput) GetAllItems() []map[string]interface{} {
	length := o.Len()
	if length == 1 && !o.HasIteration() {
		// No iteration - return single item with all non-indexed keys
		result := make(map[string]interface{})
		for k, v := range o {
			if !strings.Contains(k, "[") {
				result[k] = v
			}
		}
		if len(result) > 0 {
			return []map[string]interface{}{result}
		}
		return nil
	}

	// Has iteration - group by index
	result := make([]map[string]interface{}, length)
	for i := 0; i < length; i++ {
		result[i] = o.GetItem(i)
	}
	return result
}

// GetAllValues collects all values for a key across all indices
func (o StandardUnitOutput) GetAllValues(key string) []interface{} {
	var values []interface{}

	// Check for indexed variants
	for i := 0; ; i++ {
		indexedKey := fmt.Sprintf("%s[%d]", key, i)
		if val, ok := o[indexedKey]; ok {
			values = append(values, val)
		} else {
			break
		}
	}

	// If no indexed values, check for non-indexed
	if len(values) == 0 {
		if val, ok := o[key]; ok {
			return []interface{}{val}
		}
	}

	return values
}

// GetNodeIds extracts unique node IDs from keys
func (o StandardUnitOutput) GetNodeIds() []string {
	seen := make(map[string]bool)
	var nodeIds []string

	for k := range o {
		// Extract nodeId from "nodeId-/path" format
		if idx := strings.Index(k, "-/"); idx > 0 {
			nodeId := k[:idx]
			if !seen[nodeId] {
				seen[nodeId] = true
				nodeIds = append(nodeIds, nodeId)
			}
		}
	}

	return nodeIds
}

// GetKeysForNode returns all keys for a specific node ID
func (o StandardUnitOutput) GetKeysForNode(nodeId string) []string {
	prefix := nodeId + "-"
	var keys []string

	for k := range o {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	return keys
}

// ExtractIndex parses a key to extract base key and indices
// Returns: baseKey, indices, hasIndex
func ExtractIndex(key string) (string, []int, bool) {
	if !strings.Contains(key, "[") {
		return key, nil, false
	}

	// Find first [
	idx := strings.Index(key, "[")
	if idx < 0 {
		return key, nil, false
	}

	baseKey := key[:idx]
	var indices []int

	// Parse all [n] patterns
	remaining := key[idx:]
	for {
		if len(remaining) == 0 || remaining[0] != '[' {
			break
		}

		endIdx := strings.Index(remaining, "]")
		if endIdx < 0 {
			break
		}

		var index int
		if _, err := fmt.Sscanf(remaining[1:endIdx], "%d", &index); err == nil {
			indices = append(indices, index)
		}

		remaining = remaining[endIdx+1:]
	}

	return baseKey, indices, len(indices) > 0
}

// NestedIterationContext represents a single level of array iteration
type NestedIterationContext struct {
	Depth             int                    // Nesting depth (0 = first level)
	SourceNodeId      string                 // Node that provided the array
	InitiatedByNodeId string                 // Node that started iterating
	ArrayPath         string                 // Path to the array (e.g., "data", "assignments")
	CurrentIndex      int                    // Current item index being processed
	TotalItems        int                    // Total number of items
	Items             []interface{}          // The array items
	ItemData          map[string]interface{} // Current item as map
}

// IterationStack manages nested iteration contexts
type IterationStack struct {
	contexts []*NestedIterationContext
}

// NewIterationStack creates an empty iteration stack
func NewIterationStack() *IterationStack {
	return &IterationStack{
		contexts: []*NestedIterationContext{},
	}
}

// Push adds a new iteration context to the stack
func (s *IterationStack) Push(ctx *NestedIterationContext) {
	ctx.Depth = len(s.contexts)
	s.contexts = append(s.contexts, ctx)
}

// Pop removes the top context from the stack
func (s *IterationStack) Pop() {
	if len(s.contexts) > 0 {
		s.contexts = s.contexts[:len(s.contexts)-1]
	}
}

// Current returns the current (top) iteration context
func (s *IterationStack) Current() *NestedIterationContext {
	if len(s.contexts) == 0 {
		return nil
	}
	return s.contexts[len(s.contexts)-1]
}

// IsActive returns true if there are active iterations
func (s *IterationStack) IsActive() bool {
	return len(s.contexts) > 0
}

// Depth returns the current nesting depth
func (s *IterationStack) Depth() int {
	return len(s.contexts)
}

// GetCurrentIndices returns array of current indices at each depth
func (s *IterationStack) GetCurrentIndices() []int {
	indices := make([]int, len(s.contexts))
	for i, ctx := range s.contexts {
		indices[i] = ctx.CurrentIndex
	}
	return indices
}

// IsIteratingFrom checks if already iterating from this source and path
func (s *IterationStack) IsIteratingFrom(sourceNodeId, arrayPath string) bool {
	for _, ctx := range s.contexts {
		if ctx.SourceNodeId == sourceNodeId && ctx.ArrayPath == arrayPath {
			return true
		}
	}
	return false
}

// GetContextForDepth returns the context at a specific depth
func (s *IterationStack) GetContextForDepth(depth int) *NestedIterationContext {
	if depth < 0 || depth >= len(s.contexts) {
		return nil
	}
	return s.contexts[depth]
}

// CloneWithCurrentIndex returns a new stack sharing parent contexts, with the top
// context copied and CurrentIndex/ItemData set for the given item. Safe for concurrent use.
// Returns nil if stack is empty.
func (s *IterationStack) CloneWithCurrentIndex(itemIndex int, itemData map[string]interface{}) *IterationStack {
	if len(s.contexts) == 0 {
		return nil
	}
	newStack := &IterationStack{
		contexts: make([]*NestedIterationContext, len(s.contexts)),
	}
	copy(newStack.contexts, s.contexts)
	top := s.contexts[len(s.contexts)-1]
	topCopy := *top
	topCopy.CurrentIndex = itemIndex
	topCopy.ItemData = itemData
	newStack.contexts[len(newStack.contexts)-1] = &topCopy
	return newStack
}

// BuildIterationPathPrefix returns a path with indices at each level for flat output keys.
// Example: with stack (data/0, assignments/0, details/0, topics/0) returns "/data[0]/assignments[0]/details[0]/topics[0]".
// Used so flat keys become "nodeId-/data[0]/assignments[0]/.../field[0]" instead of "nodeId-/field[0][0][0][0]".
func (s *IterationStack) BuildIterationPathPrefix() string {
	if len(s.contexts) == 0 {
		return ""
	}
	var b strings.Builder
	for _, ctx := range s.contexts {
		if ctx.ArrayPath == "" {
			continue
		}
		b.WriteString("/")
		b.WriteString(ctx.ArrayPath)
		b.WriteString(fmt.Sprintf("[%d]", ctx.CurrentIndex))
	}
	return b.String()
}

// NestedIterationConfig holds configuration for subflow processing
type NestedIterationConfig struct {
	MaxIterationDepth     int         // Maximum nesting depth (default: 10)
	MaxActiveCombinations int         // Max combinations to prevent explosion (default: 100000)
	StrictArrayNotation   bool        // Strict validation of // paths
	ConcurrencyPerLevel   map[int]int // Workers per depth level
	// IterationConcurrency is max workers for single-level mid-flow iteration. 0 = sequential (default).
	IterationConcurrency int
}

// ProcessInput contains all data needed for an embedded node to process.
type ProcessInput struct {
	// Ctx is the context for cancellation and timeouts
	Ctx context.Context
	// Data is the input data built from field mappings
	Data map[string]interface{}
	// Config is the node-specific configuration (parsed from NodeConfig.Config)
	Config map[string]interface{}
	// RawConfig is the original raw JSON configuration
	RawConfig json.RawMessage
	// NodeId is the ID of the node being processed
	NodeId string
	// PluginType is the type of processor handling this node
	PluginType string
	// Label is the human-readable name of the node
	Label string
	// ItemIndex is the current iteration index (-1 if not iterating)
	ItemIndex int
	// TotalItems is the total number of items in iteration (0 if not iterating)
	TotalItems int
	// IsIteration indicates if this is part of an iteration
	IsIteration bool
	// IterationPath is the array path being iterated (e.g., "data")
	IterationPath string
}

// ProcessOutput contains the result of embedded node processing.
type ProcessOutput struct {
	// Data is the output data from the node
	Data map[string]interface{}
	// Error is set if processing failed
	Error error
	// Skipped indicates if the node was skipped (e.g., due to event trigger)
	Skipped bool
	// SkipReason provides context for why the node was skipped
	SkipReason string
}

// BatchItem represents a single item to be processed in concurrent execution.
type BatchItem struct {
	// Index is the original position in the array (for result ordering)
	Index int
	// Data is the item data to process
	Data map[string]interface{}
}

// BatchResult represents the result of processing a batch item.
type BatchResult struct {
	// Index is the original position (for result ordering)
	Index int
	// Output is the flattened output for shared/pre-iteration data
	Output map[string]interface{}
	// Items contains per-iteration outputs (empty if no mid-flow iteration)
	Items []map[string]interface{}
	// Error is set if processing failed
	Error error
}

// HasItems returns true if there are iteration items
func (r BatchResult) HasItems() bool {
	return len(r.Items) > 0
}

// IterationState holds information about an active iteration within a subflow.
type IterationState struct {
	// IsActive indicates if we're currently in an iteration
	IsActive bool
	// ArrayPath is the path to the array being iterated (e.g., "data")
	ArrayPath string
	// SourceNodeId is the node that produced the array
	SourceNodeId string
	// InitiatedByNodeId is the node that started the iteration (has iterate:true)
	InitiatedByNodeId string
	// TotalItems is the total number of items
	TotalItems int
	// Items are the actual array items
	Items []interface{}
}

// NodeOutputStore stores outputs from executed nodes during subflow processing.
// It tracks single outputs vs iterated outputs separately.
// All map accesses are protected by a mutex for concurrent access safety.
type NodeOutputStore struct {
	mu sync.RWMutex
	// SingleOutputs stores non-iterated node outputs
	// Key: nodeId
	SingleOutputs map[string]map[string]interface{}
	// IteratedOutputs stores outputs from nodes that ran in iteration
	// Key: nodeId, Value: array of outputs (one per iteration)
	IteratedOutputs map[string][]map[string]interface{}
	// IterationInfo tracks which nodes produced arrays and their state
	IterationInfo map[string]IterationState
	// CurrentIterationItems stores the current item being processed for each iteration source
	// Key: sourceNodeId, Value: {item, index}
	CurrentIterationItems map[string]currentIterationItem
	// contextOutputs stores outputs for specific nested iteration contexts
	// Key: nodeId:index0:index1:..., Value: output
	contextOutputs map[string]map[string]interface{}
	// ArrayPathItems stores current iteration items keyed by array path
	// Key: arrayPath (e.g., "$items", "assignments", "chapters"), Value: item data
	// This allows looking up parent iteration context data by array path
	ArrayPathItems map[string]map[string]interface{}
}

// currentIterationItem holds the current item and its index for iteration
type currentIterationItem struct {
	Item  map[string]interface{}
	Index int
}

// NewNodeOutputStore creates a new output store
func NewNodeOutputStore() *NodeOutputStore {
	return &NodeOutputStore{
		SingleOutputs:         make(map[string]map[string]interface{}),
		IteratedOutputs:       make(map[string][]map[string]interface{}),
		IterationInfo:         make(map[string]IterationState),
		CurrentIterationItems: make(map[string]currentIterationItem),
		ArrayPathItems:        make(map[string]map[string]interface{}),
	}
}

// SetSingleOutput stores a single (non-iterated) output
func (s *NodeOutputStore) SetSingleOutput(nodeId string, output map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SingleOutputs[nodeId] = output
}

// SetIteratedOutputs stores all iterated outputs at once
func (s *NodeOutputStore) SetIteratedOutputs(nodeId string, outputs []map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IteratedOutputs[nodeId] = outputs
}

// GetOutput retrieves output for a node, optionally at a specific index
// If index is -1, returns single output; otherwise returns iterated output at index
func (s *NodeOutputStore) GetOutput(nodeId string, index int) (map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Check iterated outputs first if index >= 0
	if index >= 0 {
		if outputs, ok := s.IteratedOutputs[nodeId]; ok {
			if index < len(outputs) {
				return outputs[index], true
			}
			return nil, false
		}
	}
	// Fall back to single output
	if output, ok := s.SingleOutputs[nodeId]; ok {
		return output, true
	}
	return nil, false
}

// GetAllIteratedOutputs returns all outputs for a node that ran in iteration
func (s *NodeOutputStore) GetAllIteratedOutputs(nodeId string) ([]map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	outputs, ok := s.IteratedOutputs[nodeId]
	return outputs, ok
}

// HasIteratedOutput checks if a node has iterated outputs
func (s *NodeOutputStore) HasIteratedOutput(nodeId string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.IteratedOutputs[nodeId]
	return ok
}

// SetIterationInfo stores iteration info for a node
func (s *NodeOutputStore) SetIterationInfo(nodeId string, info IterationState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IterationInfo[nodeId] = info
}

// GetIterationInfo retrieves iteration info for a node
func (s *NodeOutputStore) GetIterationInfo(nodeId string) (IterationState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.IterationInfo[nodeId]
	return info, ok
}

// GetAllSingleOutputs returns all single (non-iterated) outputs
// Returns a copy to prevent external mutation of the internal map
func (s *NodeOutputStore) GetAllSingleOutputs() map[string]map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]map[string]interface{}, len(s.SingleOutputs))
	for k, v := range s.SingleOutputs {
		result[k] = v
	}
	return result
}

// SetCurrentIterationItem stores the current iteration item for a source node
func (s *NodeOutputStore) SetCurrentIterationItem(sourceNodeId string, item map[string]interface{}, index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CurrentIterationItems[sourceNodeId] = currentIterationItem{
		Item:  item,
		Index: index,
	}
}

// GetCurrentIterationItem retrieves the current iteration item for a source node
func (s *NodeOutputStore) GetCurrentIterationItem(sourceNodeId string) (map[string]interface{}, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ci, ok := s.CurrentIterationItems[sourceNodeId]; ok {
		return ci.Item, ci.Index
	}
	return nil, -1
}

// SetArrayPathItem stores an iteration item keyed by its array path
func (s *NodeOutputStore) SetArrayPathItem(arrayPath string, item map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ArrayPathItems[arrayPath] = item
}

// GetArrayPathItem retrieves an iteration item by its array path
func (s *NodeOutputStore) GetArrayPathItem(arrayPath string) (map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.ArrayPathItems[arrayPath]
	return item, ok
}

// GetOutputAtContext retrieves output considering the iteration stack context
func (s *NodeOutputStore) GetOutputAtContext(
	nodeId string,
	iterStack *IterationStack,
) map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// First check if there's a context-specific output
	if iterStack != nil && iterStack.IsActive() {
		key := s.buildContextKey(nodeId, iterStack)
		if output, ok := s.contextOutputs[key]; ok {
			return output
		}
	}

	// Fall back to single output
	return s.SingleOutputs[nodeId]
}

// SetOutputAtContext stores output for a specific nested iteration context
func (s *NodeOutputStore) SetOutputAtContext(
	nodeId string,
	data map[string]interface{},
	iterStack *IterationStack,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if iterStack == nil || !iterStack.IsActive() {
		s.SingleOutputs[nodeId] = data
		return
	}

	key := s.buildContextKey(nodeId, iterStack)
	if s.contextOutputs == nil {
		s.contextOutputs = make(map[string]map[string]interface{})
	}
	s.contextOutputs[key] = data
}

// buildContextKey generates a unique key for this iteration context
func (s *NodeOutputStore) buildContextKey(nodeId string, iterStack *IterationStack) string {
	indices := iterStack.GetCurrentIndices()
	key := nodeId
	for _, idx := range indices {
		key += fmt.Sprintf(":%d", idx)
	}
	return key
}

// IterationContext holds information about array iteration at the parent level.
type IterationContext struct {
	// IsArrayIteration indicates if we're iterating over an array
	IsArrayIteration bool
	// ArrayPath is the path to the array (e.g., "data" from "/data//field")
	ArrayPath string
	// TotalItems is the number of items to iterate over
	TotalItems int
}

// DestinationStructure holds information about the destination format.
type DestinationStructure struct {
	// HasArrayDest indicates if any destination uses array notation (//)
	HasArrayDest bool
	// ArrayPath is the array path from the destination (e.g., "data")
	ArrayPath string
}

// (SuccessOutput, ErrorOutput and SkippedOutput are provided by specific node helpers in base_node.go)
