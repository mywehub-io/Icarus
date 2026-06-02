package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	argusemitter "github.com/wehubfusion/Argus/pkg/emitter"
	"go.uber.org/zap"
)

// ArgusEmbeddedNodeLifecycleEmitter implements EmbeddedNodeLifecycleEmitter
// and publishes node lifecycle events for embedded and parent nodes using the Argus observer.
type ArgusEmbeddedNodeLifecycleEmitter struct {
	nodeEndEmitter   argusemitter.NodeEndEmitter
	nodeStartEmitter argusemitter.NodeStartEmitter
	logger           *zap.Logger
}

// NewArgusEmbeddedNodeLifecycleEmitter creates a lifecycle-payload adapter.
func NewArgusEmbeddedNodeLifecycleEmitter(
	nodeEndEmitter argusemitter.NodeEndEmitter,
	nodeStartEmitter argusemitter.NodeStartEmitter,
	logger *zap.Logger,
) *ArgusEmbeddedNodeLifecycleEmitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ArgusEmbeddedNodeLifecycleEmitter{
		nodeEndEmitter:   nodeEndEmitter,
		nodeStartEmitter: nodeStartEmitter,
		logger:           logger,
	}
}

// EmitNodeStartEvent emits a node.started event with input payload for an embedded node.
func (e *ArgusEmbeddedNodeLifecycleEmitter) EmitNodeStartEvent(ctx context.Context, info EmbeddedNodeIOInfo) error {
	if e == nil {
		return fmt.Errorf("lifecycle emitter is nil")
	}
	if e.nodeStartEmitter == nil {
		return fmt.Errorf("nodeStartEmitter is nil")
	}
	if !hasRequiredIDs(info.WorkflowID, info.RunID, info.ClientID, info.EmbeddedNodeID) {
		return fmt.Errorf("missing required IDs for node start event")
	}
	// Emit a start event even when input is empty; this allows timeline visibility.
	// Empty input is normalized to {} so downstream can later backfill with real input.
	input := info.Data
	if input == nil {
		input = map[string]interface{}{}
	}
	jsonBytes, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal embedded node input for %s: %w", info.EmbeddedNodeID, err)
	}
	if err := e.nodeStartEmitter.EmitNodeStart(ctx, argusemitter.NodeStartEmitParams{
		ClientID:      info.ClientID,
		ProjectID:     info.ProjectID,
		EnvironmentID: info.EnvironmentID,
		WorkflowID:    info.WorkflowID,
		RunID:      info.RunID,
		NodeID:     info.EmbeddedNodeID,
		Label:      info.Label,
		Input:      jsonBytes,
	}); err != nil {
		return fmt.Errorf("emit NodeStart for %s: %w", info.EmbeddedNodeID, err)
	}
	return nil
}

// EmitNodeEndEvent emits a terminal node event with output payload for either parent or embedded node.
func (e *ArgusEmbeddedNodeLifecycleEmitter) EmitNodeEndEvent(ctx context.Context, info EmbeddedNodeIOInfo) error {
	if e == nil {
		return fmt.Errorf("lifecycle emitter is nil")
	}
	if e.nodeEndEmitter == nil {
		return fmt.Errorf("nodeEndEmitter is nil")
	}
	nodeID := info.EmbeddedNodeID
	if nodeID == "" {
		nodeID = info.ParentNodeID
	}
	if !hasRequiredIDs(info.WorkflowID, info.RunID, info.ClientID, nodeID) {
		return fmt.Errorf("missing required IDs for node end event")
	}
	if info.Data == nil {
		return fmt.Errorf("node end event has nil data")
	}
	if err := e.nodeEndEmitter.EmitNodeEnd(ctx, argusemitter.NodeEndEmitParams{
		ClientID:      info.ClientID,
		ProjectID:     info.ProjectID,
		EnvironmentID: info.EnvironmentID,
		WorkflowID:    info.WorkflowID,
		RunID:        info.RunID,
		NodeID:       nodeID,
		Label:        info.Label,
		Output:       info.Data,
		HasError:     info.HasError,
		ErrorMessage: info.ErrorMessage,
	}); err != nil {
		return fmt.Errorf("emit NodeEnd for %s: %w", nodeID, err)
	}
	return nil
}

func hasRequiredIDs(workflowID, runID, clientID, nodeID string) bool {
	return workflowID != "" && runID != "" && clientID != "" && nodeID != ""
}
