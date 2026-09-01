// Package argus provides Argus-backed helpers for the Icarus runner, including a
// ProcessFailureObserver that emits node.ended events after ReportError.
package argus

import (
	"context"
	"strings"

	argusemitter "github.com/wehubfusion/Argus/pkg/emitter"
	embeddedrt "github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/runner"
	"go.uber.org/zap"
)

// parentLabelFromMessage returns the parent node's label from message metadata (e.g. set by Zeus from unit.Label).
// Falls back to msg.Node.NodeID when metadata "label" is missing or empty.
func parentLabelFromMessage(msg *message.Message) string {
	if msg == nil || msg.Node == nil {
		return ""
	}
	if msg.Metadata != nil {
		if l := msg.Metadata["label"]; l != "" {
			return l
		}
	}
	return msg.Node.NodeID
}

func embeddedNodeDepth(nodes []message.EmbeddedNode, nodeID string) (int, bool) {
	if len(nodes) == 0 || nodeID == "" {
		return 0, false
	}
	for _, n := range nodes {
		if n.NodeID == nodeID {
			return n.Depth, true
		}
	}
	return 0, false
}

// NewProcessFailureObserver returns a runner.ProcessFailureObserver that emits Argus
// node.ended events for the failed trigger run.
//
// When it fires: the runner calls the returned observer after ReportError has
// successfully published to the RESULTS stream (i.e., Temporal's workflow
// activity has received the error signal). The observer is NOT called on
// successful processing.
//
// Idempotency: JetStream may redeliver a message after an AckWait timeout even
// when the processor already failed and ReportError was called. Each redelivery
// triggers the observer again. The emitter's EventID (derived from
// workflowID + runID + nodeID) must be stable across retries so that Athena
// treats duplicate node.ended events as the same event.
//
// Produced payload (node.ended event):
//   - ClientID, ProjectID, WorkflowID, RunID, NodeID — taken from message
//     metadata and msg.Workflow / msg.Node.
//   - Output — always {"error": true, "description": processErr.Error()}.
//   - HasError = true, ErrorMessage = processErr.Error().
//   - ContainsNodes — for the parent event, the list of EmbeddedNode IDs so
//     Athena can mark them all as completed (failed) in one update.
//
// Structured embedded failure path: when the processor sets embed_failed_node_id
// and embed_root_cause in message metadata (see pkg/embedded/runtime constants),
// the embedded subflow has already emitted node.ended for all embedded nodes it
// processed (success or failure). This observer then skips re-emission for all
// embedded nodes (to avoid overwriting their status), and also skips the parent
// (it already emitted node.ended success). It returns nil immediately.
//
// Fallback path: when embed_failed_node_id is absent, the observer emits
// node.ended (HasError=true) for the parent node first, then for every
// EmbeddedNode in msg.EmbeddedNodes in order. This covers cases where the
// processor panicked or failed before the embedded subflow ran.
//
// Relationship to Argus packages: this observer calls argusemitter.NodeEndEmitter
// (from Argus pkg/emitter), which calls argusemitter.PreparePayload and then
// publishes to the OBSERVATION NATS stream via the Argus pkg/observer. It does
// not use pkg/observer directly — it is wired to the higher-level
// NodeEndEmitter abstraction so the caller controls transport details.
func NewProcessFailureObserver(emitter argusemitter.NodeEndEmitter, logger *zap.Logger) runner.ProcessFailureObserver {
	if logger == nil {
		logger = zap.NewNop()
	}
	return func(ctx context.Context, msg *message.Message, processErr error) error {
		if emitter == nil || msg == nil || processErr == nil {
			return nil
		}
		clientID := ""
		projectID := ""
		if msg.Metadata != nil {
			clientID = msg.Metadata["client_id"]
			projectID = msg.Metadata["project_id"]
		}
		if clientID == "" {
			logger.Warn("runner process failure: node.ended NOT emitted — client_id missing from message metadata (node will stay 'running' in Athena; Hermes trigger-sync may hang waiting for manifest match)",
				zap.String("process_error", processErr.Error()))
			return nil
		}
		workflowID, runID := "", ""
		if msg.Workflow != nil {
			workflowID = msg.Workflow.WorkflowID
			runID = msg.Workflow.RunID
		}
		if workflowID == "" || runID == "" {
			logger.Warn("runner process failure: node.ended NOT emitted — workflow_id or run_id missing from message (node will stay 'running' in Athena; Hermes trigger-sync may hang)",
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.String("process_error", processErr.Error()))
			return nil
		}
		parentID := ""
		if msg.Node != nil {
			parentID = msg.Node.NodeID
		}
		if parentID == "" && msg.Payload != nil {
			parentID = msg.Payload.NodeID
		}
		if parentID == "" {
			logger.Warn("runner process failure: node.ended NOT emitted — parent node_id missing from message (node will stay 'running' in Athena)",
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.String("process_error", processErr.Error()))
			return nil
		}
		parentLabel := parentLabelFromMessage(msg)
		if parentLabel == "" {
			parentLabel = parentID
		}
		errText := processErr.Error()

		var failedEmbeddedID, rootCause string
		if msg.Metadata != nil {
			failedEmbeddedID = strings.TrimSpace(msg.Metadata[runner.MetaEmbedFailedNodeID])
			rootCause = strings.TrimSpace(msg.Metadata[runner.MetaEmbedRootCause])
		}
		if rootCause == "" {
			rootCause = errText
		}

		failedDepth, foundFailedDepth := embeddedNodeDepth(msg.EmbeddedNodes, failedEmbeddedID)
		logger.Debug("runner process failure observer: metadata snapshot",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("parent_id", parentID),
			zap.String("embed_failed_node_id", failedEmbeddedID),
			zap.String("embed_root_cause", rootCause),
			zap.Int("embed_failed_node_depth", failedDepth),
			zap.Bool("embed_failed_node_depth_found", foundFailedDepth),
			zap.Int("embedded_nodes_count", len(msg.EmbeddedNodes)),
		)

		// Structured embedded failure:
		// - Parent already emitted success.
		// - Embedded subflow emits node.ended for all embedded nodes it processes (success or failure).
		// - Subflow processes depths sequentially; when a node fails at depth D, no nodes at depth > D run.
		// Therefore we must NOT re-emit node.ended for any embedded node here, otherwise we risk overwriting
		// valid success statuses in Athena (success/failed have equal rank) and we may incorrectly mark
		// downstream-never-ran nodes as failed.
		if failedEmbeddedID != "" {
			if !foundFailedDepth {
				logger.Warn("runner process failure: structured embedded failure missing failed node depth; skipping embedded node re-emission",
					zap.String("workflow_id", workflowID),
					zap.String("run_id", runID),
					zap.String("embed_failed_node_id", failedEmbeddedID))
			}
			return nil
		}

		// Fallback: no structured embedded metadata — emit parent + every embedded as failed with processErr text.
		errorOut := map[string]interface{}{
			embeddedrt.ErrorOutputKeyError:       true,
			embeddedrt.ErrorOutputKeyDescription: errText,
		}
		embeddedIDs := make([]string, 0, len(msg.EmbeddedNodes))
		for _, en := range msg.EmbeddedNodes {
			if en.NodeID != "" {
				embeddedIDs = append(embeddedIDs, en.NodeID)
			}
		}
		parentParams := argusemitter.NodeEndEmitParams{
			ClientID:      clientID,
			ProjectID:     projectID,
			WorkflowID:    workflowID,
			RunID:         runID,
			NodeID:        parentID,
			Label:         parentLabel,
			Output:        errorOut,
			HasError:      true,
			ErrorMessage:  errText,
			ContainsNodes: embeddedIDs,
		}
		if err := emitter.EmitNodeEnd(ctx, parentParams); err != nil {
			logger.Warn("runner process failure: parent node.ended emit failed",
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.String("node_id", parentID),
				zap.Error(err))
			return err
		}
		for _, en := range msg.EmbeddedNodes {
			if en.NodeID == "" {
				continue
			}
			lbl := en.Label
			if lbl == "" {
				lbl = en.NodeID
			}
			if err := emitter.EmitNodeEnd(ctx, argusemitter.NodeEndEmitParams{
				ClientID:      clientID,
				ProjectID:     projectID,
				WorkflowID:    workflowID,
				RunID:        runID,
				NodeID:       en.NodeID,
				Label:        lbl,
				Output:       errorOut,
				HasError:     true,
				ErrorMessage: errText,
			}); err != nil {
				logger.Warn("runner process failure: embedded node.ended emit failed",
					zap.String("workflow_id", workflowID),
					zap.String("run_id", runID),
					zap.String("node_id", en.NodeID),
					zap.Error(err))
				return err
			}
		}
		return nil
	}
}
