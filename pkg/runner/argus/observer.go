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

const (
	metaEmbedFailedNodeID = "embed_failed_node_id"
	metaEmbedRootCause    = "embed_root_cause"
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
// node.ended for failed trigger runs. When the triggers processor sets embed_failed_node_id /
// embed_root_cause (structured embedded failure), the observer does not re-emit the parent
// (already success) or the failing embedded node (subflow already emitted node.ended), and does
// not emit anything for downstream embedded nodes that never ran. Otherwise it falls back to
// emitting parent + all embedded as failed.
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
			failedEmbeddedID = strings.TrimSpace(msg.Metadata[metaEmbedFailedNodeID])
			rootCause = strings.TrimSpace(msg.Metadata[metaEmbedRootCause])
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
				ClientID:     clientID,
				ProjectID:    projectID,
				WorkflowID:   workflowID,
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
