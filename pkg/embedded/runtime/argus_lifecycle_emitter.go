package runtime

import (
	"context"
	"encoding/json"

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
func (e *ArgusEmbeddedNodeLifecycleEmitter) EmitNodeStartEvent(ctx context.Context, info EmbeddedNodeIOInfo) {
	if e == nil || e.nodeStartEmitter == nil {
		return
	}
	if !hasRequiredIDs(info.WorkflowID, info.RunID, info.ClientID, info.EmbeddedNodeID) {
		return
	}
	// Emit a start event even when input is empty; this allows timeline visibility.
	// Empty input is normalized to {} so downstream can later backfill with real input.
	input := info.Data
	if input == nil {
		input = map[string]interface{}{}
	}
	jsonBytes, err := json.Marshal(input)
	if err != nil {
		e.logger.Warn("failed to marshal embedded node input", zap.String("node_id", info.EmbeddedNodeID), zap.Error(err))
		return
	}
	_ = e.nodeStartEmitter.EmitNodeStart(ctx, argusemitter.NodeStartEmitParams{
		ClientID:   info.ClientID,
		ProjectID:  info.ProjectID,
		WorkflowID: info.WorkflowID,
		RunID:      info.RunID,
		NodeID:     info.EmbeddedNodeID,
		Label:      info.Label,
		Input:      jsonBytes,
	})
}

// EmitNodeEndEvent emits a terminal node event with output payload for either parent or embedded node.
func (e *ArgusEmbeddedNodeLifecycleEmitter) EmitNodeEndEvent(ctx context.Context, info EmbeddedNodeIOInfo) {
	if e == nil || e.nodeEndEmitter == nil {
		return
	}
	nodeID := info.EmbeddedNodeID
	if nodeID == "" {
		nodeID = info.ParentNodeID
	}
	if !hasRequiredIDs(info.WorkflowID, info.RunID, info.ClientID, nodeID) {
		return
	}
	if info.Data == nil {
		return
	}
	_ = e.nodeEndEmitter.EmitNodeEnd(ctx, argusemitter.NodeEndEmitParams{
		ClientID:     info.ClientID,
		ProjectID:    info.ProjectID,
		WorkflowID:   info.WorkflowID,
		RunID:        info.RunID,
		NodeID:       nodeID,
		Label:        info.Label,
		Output:       info.Data,
		HasError:     info.HasError,
		ErrorMessage: info.ErrorMessage,
	})
}

func hasRequiredIDs(workflowID, runID, clientID, nodeID string) bool {
	return workflowID != "" && runID != "" && clientID != "" && nodeID != ""
}
