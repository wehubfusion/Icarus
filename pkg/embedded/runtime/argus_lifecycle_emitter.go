package runtime

import (
	"context"
	"time"

	argusevent "github.com/wehubfusion/Argus/pkg/event"
	argusobserver "github.com/wehubfusion/Argus/pkg/observer"
	"go.uber.org/zap"
)

// ArgusEmbeddedNodeLifecycleEmitter implements EmbeddedNodeLifecycleEmitter
// and publishes node lifecycle events for embedded and parent nodes using the Argus observer.
type ArgusEmbeddedNodeLifecycleEmitter struct {
	observer argusobserver.Observer
	logger   *zap.Logger
}

// NewArgusEmbeddedNodeLifecycleEmitter creates a new emitter.
func NewArgusEmbeddedNodeLifecycleEmitter(
	observer argusobserver.Observer,
	logger *zap.Logger,
) *ArgusEmbeddedNodeLifecycleEmitter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ArgusEmbeddedNodeLifecycleEmitter{
		observer: observer,
		logger:   logger,
	}
}

// ParentNodeEnded emits a best-effort node.ended observation event for a parent node
// whose plugin execution has completed and is about to have its embedded nodes processed.
func (e *ArgusEmbeddedNodeLifecycleEmitter) ParentNodeEnded(ctx context.Context, info ParentNodeEndInfo) {
	if e == nil || e.observer == nil {
		return
	}

	// Require full Level 3 context to avoid emitting partial events.
	if info.WorkflowID == "" || info.RunID == "" || info.ClientID == "" || info.ParentNodeID == "" {
		e.logger.Debug("skipping parent node.ended event due to missing context",
			zap.String("workflow_id", info.WorkflowID),
			zap.String("run_id", info.RunID),
			zap.String("client_id", info.ClientID),
			zap.String("parent_node_id", info.ParentNodeID),
		)
		return
	}

	endData := &argusevent.EndNode{
		WorkflowID: info.WorkflowID,
		RunID:      info.RunID,
		ClientID:   info.ClientID,
		ProjectID:  info.ProjectID,
		NodeID:     info.ParentNodeID,
		Label:      info.Label,
		EndedAt:    time.Now().UnixMilli(),
		// At this point we only know that the parent plugin completed successfully.
		// Embedded nodes (if any) will run afterward and may still fail, so we
		// do not attach output or error information here.
		Output:       nil,
		HasError:     false,
		ErrorMessage: "",
	}

	evt := argusevent.New(argusevent.TypeNodeEnded).
		WithClient(info.ClientID).
		WithWorkflow(info.WorkflowID).
		WithRun(info.RunID).
		WithNode(info.ParentNodeID).
		WithData(endData)

	if err := e.observer.Emit(ctx, evt); err != nil {
		e.logger.Warn("failed to emit parent node.ended observation event",
			zap.String("workflow_id", info.WorkflowID),
			zap.String("run_id", info.RunID),
			zap.String("node_id", info.ParentNodeID),
			zap.Error(err),
		)
	}
}

// EmbeddedNodeStarted emits a best-effort node.started observation event for an embedded node.
func (e *ArgusEmbeddedNodeLifecycleEmitter) EmbeddedNodeStarted(ctx context.Context, info EmbeddedNodeStartInfo) {
	if e == nil || e.observer == nil {
		return
	}

	// Require full Level 3 context to avoid emitting partial events.
	if info.WorkflowID == "" || info.RunID == "" || info.ClientID == "" || info.EmbeddedNodeID == "" {
		e.logger.Debug("skipping embedded node.started event due to missing context",
			zap.String("workflow_id", info.WorkflowID),
			zap.String("run_id", info.RunID),
			zap.String("client_id", info.ClientID),
			zap.String("parent_node_id", info.ParentNodeID),
			zap.String("embedded_node_id", info.EmbeddedNodeID),
		)
		return
	}

	startData := &argusevent.StartNode{
		WorkflowID: info.WorkflowID,
		RunID:      info.RunID,
		ClientID:   info.ClientID,
		ProjectID:  info.ProjectID,
		NodeID:     info.EmbeddedNodeID,
		StartedAt:  time.Now().UnixMilli(),
		// For now we do not populate Input for embedded nodes; it can be added later if needed.
		Input: nil,
	}

	evt := argusevent.New(argusevent.TypeNodeStarted).
		WithClient(info.ClientID).
		WithWorkflow(info.WorkflowID).
		WithRun(info.RunID).
		WithNode(info.EmbeddedNodeID).
		WithData(startData)

	if err := e.observer.Emit(ctx, evt); err != nil {
		e.logger.Warn("failed to emit embedded node.started observation event",
			zap.String("workflow_id", info.WorkflowID),
			zap.String("run_id", info.RunID),
			zap.String("node_id", info.EmbeddedNodeID),
			zap.Error(err),
		)
	}
}
