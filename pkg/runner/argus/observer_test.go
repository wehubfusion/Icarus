package argus

import (
	"context"
	"errors"
	"sync"
	"testing"

	argusemitter "github.com/wehubfusion/Argus/pkg/emitter"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type recordingNodeEndEmitter struct {
	mu     sync.Mutex
	params []argusemitter.NodeEndEmitParams
}

func (r *recordingNodeEndEmitter) EmitNodeEnd(_ context.Context, p argusemitter.NodeEndEmitParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.params = append(r.params, p)
	return nil
}

func TestProcessFailureObserver_StructuredEmbedded_NoEmitForDownstreamNeverRan(t *testing.T) {
	rec := &recordingNodeEndEmitter{}
	obs := NewProcessFailureObserver(rec, zap.NewNop())

	msg := message.NewWorkflowMessage("wf-1", "run-1")
	msg.Metadata["client_id"] = "c1"
	msg.Metadata["project_id"] = "p1"
	msg.WithNode("parent-1", nil)
	msg.EmbeddedNodes = []message.EmbeddedNode{
		{NodeID: "fail-1", Label: "F", ExecutionOrder: 0},
		{NodeID: "skip-1", Label: "S", ExecutionOrder: 1},
	}
	msg.Metadata["embed_failed_node_id"] = "fail-1"
	msg.Metadata["embed_root_cause"] = "parse error"

	err := obs(context.Background(), msg, errors.New("wrapped temporal error"))
	require.NoError(t, err)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Empty(t, rec.params, "downstream never-ran nodes must not produce node.ended emits")
}

func TestProcessFailureObserver_NoMetadata_EmitsParentAndEmbeddedFailed(t *testing.T) {
	rec := &recordingNodeEndEmitter{}
	obs := NewProcessFailureObserver(rec, zap.NewNop())

	msg := message.NewWorkflowMessage("wf-1", "run-1")
	msg.Metadata["client_id"] = "c1"
	msg.Metadata["project_id"] = "p1"
	msg.WithNode("parent-1", nil)
	msg.EmbeddedNodes = []message.EmbeddedNode{{NodeID: "e1", Label: "E"}}

	procErr := errors.New("boom")
	err := obs(context.Background(), msg, procErr)
	require.NoError(t, err)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.params, 2)
	require.Equal(t, "parent-1", rec.params[0].NodeID)
	require.True(t, rec.params[0].HasError)
	require.Equal(t, "e1", rec.params[1].NodeID)
	require.True(t, rec.params[1].HasError)
}
