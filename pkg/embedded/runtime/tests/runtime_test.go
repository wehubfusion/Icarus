package tests

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// dummy node for factory tests
type dummyNode struct{ runtime.BaseNode }

func (d *dummyNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	return runtime.ProcessOutput{Data: map[string]interface{}{"ok": true}}
}

func TestDefaultNodeFactory(t *testing.T) {
	factory := runtime.NewDefaultNodeFactory()
	if factory.HasCreator("plugin-x") {
		t.Fatalf("expected no creator registered")
	}

	factory.Register("plugin-x", func(cfg runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
		return &dummyNode{BaseNode: runtime.NewBaseNode(cfg)}, nil
	})

	if !factory.HasCreator("plugin-x") {
		t.Fatalf("expected creator registered")
	}

	node, err := factory.Create(runtime.EmbeddedNodeConfig{NodeId: "n1", PluginType: "plugin-x", Label: "x"})
	if err != nil {
		t.Fatalf("unexpected create error: %v", err)
	}
	if node.NodeId() != "n1" || node.PluginType() != "plugin-x" {
		t.Fatalf("node metadata mismatch")
	}

	if _, err := factory.Create(runtime.EmbeddedNodeConfig{PluginType: "missing"}); err == nil || !errors.Is(err, runtime.ErrNoExecutor) {
		t.Fatalf("expected ErrNoExecutor, got %v", err)
	}
}

func TestBaseNodeConfigParsing(t *testing.T) {
	cfgMap := map[string]interface{}{"key": "value"}
	raw, _ := json.Marshal(cfgMap)
	base := runtime.NewBaseNode(runtime.EmbeddedNodeConfig{NodeId: "n1", PluginType: "p", Label: "label", NodeConfig: runtime.NodeConfig{Config: raw}})

	if base.NodeId() != "n1" || base.PluginType() != "p" || base.Label() != "label" {
		t.Fatalf("unexpected metadata from BaseNode")
	}
	if base.GetConfigString("key") != "value" {
		t.Fatalf("expected config value")
	}
	if string(base.RawConfig()) != string(raw) {
		t.Fatalf("raw config mismatch")
	}
}




