package errornode

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func createErrorNodeForTest(t *testing.T, nodeID string) *ErrorNode {
	t.Helper()
	cfg := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-error-node",
		PluginType: "plugin-error",
		Embeddable: true,
		Depth:      0,
		NodeConfig: runtime.NodeConfig{NodeId: nodeID},
	}

	node, err := NewErrorNode(cfg)
	if err != nil {
		t.Fatalf("failed to create ErrorNode: %v", err)
	}
	return node.(*ErrorNode)
}

func createProcessInputForTest(nodeID string, data map[string]interface{}, rawConfig json.RawMessage) runtime.ProcessInput {
	return runtime.ProcessInput{
		Ctx:        context.Background(),
		Data:       data,
		RawConfig:  rawConfig,
		NodeId:     nodeID,
		PluginType: "plugin-error",
		Label:      "test-error-node",
		ItemIndex:  -1,
	}
}

func TestNewErrorNode_InvalidPluginType(t *testing.T) {
	cfg := runtime.EmbeddedNodeConfig{
		NodeId:     "node-1",
		Label:      "test-error-node",
		PluginType: "plugin-not-error",
	}

	if _, err := NewErrorNode(cfg); err == nil {
		t.Fatalf("expected error for invalid plugin type, got nil")
	}
}

func TestProcess_UsesInputMessageWhenPresent(t *testing.T) {
	node := createErrorNodeForTest(t, "node-input-msg")

	cfg := Config{
		Label:               "test-label",
		DefaultErrorMessage: "default message",
	}
	rawCfg, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	inputData := map[string]interface{}{
		"message": "runtime message wins",
	}
	input := createProcessInputForTest("node-input-msg", inputData, rawCfg)

	out := node.Process(input)
	if out.Error == nil {
		t.Fatalf("expected error output, got nil error")
	}
	if out.Data != nil {
		t.Fatalf("expected nil data for error output, got: %#v", out.Data)
	}
	if out.Skipped {
		t.Fatalf("expected not skipped")
	}

	errMsg := out.Error.Error()
	if !strings.Contains(errMsg, "runtime message wins") {
		t.Errorf("expected error message to contain input message, got: %q", errMsg)
	}
	if strings.Contains(errMsg, "default message") && !strings.Contains(errMsg, "runtime message wins") {
		t.Errorf("expected input message to take precedence over default error message, got: %q", errMsg)
	}
}

func TestProcess_UsesDefaultMessageWhenNoInputMessage(t *testing.T) {
	node := createErrorNodeForTest(t, "node-default-msg")

	cfg := Config{
		Label:               "test-label",
		DefaultErrorMessage: "configured default message",
	}
	rawCfg, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	input := createProcessInputForTest("node-default-msg", map[string]interface{}{}, rawCfg)

	out := node.Process(input)
	if out.Error == nil {
		t.Fatalf("expected error output, got nil error")
	}

	errMsg := out.Error.Error()
	if !strings.Contains(errMsg, "configured default message") {
		t.Errorf("expected error message to contain default error message, got: %q", errMsg)
	}
}

func TestProcess_UsesGenericFallbackWhenNoMessages(t *testing.T) {
	node := createErrorNodeForTest(t, "node-generic-msg")

	cfg := Config{
		Label:               "test-label",
		DefaultErrorMessage: "",
	}
	rawCfg, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	input := createProcessInputForTest("node-generic-msg", map[string]interface{}{}, rawCfg)

	out := node.Process(input)
	if out.Error == nil {
		t.Fatalf("expected error output, got nil error")
	}

	errMsg := out.Error.Error()
	if !strings.Contains(errMsg, "test-label") || !strings.Contains(errMsg, "plugin-error") {
		t.Errorf("expected fallback to include node label and plugin type, got: %q", errMsg)
	}
}

func TestProcess_InvalidConfigReturnsConfigError(t *testing.T) {
	node := createErrorNodeForTest(t, "node-invalid-config")

	// Intentionally invalid JSON
	rawCfg := json.RawMessage(`{invalid json`)
	input := createProcessInputForTest("node-invalid-config", nil, rawCfg)

	out := node.Process(input)
	if out.Error == nil {
		t.Fatalf("expected error for invalid config, got nil")
	}
	if out.Data == nil {
		// ok; for config errors we don't expect data
	} else {
		t.Errorf("expected nil data for config error, got: %#v", out.Data)
	}

	errMsg := out.Error.Error()
	if !strings.Contains(errMsg, "failed to parse configuration") {
		t.Errorf("expected error message to mention configuration parsing, got: %q", errMsg)
	}
}

