package tests

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func createConditionNode(t *testing.T, nodeID string) *simplecondition.SimpleConditionNode {
	t.Helper()
	cfg := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-condition",
		PluginType: "plugin-simple-condition",
		Embeddable: true,
		Depth:      0,
		NodeConfig: runtime.NodeConfig{NodeId: nodeID},
	}
	node, err := simplecondition.NewSimpleConditionNode(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	return node.(*simplecondition.SimpleConditionNode)
}

func createProcessInput(data map[string]interface{}, rawConfig json.RawMessage) runtime.ProcessInput {
	return runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      data,
		RawConfig: rawConfig,
		NodeId:    "test-condition-node",
		ItemIndex: -1,
	}
}

// TestProcessMissingFieldSoftFail asserts that when a condition references a missing field
// (and operator is not Is Empty / Is NOT Empty / Equals ""), the node returns success with
// output["warning"] set and both output["true"] and output["false"] nil.
func TestProcessMissingFieldSoftFail(t *testing.T) {
	node := createConditionNode(t, "test-condition-node")
	// Input has only HL7_version and sending_facility; condition references message_type
	inputData := map[string]interface{}{
		"HL7_version":     "2.4",
		"sending_facility": "MFT",
	}
	cfg := simplecondition.Config{
		LogicOperator: simplecondition.LogicAnd,
		ManualInputs: []simplecondition.ManualInput{
			{
				Name:     "message_type",
				Type:     simplecondition.DataTypeString,
				Value:    "OML_O21",
				Operator: simplecondition.OpEquals,
			},
		},
	}
	rawConfig, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	input := createProcessInput(inputData, rawConfig)

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected no error (soft-fail), got: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected non-nil output data")
	}
	warning, ok := output.Data["warning"].(string)
	if !ok || warning == "" {
		t.Fatalf("expected non-empty output[\"warning\"] string, got %v", output.Data["warning"])
	}
	if !strings.Contains(warning, "message_type") {
		t.Errorf("expected warning to mention missing field 'message_type', got: %s", warning)
	}
	if output.Data["true"] != nil {
		t.Errorf("expected output[\"true\"] to be nil when field missing, got %v", output.Data["true"])
	}
	if output.Data["false"] != nil {
		t.Errorf("expected output[\"false\"] to be nil when field missing, got %v", output.Data["false"])
	}
}

// TestProcessFieldPresentNoWarning asserts that when the referenced field is present,
// behavior is unchanged: no warning, normal true/false routing.
func TestProcessFieldPresentNoWarning(t *testing.T) {
	node := createConditionNode(t, "test-condition-node")
	inputData := map[string]interface{}{
		"message_type": "OML_O21",
	}
	cfg := simplecondition.Config{
		LogicOperator: simplecondition.LogicAnd,
		ManualInputs: []simplecondition.ManualInput{
			{
				Name:     "message_type",
				Type:     simplecondition.DataTypeString,
				Value:    "OML_O21",
				Operator: simplecondition.OpEquals,
			},
		},
	}
	rawConfig, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	input := createProcessInput(inputData, rawConfig)

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected no error, got: %v", output.Error)
	}
	if _, hasWarning := output.Data["warning"]; hasWarning {
		t.Errorf("expected no warning when field is present, got warning: %v", output.Data["warning"])
	}
	// Condition message_type == "OML_O21" is true, so data should be on "true" branch
	if output.Data["true"] == nil {
		t.Errorf("expected output[\"true\"] to be input data when condition matches")
	}
	if output.Data["false"] != nil {
		t.Errorf("expected output[\"false\"] to be nil when condition matches")
	}
}

// TestProcessMissingFieldWithIsEmptyNoWarning asserts that when field is missing but
// operator is Is Empty, we do not emit a warning and evaluate as before (missing = empty).
func TestProcessMissingFieldWithIsEmptyNoWarning(t *testing.T) {
	node := createConditionNode(t, "test-condition-node")
	inputData := map[string]interface{}{"other": "x"}
	cfg := simplecondition.Config{
		LogicOperator: simplecondition.LogicAnd,
		ManualInputs: []simplecondition.ManualInput{
			{
				Name:     "missing_field",
				Type:     simplecondition.DataTypeString,
				Value:    "",
				Operator: simplecondition.OpIsEmpty,
			},
		},
	}
	rawConfig, _ := json.Marshal(cfg)
	input := createProcessInput(inputData, rawConfig)

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected no error: %v", output.Error)
	}
	if _, hasWarning := output.Data["warning"]; hasWarning {
		t.Errorf("expected no warning for Is Empty when field missing (treated as empty)")
	}
	// Is Empty on missing field returns true, so data goes to "true"
	if output.Data["true"] == nil {
		t.Errorf("expected output[\"true\"] when Is Empty and field missing")
	}
}
