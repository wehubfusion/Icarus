package strings_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	stringsproc "github.com/wehubfusion/Icarus/pkg/embedded/processors/strings"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func TestConfigValidate(t *testing.T) {
	cfg := stringsproc.Config{Action: "concatenate"}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	cfg = stringsproc.Config{Action: "bad"}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected invalid action error")
	}

	cfg = stringsproc.Config{Action: "concatenate", ManualInputs: []stringsproc.ManualInput{{Name: "", Type: "string"}}}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected manual input validation error")
	}

	cfg = stringsproc.Config{Action: "concatenate", ManualInputs: []stringsproc.ManualInput{{Name: "id", Type: "number"}}}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid manual input config, got %v", err)
	}
}

// createTestNode creates a StringsNode for testing
func createTestNode(t *testing.T, nodeID string) *stringsproc.StringsNode {
	config := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-strings",
		PluginType: "plugin-strings",
		Embeddable: true,
		Depth:      0,
	}
	node, err := stringsproc.NewStringsNode(config)
	if err != nil {
		t.Fatalf("failed to create test node: %v", err)
	}
	return node.(*stringsproc.StringsNode)
}

// createProcessInput creates a ProcessInput for testing
func createProcessInput(data map[string]interface{}, rawConfig json.RawMessage, itemIndex int) runtime.ProcessInput {
	return runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      data,
		RawConfig: rawConfig,
		NodeId:    "test-node-1",
		ItemIndex: itemIndex,
	}
}

// TestProcessInvalidJSONConfig tests Process with invalid JSON configuration
func TestProcessInvalidJSONConfig(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"field1": "value1"},
		json.RawMessage(`{invalid json}`),
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid JSON config")
	}
	if _, ok := output.Error.(*stringsproc.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}
	if output.Data != nil {
		t.Fatalf("expected nil data on error")
	}
}

// TestProcessEmptyAction tests Process with empty action
func TestProcessEmptyAction(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := stringsproc.Config{
		Action: "",
		Params:    map[string]interface{}{},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"field1": "value1"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for empty action")
	}
	if _, ok := output.Error.(*stringsproc.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}
}

// TestProcessUnsupportedAction tests Process with unsupported action
func TestProcessUnsupportedAction(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := stringsproc.Config{
		Action: "unsupported_op",
		Params:    map[string]interface{}{},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"field1": "value1"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for unsupported action")
	}
	if _, ok := output.Error.(*stringsproc.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}
}

// TestProcessInvalidManualInputs tests Process with invalid manual inputs
func TestProcessInvalidManualInputs(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	// Test with empty name
	config := stringsproc.Config{
		Action: "concatenate",
		Params:    map[string]interface{}{},
		ManualInputs: []stringsproc.ManualInput{
			{Name: "", Type: "string"},
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"field1": "value1"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for empty manual input name")
	}
	if _, ok := output.Error.(*stringsproc.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}

	// Empty type is allowed and defaults to "string" (seed-node-schemas.sql concatenate/join only define name)
	config = stringsproc.Config{
		Action: "concatenate",
		Params:    map[string]interface{}{},
		ManualInputs: []stringsproc.ManualInput{
			{Name: "field1", Type: ""},
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{"field1": "value1"},
		rawConfig,
		0,
	)
	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("empty type defaults to string, expected success: %v", output.Error)
	}

	// Test with invalid type
	config = stringsproc.Config{
		Action: "concatenate",
		Params:    map[string]interface{}{},
		ManualInputs: []stringsproc.ManualInput{
			{Name: "field1", Type: "invalid"},
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{"field1": "value1"},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid manual input type")
	}
}

// TestProcessConcatenate tests Process with concatenate action
func TestProcessConcatenate(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	// Test with separator and parts
	config := stringsproc.Config{
		Action: "concatenate",
		Params: map[string]interface{}{
			"separator": "-",
			"parts":     []string{"hello", "world"},
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected non-nil data")
	}
	if output.Data["result"] != "hello-world" {
		t.Fatalf("expected 'hello-world', got %v", output.Data["result"])
	}

	// Test with empty separator
	config = stringsproc.Config{
		Action: "concatenate",
		Params: map[string]interface{}{
			"separator": "",
			"parts":     []string{"a", "b", "c"},
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "abc" {
		t.Fatalf("expected 'abc', got %v", output.Data["result"])
	}

	// Test with parts from input data
	config = stringsproc.Config{
		Action: "concatenate",
		Params: map[string]interface{}{
			"separator": " ",
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{
			"first":  "hello",
			"second": "world",
		},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello world" {
		t.Fatalf("expected 'hello world', got %v", output.Data["result"])
	}
}

// TestProcessSplit tests Process with split action
func TestProcessSplit(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "split",
		Params: map[string]interface{}{
			"string":    "a,b,c",
			"delimiter": ",",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected non-nil data")
	}
	result, ok := output.Data["result"].([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", output.Data["result"])
	}
	if len(result) != 3 || result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Fatalf("expected [a b c], got %v", result)
	}

	// Test with string from input data
	config = stringsproc.Config{
		Action: "split",
		Params: map[string]interface{}{
			"delimiter": " ",
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{
			"string": "hello world test",
		},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	result, ok = output.Data["result"].([]string)
	if !ok || len(result) != 3 {
		t.Fatalf("expected 3 parts, got %v", result)
	}
}

// TestProcessJoin tests Process with join action
func TestProcessJoin(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "join",
		Params: map[string]interface{}{
			"items":     []string{"a", "b", "c"},
			"separator": "-",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "a-b-c" {
		t.Fatalf("expected 'a-b-c', got %v", output.Data["result"])
	}

	// seed-node-schemas.sql String Join: manual_inputs flow in as flat input keys
	config2 := stringsproc.Config{
		Action: "join",
		Params: map[string]interface{}{
			"separator": "-",
		},
	}
	rawConfig2, _ := json.Marshal(config2)
	input2 := createProcessInput(
		map[string]interface{}{
			"a": "hello",
			"b": "world",
		},
		rawConfig2,
		0,
	)

	output2 := node.Process(input2)
	if output2.Error != nil {
		t.Fatalf("unexpected error: %v", output2.Error)
	}
	// extractStringValues order is map-iteration order; just verify both values present
	result := output2.Data["result"].(string)
	parts := strings.Split(result, "-")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), result)
	}
	hasHello, hasWorld := false, false
	for _, p := range parts {
		if p == "hello" {
			hasHello = true
		}
		if p == "world" {
			hasWorld = true
		}
	}
	if !hasHello || !hasWorld {
		t.Fatalf("expected 'hello' and 'world' in result, got %v", result)
	}
}

// TestProcessTrim tests Process with trim action
func TestProcessTrim(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "trim",
		Params: map[string]interface{}{
			"string": "  hello world  ",
			"cutset": "",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello world" {
		t.Fatalf("expected 'hello world', got %v", output.Data["result"])
	}

	// Test with custom cutset
	config = stringsproc.Config{
		Action: "trim",
		Params: map[string]interface{}{
			"string": "!!!hello!!!",
			"cutset": "!",
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello" {
		t.Fatalf("expected 'hello', got %v", output.Data["result"])
	}
}

// TestProcessReplace tests Process with replace action
func TestProcessReplace(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	// Test simple replace
	config := stringsproc.Config{
		Action: "replace",
		Params: map[string]interface{}{
			"string": "hello world",
			"old":    "world",
			"new":    "universe",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello universe" {
		t.Fatalf("expected 'hello universe', got %v", output.Data["result"])
	}

	// Test replace with count
	config = stringsproc.Config{
		Action: "replace",
		Params: map[string]interface{}{
			"string": "foo foo foo",
			"old":    "foo",
			"new":    "bar",
			"count":  2,
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "bar bar foo" {
		t.Fatalf("expected 'bar bar foo', got %v", output.Data["result"])
	}

	// Test replace with regex
	config = stringsproc.Config{
		Action: "replace",
		Params: map[string]interface{}{
			"string":   "hello123world456",
			"old":      "\\d+",
			"new":      "X",
			"use_regex": true,
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "helloXworldX" {
		t.Fatalf("expected 'helloXworldX', got %v", output.Data["result"])
	}
}

// TestProcessReplaceInvalidRegex tests Process with replace action using invalid regex
func TestProcessReplaceInvalidRegex(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "replace",
		Params: map[string]interface{}{
			"string":   "hello world",
			"old":      "[invalid",
			"new":      "X",
			"use_regex": true,
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid regex")
	}
	if _, ok := output.Error.(*stringsproc.ActionError); !ok {
		t.Fatalf("expected ActionError, got %T", output.Error)
	}
}

// TestProcessSubstring tests Process with substring action
func TestProcessSubstring(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "substring",
		Params: map[string]interface{}{
			"string": "hello world",
			"start":  0,
			"end":    5,
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello" {
		t.Fatalf("expected 'hello', got %v", output.Data["result"])
	}

	// Test with negative indices
	config = stringsproc.Config{
		Action: "substring",
		Params: map[string]interface{}{
			"string": "hello world",
			"start":  -5,
			"end":    0,
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "world" {
		t.Fatalf("expected 'world', got %v", output.Data["result"])
	}
}

// TestProcessToUpper tests Process with to_upper action
func TestProcessToUpper(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "to_upper",
		Params: map[string]interface{}{
			"string": "hello world",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "HELLO WORLD" {
		t.Fatalf("expected 'HELLO WORLD', got %v", output.Data["result"])
	}
}

// TestProcessToLower tests Process with to_lower action
func TestProcessToLower(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "to_lower",
		Params: map[string]interface{}{
			"string": "HELLO WORLD",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello world" {
		t.Fatalf("expected 'hello world', got %v", output.Data["result"])
	}
}

// TestProcessTitleCase tests Process with title_case action
func TestProcessTitleCase(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "title_case",
		Params: map[string]interface{}{
			"string": "hello world",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "Hello World" {
		t.Fatalf("expected 'Hello World', got %v", output.Data["result"])
	}
}

// TestProcessCapitalize tests Process with capitalize action
func TestProcessCapitalize(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "capitalize",
		Params: map[string]interface{}{
			"string": "hello world",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "Hello world" {
		t.Fatalf("expected 'Hello world', got %v", output.Data["result"])
	}

	// Test with empty string
	config = stringsproc.Config{
		Action: "capitalize",
		Params: map[string]interface{}{
			"string": "",
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "" {
		t.Fatalf("expected empty string, got %v", output.Data["result"])
	}
}

// TestProcessContains tests Process with contains action
func TestProcessContains(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	// Test simple contains
	config := stringsproc.Config{
		Action: "contains",
		Params: map[string]interface{}{
			"string":    "hello world",
			"substring": "world",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != true {
		t.Fatalf("expected true, got %v", output.Data["result"])
	}

	// Test with regex
	config = stringsproc.Config{
		Action: "contains",
		Params: map[string]interface{}{
			"string":    "hello123world",
			"substring": "\\d+",
			"use_regex": true,
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != true {
		t.Fatalf("expected true, got %v", output.Data["result"])
	}

	// Test not contains
	config = stringsproc.Config{
		Action: "contains",
		Params: map[string]interface{}{
			"string":    "hello world",
			"substring": "xyz",
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != false {
		t.Fatalf("expected false, got %v", output.Data["result"])
	}
}

// TestProcessContainsInvalidRegex tests Process with contains action using invalid regex
func TestProcessContainsInvalidRegex(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "contains",
		Params: map[string]interface{}{
			"string":    "hello world",
			"substring": "[invalid",
			"use_regex": true,
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid regex")
	}
	if _, ok := output.Error.(*stringsproc.ActionError); !ok {
		t.Fatalf("expected ActionError, got %T", output.Error)
	}
}

// TestProcessLength tests Process with length action
func TestProcessLength(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "length",
		Params: map[string]interface{}{
			"string": "hello",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != 5 {
		t.Fatalf("expected 5, got %v", output.Data["result"])
	}

	// Test with unicode
	config = stringsproc.Config{
		Action: "length",
		Params: map[string]interface{}{
			"string": "héllo",
		},
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != 5 {
		t.Fatalf("expected 5 (rune count), got %v", output.Data["result"])
	}
}

// TestProcessRegexExtract tests Process with regex_extract action
func TestProcessRegexExtract(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "regex_extract",
		Params: map[string]interface{}{
			"string":  "hello123world456",
			"pattern": "\\d+",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] == nil {
		t.Fatalf("expected non-nil result")
	}
}

// TestProcessRegexExtractInvalidPattern tests Process with regex_extract action using invalid pattern
func TestProcessRegexExtractInvalidPattern(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "regex_extract",
		Params: map[string]interface{}{
			"string":  "hello world",
			"pattern": "[invalid",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid regex pattern")
	}
	if _, ok := output.Error.(*stringsproc.ActionError); !ok {
		t.Fatalf("expected ActionError, got %T", output.Error)
	}
}

// TestProcessFormat tests Process with format action
func TestProcessFormat(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "format",
		Params: map[string]interface{}{
			"template": "Hello ${name}, you are {age} years old",
			"data": map[string]interface{}{
				"name": "Alice",
				"age":  "30",
			},
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "Hello Alice, you are 30 years old" {
		t.Fatalf("expected formatted string, got %v", output.Data["result"])
	}

	// seed-node-schemas.sql String Format: manual_inputs flow in as flat input keys
	config2 := stringsproc.Config{
		Action: "format",
		Params: map[string]interface{}{
			"template": "Hello {name}, welcome to {city}",
		},
	}
	rawConfig2, _ := json.Marshal(config2)
	input2 := createProcessInput(
		map[string]interface{}{
			"name": "Bob",
			"city": "NYC",
		},
		rawConfig2,
		0,
	)

	output2 := node.Process(input2)
	if output2.Error != nil {
		t.Fatalf("unexpected error: %v", output2.Error)
	}
	if output2.Data["result"] != "Hello Bob, welcome to NYC" {
		t.Fatalf("expected 'Hello Bob, welcome to NYC', got %v", output2.Data["result"])
	}
}

// TestProcessBase64Encode tests Process with base64_encode action
func TestProcessBase64Encode(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "base64_encode",
		Params: map[string]interface{}{
			"string": "hello world",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "aGVsbG8gd29ybGQ=" {
		t.Fatalf("expected base64 encoded string, got %v", output.Data["result"])
	}
}

// TestProcessBase64Decode tests Process with base64_decode action
func TestProcessBase64Decode(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "base64_decode",
		Params: map[string]interface{}{
			"string": "aGVsbG8gd29ybGQ=",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello world" {
		t.Fatalf("expected 'hello world', got %v", output.Data["result"])
	}
}

// TestProcessBase64DecodeInvalid tests Process with base64_decode action using invalid base64
func TestProcessBase64DecodeInvalid(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "base64_decode",
		Params: map[string]interface{}{
			"string": "invalid!!!",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid base64")
	}
	if _, ok := output.Error.(*stringsproc.ActionError); !ok {
		t.Fatalf("expected ActionError, got %T", output.Error)
	}
}

// TestProcessURIEncode tests Process with uri_encode action
func TestProcessURIEncode(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "uri_encode",
		Params: map[string]interface{}{
			"string": "hello world",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello+world" {
		t.Fatalf("expected 'hello+world', got %v", output.Data["result"])
	}
}

// TestProcessURIDecode tests Process with uri_decode action
func TestProcessURIDecode(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "uri_decode",
		Params: map[string]interface{}{
			"string": "hello+world",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "hello world" {
		t.Fatalf("expected 'hello world', got %v", output.Data["result"])
	}
}

// TestProcessURIDecodeInvalid tests Process with uri_decode action using invalid URI encoding
func TestProcessURIDecodeInvalid(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "uri_decode",
		Params: map[string]interface{}{
			"string": "%ZZ",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid URI encoding")
	}
	if _, ok := output.Error.(*stringsproc.ActionError); !ok {
		t.Fatalf("expected ActionError, got %T", output.Error)
	}
}

// TestProcessNormalize tests Process with normalize action
func TestProcessNormalize(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "normalize",
		Params: map[string]interface{}{
			"string": "héllo wörld",
		},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] == nil {
		t.Fatalf("expected non-nil result")
	}
}

// TestProcessWithItemIndex tests Process with different item indices
func TestProcessWithItemIndex(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	config := stringsproc.Config{
		Action: "to_upper",
		Params: map[string]interface{}{
			"string": "hello",
		},
	}
	rawConfig, _ := json.Marshal(config)

	// Test with itemIndex 0
	input := createProcessInput(
		map[string]interface{}{},
		rawConfig,
		0,
	)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}

	// Test with itemIndex 5
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		5,
	)
	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}

	// Test with itemIndex -1
	input = createProcessInput(
		map[string]interface{}{},
		rawConfig,
		-1,
	)
	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
}

// TestProcessWithInputData tests Process with data from input
func TestProcessWithInputData(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	// Test action that uses input data when params don't have the value
	config := stringsproc.Config{
		Action: "to_upper",
		Params:    map[string]interface{}{},
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{
			"string": "hello world",
		},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data["result"] != "HELLO WORLD" {
		t.Fatalf("expected 'HELLO WORLD', got %v", output.Data["result"])
	}
}

// TestProcessAllActions tests all supported actions to ensure they work
func TestProcessAllActions(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	actions := []string{
		"concatenate", "split", "join", "trim", "replace", "substring",
		"to_upper", "to_lower", "title_case", "capitalize", "contains",
		"length", "regex_extract", "format", "base64_encode", "base64_decode",
		"uri_encode", "uri_decode", "normalize",
	}

	for _, op := range actions {
		config := stringsproc.Config{
			Action: op,
			Params:    getDefaultParamsForAction(op),
		}
		rawConfig, err := json.Marshal(config)
		if err != nil {
			t.Fatalf("failed to marshal config for %s: %v", op, err)
		}
		input := createProcessInput(
			getDefaultInputForAction(op),
			rawConfig,
			0,
		)

		output := node.Process(input)
		if output.Error != nil {
			// Some actions may fail with default params, which is expected
			// We just want to ensure they don't crash
			if _, ok := output.Error.(*stringsproc.ActionError); !ok {
				t.Fatalf("action %s returned unexpected error type: %T", op, output.Error)
			}
		}
	}
}

// getDefaultParamsForAction returns default params for testing each action
func getDefaultParamsForAction(op string) map[string]interface{} {
	switch op {
	case "concatenate":
		return map[string]interface{}{
			"separator": "-",
			"parts":     []string{"a", "b"},
		}
	case "split":
		return map[string]interface{}{
			"string":    "a,b",
			"delimiter": ",",
		}
	case "join":
		return map[string]interface{}{
			"items":     []string{"a", "b"},
			"separator": "-",
		}
	case "trim":
		return map[string]interface{}{
			"string": "  hello  ",
			"cutset": "",
		}
	case "replace":
		return map[string]interface{}{
			"string": "hello world",
			"old":    "world",
			"new":    "universe",
		}
	case "substring":
		return map[string]interface{}{
			"string": "hello",
			"start":  0,
			"end":    3,
		}
	case "to_upper", "to_lower", "title_case", "capitalize":
		return map[string]interface{}{
			"string": "hello",
		}
	case "contains":
		return map[string]interface{}{
			"string":    "hello",
			"substring": "he",
		}
	case "length":
		return map[string]interface{}{
			"string": "hello",
		}
	case "regex_extract":
		return map[string]interface{}{
			"string":  "hello123",
			"pattern": "\\d+",
		}
	case "format":
		return map[string]interface{}{
			"template": "Hello ${name}",
			"data": map[string]interface{}{
				"name": "World",
			},
		}
	case "base64_encode":
		return map[string]interface{}{
			"string": "hello",
		}
	case "base64_decode":
		return map[string]interface{}{
			"string": "aGVsbG8=",
		}
	case "uri_encode":
		return map[string]interface{}{
			"string": "hello world",
		}
	case "uri_decode":
		return map[string]interface{}{
			"string": "hello+world",
		}
	case "normalize":
		return map[string]interface{}{
			"string": "hello",
		}
	default:
		return map[string]interface{}{}
	}
}

// getDefaultInputForAction returns default input data for testing each action
func getDefaultInputForAction(op string) map[string]interface{} {
	return map[string]interface{}{
		"string": "hello",
	}
}





