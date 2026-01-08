package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/dateformatter"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func TestConfigValidate(t *testing.T) {
	cfg := dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateOnly"}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	// invalid format
	bad := dateformatter.Config{InFormat: "bad", OutFormat: "DateOnly"}
	if err := bad.Validate("node1"); err == nil {
		t.Fatalf("expected invalid input format error")
	}

	// invalid date style on non-date output
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "TimeOnly", DateStyle: string(dateformatter.DateStyleMM_DD_YYYY)}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected date_style validation error")
	}

	// invalid time style on non-time output
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateOnly", TimeStyle: string(dateformatter.TimeStyle12Hour)}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected time_style validation error")
	}
}

func TestConfigGetLayouts(t *testing.T) {
	cfg := dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateTime"}
	if got := cfg.GetInputLayout(); got == "" {
		t.Fatalf("expected input layout for RFC3339")
	}

	// DateTime with style overrides
	cfg.DateStyle = string(dateformatter.DateStyleMM_DD_YYYY_Slash)
	cfg.TimeStyle = string(dateformatter.TimeStyle12HourHM)
	outLayout := cfg.GetOutputLayout()
	if outLayout != "01/02/2006 03:04 PM" {
		t.Fatalf("unexpected output layout: %s", outLayout)
	}

	// DateOnly with date style
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateOnly", DateStyle: string(dateformatter.DateStyleYYYY_MM_DD_Slash)}
	if out := cfg.GetOutputLayout(); out != "2006/01/02" {
		t.Fatalf("expected date style layout, got %s", out)
	}

	// TimeOnly with time style
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "TimeOnly", TimeStyle: string(dateformatter.TimeStyle24HourHM)}
	if out := cfg.GetOutputLayout(); out != "15:04" {
		t.Fatalf("expected time style layout, got %s", out)
	}
}

// createTestNode creates a DateFormatterNode for testing
func createTestNode(t *testing.T, nodeID string) *dateformatter.DateFormatterNode {
	config := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-date-formatter",
		PluginType: "plugin-date-formatter",
		Embeddable: true,
		Depth:      0,
	}
	node, err := dateformatter.NewDateFormatterNode(config)
	if err != nil {
		t.Fatalf("failed to create test node: %v", err)
	}
	return node.(*dateformatter.DateFormatterNode)
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
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		json.RawMessage(`{invalid json}`),
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid JSON config")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
	if output.Data != nil {
		t.Fatalf("expected nil data on error")
	}
}

// TestProcessEmptyConfig tests Process with empty JSON config
func TestProcessEmptyConfig(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		json.RawMessage(`{}`),
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for empty config")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessMissingInFormat tests Process with missing in_format
func TestProcessMissingInFormat(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{OutFormat: "DateOnly"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for missing in_format")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessMissingOutFormat tests Process with missing out_format
func TestProcessMissingOutFormat(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{InFormat: "RFC3339"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for missing out_format")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessInvalidInFormat tests Process with invalid in_format
func TestProcessInvalidInFormat(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{InFormat: "InvalidFormat", OutFormat: "DateOnly"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid in_format")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessInvalidOutFormat tests Process with invalid out_format
func TestProcessInvalidOutFormat(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{InFormat: "RFC3339", OutFormat: "InvalidFormat"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid out_format")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessInvalidDateStyle tests Process with invalid date_style
func TestProcessInvalidDateStyle(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
		DateStyle: "InvalidDateStyle",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid date_style")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessInvalidTimeStyle tests Process with invalid time_style
func TestProcessInvalidTimeStyle(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "TimeOnly",
		TimeStyle: "InvalidTimeStyle",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid time_style")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessDateStyleOnTimeOnly tests Process with date_style on TimeOnly output
func TestProcessDateStyleOnTimeOnly(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "TimeOnly",
		DateStyle: string(dateformatter.DateStyleMM_DD_YYYY),
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for date_style on TimeOnly output")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessTimeStyleOnDateOnly tests Process with time_style on DateOnly output
func TestProcessTimeStyleOnDateOnly(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
		TimeStyle: string(dateformatter.TimeStyle12Hour),
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-01T00:00:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for time_style on DateOnly output")
	}
	if _, ok := output.Error.(*dateformatter.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessSuccessBasic tests successful Process with basic config
func TestProcessSuccessBasic(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T10:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "2023-01-15" {
		t.Fatalf("expected result '2023-01-15', got '%s'", result)
	}
}

// TestProcessSuccessWithDateStyle tests successful Process with date_style
func TestProcessSuccessWithDateStyle(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
		DateStyle: string(dateformatter.DateStyleMM_DD_YYYY_Slash),
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T10:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "01/15/2023" {
		t.Fatalf("expected result '01/15/2023', got '%s'", result)
	}
}

// TestProcessSuccessWithTimeStyle tests successful Process with time_style
func TestProcessSuccessWithTimeStyle(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "TimeOnly",
		TimeStyle: string(dateformatter.TimeStyle12HourHM),
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T14:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "02:30 PM" {
		t.Fatalf("expected result '02:30 PM', got '%s'", result)
	}
}

// TestProcessSuccessWithDateTimeAndStyles tests successful Process with DateTime and both styles
func TestProcessSuccessWithDateTimeAndStyles(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateTime",
		DateStyle: string(dateformatter.DateStyleMM_DD_YYYY_Slash),
		TimeStyle: string(dateformatter.TimeStyle12HourHM),
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T14:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "01/15/2023 02:30 PM" {
		t.Fatalf("expected result '01/15/2023 02:30 PM', got '%s'", result)
	}
}

// TestProcessSuccessWithTimezone tests successful Process with timezone conversion
func TestProcessSuccessWithTimezone(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:    "RFC3339",
		OutFormat:   "DateTime",
		InTimezone:  "UTC",
		OutTimezone: "America/New_York",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T14:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	// Should be converted to EST/EDT (UTC-5 or UTC-4 depending on DST)
	// In January, it's EST (UTC-5), so 14:30 UTC = 09:30 EST
	if result != "2023-01-15 09:30:00" {
		t.Fatalf("expected timezone conversion, got '%s'", result)
	}
}

// TestProcessInvalidTimezone tests Process with invalid timezone
func TestProcessInvalidTimezone(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:   "RFC3339",
		OutFormat:  "DateOnly",
		InTimezone: "Invalid/Timezone",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T14:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid timezone")
	}
	if _, ok := output.Error.(*dateformatter.TimezoneError); !ok {
		t.Fatalf("expected TimezoneError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessInvalidOutputTimezone tests Process with invalid output timezone
func TestProcessInvalidOutputTimezone(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:    "RFC3339",
		OutFormat:   "DateOnly",
		OutTimezone: "Invalid/Timezone",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T14:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid output timezone")
	}
	if _, ok := output.Error.(*dateformatter.TimezoneError); !ok {
		t.Fatalf("expected TimezoneError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessParseError tests Process with unparseable date string
func TestProcessParseError(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "not-a-date"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for unparseable date")
	}
	if _, ok := output.Error.(*dateformatter.ParseError); !ok {
		t.Fatalf("expected ParseError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessMissingDateField tests Process with missing date field
func TestProcessMissingDateField(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"other_field": "value"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for missing date field: %v", output.Error)
	}
	// Missing date field should return nil result, not an error
	if output.Data == nil {
		t.Fatalf("expected output data even with missing date field")
	}
	result := output.Data["result"]
	if result != nil {
		t.Fatalf("expected nil result for missing date field, got %v", result)
	}
}

// TestProcessNilDateField tests Process with nil date field
func TestProcessNilDateField(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": nil},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for nil date field: %v", output.Error)
	}
	// Nil date field should return nil result, not an error
	if output.Data == nil {
		t.Fatalf("expected output data even with nil date field")
	}
	result := output.Data["result"]
	if result != nil {
		t.Fatalf("expected nil result for nil date field, got %v", result)
	}
}

// TestProcessEmptyDateString tests Process with empty date string
func TestProcessEmptyDateString(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": ""},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for empty date string: %v", output.Error)
	}
	// Empty date string should return nil result, not an error
	if output.Data == nil {
		t.Fatalf("expected output data even with empty date string")
	}
	result := output.Data["result"]
	if result != nil {
		t.Fatalf("expected nil result for empty date string, got %v", result)
	}
}

// TestProcessWhitespaceDateString tests Process with whitespace-only date string
func TestProcessWhitespaceDateString(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "   "},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for whitespace date string: %v", output.Error)
	}
	// Whitespace-only date string should return nil result, not an error
	if output.Data == nil {
		t.Fatalf("expected output data even with whitespace date string")
	}
	result := output.Data["result"]
	if result != nil {
		t.Fatalf("expected nil result for whitespace date string, got %v", result)
	}
}

// TestProcessNonStringDate tests Process with non-string date value
func TestProcessNonStringDate(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": 12345},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for non-string date value")
	}
	if _, ok := output.Error.(*dateformatter.InputError); !ok {
		t.Fatalf("expected InputError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessNilInputData tests Process with nil input data
func TestProcessNilInputData(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		nil,
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for nil input data: %v", output.Error)
	}
	// Nil input data should return nil result, not an error
	if output.Data == nil {
		t.Fatalf("expected output data even with nil input data")
	}
	result := output.Data["result"]
	if result != nil {
		t.Fatalf("expected nil result for nil input data, got %v", result)
	}
}

// TestProcessWithDifferentItemIndex tests Process with different item indices
func TestProcessWithDifferentItemIndex(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	// Test with ItemIndex = 0
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T10:30:00Z"},
		rawConfig,
		0,
	)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for ItemIndex 0: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data for ItemIndex 0")
	}

	// Test with ItemIndex = 5
	input = createProcessInput(
		map[string]interface{}{"date": "2023-01-15T10:30:00Z"},
		rawConfig,
		5,
	)
	output = node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for ItemIndex 5: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data for ItemIndex 5")
	}

	// Test with ItemIndex = 10 and parse error
	input = createProcessInput(
		map[string]interface{}{"date": "invalid-date"},
		rawConfig,
		10,
	)
	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid date with ItemIndex 10")
	}
	if parseErr, ok := output.Error.(*dateformatter.ParseError); ok {
		if parseErr.ItemIndex != 10 {
			t.Fatalf("expected ItemIndex 10 in error, got %d", parseErr.ItemIndex)
		}
	} else {
		t.Fatalf("expected ParseError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessDifferentInputFormats tests Process with various input formats
func TestProcessDifferentInputFormats(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	testCases := []struct {
		name      string
		inFormat  string
		dateStr   string
		outFormat string
		expected  string
	}{
		{
			name:      "ANSIC format",
			inFormat:  "ANSIC",
			dateStr:   "Mon Jan 15 10:30:00 2023",
			outFormat: "DateOnly",
			expected:  "2023-01-15",
		},
		{
			name:      "RFC822 format",
			inFormat:  "RFC822",
			dateStr:   "15 Jan 23 10:30 MST",
			outFormat: "DateOnly",
			expected:  "2023-01-15",
		},
		{
			name:      "UnixDate format",
			inFormat:  "UnixDate",
			dateStr:   "Mon Jan 15 10:30:00 MST 2023",
			outFormat: "DateOnly",
			expected:  "2023-01-15",
		},
		{
			name:      "Kitchen format",
			inFormat:  "Kitchen",
			dateStr:   "10:30AM",
			outFormat: "TimeOnly",
			expected:  "10:30:00",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := dateformatter.Config{
				InFormat:  tc.inFormat,
				OutFormat: tc.outFormat,
			}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(
				map[string]interface{}{"date": tc.dateStr},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for %s: %v", tc.name, output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data for %s", tc.name)
			}
			result, ok := output.Data["result"].(string)
			if !ok {
				t.Fatalf("expected result to be string for %s, got %T", tc.name, output.Data["result"])
			}
			if result != tc.expected {
				t.Fatalf("expected result '%s' for %s, got '%s'", tc.expected, tc.name, result)
			}
		})
	}
}

// TestProcessDifferentOutputFormats tests Process with various output formats
func TestProcessDifferentOutputFormats(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateTime",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T14:30:45Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "2023-01-15 14:30:45" {
		t.Fatalf("expected result '2023-01-15 14:30:45', got '%s'", result)
	}
}

// TestProcessAllDateStyles tests Process with all date style options
func TestProcessAllDateStyles(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	dateStyles := []dateformatter.DateStyle{
		dateformatter.DateStyleYYYY_MM_DD,
		dateformatter.DateStyleDD_MM_YYYY,
		dateformatter.DateStyleMM_DD_YYYY,
		dateformatter.DateStyleYYYY_MM_DD_Slash,
		dateformatter.DateStyleDD_MM_YYYY_Slash,
		dateformatter.DateStyleMM_DD_YYYY_Slash,
	}

	for _, style := range dateStyles {
		t.Run(string(style), func(t *testing.T) {
			config := dateformatter.Config{
				InFormat:  "RFC3339",
				OutFormat: "DateOnly",
				DateStyle: string(style),
			}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(
				map[string]interface{}{"date": "2023-01-15T10:30:00Z"},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for date style %s: %v", style, output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data for date style %s", style)
			}
			result, ok := output.Data["result"].(string)
			if !ok {
				t.Fatalf("expected result to be string for date style %s, got %T", style, output.Data["result"])
			}
			if result == "" {
				t.Fatalf("expected non-empty result for date style %s", style)
			}
		})
	}
}

// TestProcessAllTimeStyles tests Process with all time style options
func TestProcessAllTimeStyles(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	timeStyles := []dateformatter.TimeStyle{
		dateformatter.TimeStyle24Hour,
		dateformatter.TimeStyle12Hour,
		dateformatter.TimeStyle24HourHM,
		dateformatter.TimeStyle12HourHM,
	}

	for _, style := range timeStyles {
		t.Run(string(style), func(t *testing.T) {
			config := dateformatter.Config{
				InFormat:  "RFC3339",
				OutFormat: "TimeOnly",
				TimeStyle: string(style),
			}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(
				map[string]interface{}{"date": "2023-01-15T14:30:45Z"},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for time style %s: %v", style, output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data for time style %s", style)
			}
			result, ok := output.Data["result"].(string)
			if !ok {
				t.Fatalf("expected result to be string for time style %s, got %T", style, output.Data["result"])
			}
			if result == "" {
				t.Fatalf("expected non-empty result for time style %s", style)
			}
		})
	}
}

// TestProcessAllInputFormats tests Process with all supported input formats
func TestProcessAllInputFormats(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	testCases := []struct {
		name      string
		inFormat  string
		dateStr   string
		outFormat string
	}{
		{"RubyDate", "RubyDate", "Mon Jan 15 10:30:00 -0500 2023", "DateOnly"},
		{"RFC822Z", "RFC822Z", "15 Jan 23 10:30 -0500", "DateOnly"},
		{"RFC850", "RFC850", "Monday, 15-Jan-23 10:30:00 MST", "DateOnly"},
		{"RFC1123", "RFC1123", "Mon, 15 Jan 2023 10:30:00 MST", "DateOnly"},
		{"RFC1123Z", "RFC1123Z", "Mon, 15 Jan 2023 10:30:00 -0500", "DateOnly"},
		{"RFC3339Nano", "RFC3339Nano", "2023-01-15T10:30:00.123456789Z", "DateOnly"},
		{"Stamp", "Stamp", "Jan 15 10:30:00", "DateOnly"},
		{"StampMilli", "StampMilli", "Jan 15 10:30:00.123", "DateOnly"},
		{"StampMicro", "StampMicro", "Jan 15 10:30:00.123456", "DateOnly"},
		{"StampNano", "StampNano", "Jan 15 10:30:00.123456789", "DateOnly"},
		{"DateTime", "DateTime", "2023-01-15 10:30:00", "DateOnly"},
		{"DateOnly", "DateOnly", "2023-01-15", "DateTime"},
		{"TimeOnly", "TimeOnly", "10:30:00", "TimeOnly"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := dateformatter.Config{
				InFormat:  tc.inFormat,
				OutFormat: tc.outFormat,
			}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(
				map[string]interface{}{"date": tc.dateStr},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for %s: %v", tc.name, output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data for %s", tc.name)
			}
			result := output.Data["result"]
			if result == nil {
				t.Fatalf("expected non-nil result for %s", tc.name)
			}
		})
	}
}

// TestProcessDateTimeNormalizationPartialDate tests DateTime format normalization for partial dates
func TestProcessDateTimeNormalizationPartialDate(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "DateTime",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	// Test partial date (YYYY-MM-DD) - should normalize to "YYYY-MM-DD 00:00:00"
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for partial date: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data for partial date")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "2023-01-15" {
		t.Fatalf("expected result '2023-01-15', got '%s'", result)
	}
}

// TestProcessDateTimeNormalizationPartialTime tests DateTime format normalization for partial time
func TestProcessDateTimeNormalizationPartialTime(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "DateTime",
		OutFormat: "TimeOnly",
	}
	rawConfig, _ := json.Marshal(config)

	// Test partial time (YYYY-MM-DD HH:MM) - should normalize to "YYYY-MM-DD HH:MM:00"
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15 14:30"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for partial time: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data for partial time")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "14:30:00" {
		t.Fatalf("expected result '14:30:00', got '%s'", result)
	}
}

// TestProcessDateOnlyCompactFormat tests DateOnly compact format (YYYYMMDD)
func TestProcessDateOnlyCompactFormat(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "DateOnly",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	// Test compact format (YYYYMMDD) - should normalize to "YYYY-MM-DD"
	input := createProcessInput(
		map[string]interface{}{"date": "20230115"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error for compact date: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data for compact date")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result != "2023-01-15" {
		t.Fatalf("expected result '2023-01-15', got '%s'", result)
	}
}

// TestProcessDateOnlyCompactFormatWithSlash tests DateOnly compact format doesn't normalize if slashes exist
func TestProcessDateOnlyCompactFormatWithSlash(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "DateOnly",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	// Test that dates with slashes are not normalized (already in correct format)
	input := createProcessInput(
		map[string]interface{}{"date": "2023/01/15"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	// This should fail because DateOnly expects "YYYY-MM-DD" format, not "YYYY/MM/DD"
	if output.Error == nil {
		t.Fatalf("expected error for date with slashes in DateOnly format")
	}
}

// TestProcessEdgeDates tests Process with edge case dates
func TestProcessEdgeDates(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	testCases := []struct {
		name     string
		dateStr  string
		expected string
	}{
		{"Leap year", "2024-02-29T00:00:00Z", "2024-02-29"},
		{"Year boundary start", "2023-01-01T00:00:00Z", "2023-01-01"},
		{"Year boundary end", "2023-12-31T23:59:59Z", "2023-12-31"},
		{"Month boundary start", "2023-01-01T00:00:00Z", "2023-01-01"},
		{"Month boundary end", "2023-01-31T23:59:59Z", "2023-01-31"},
		{"Day boundary start", "2023-01-15T00:00:00Z", "2023-01-15"},
		{"Day boundary end", "2023-01-15T23:59:59Z", "2023-01-15"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := createProcessInput(
				map[string]interface{}{"date": tc.dateStr},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for %s: %v", tc.name, output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data for %s", tc.name)
			}
			result, ok := output.Data["result"].(string)
			if !ok {
				t.Fatalf("expected result to be string for %s, got %T", tc.name, output.Data["result"])
			}
			if result != tc.expected {
				t.Fatalf("expected result '%s' for %s, got '%s'", tc.expected, tc.name, result)
			}
		})
	}
}

// TestProcessMultipleTimezones tests Process with various timezone conversions
func TestProcessMultipleTimezones(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	testCases := []struct {
		name        string
		inTimezone  string
		outTimezone string
		dateStr     string
	}{
		{"UTC to EST", "UTC", "America/New_York", "2023-01-15T14:30:00Z"},
		{"UTC to PST", "UTC", "America/Los_Angeles", "2023-01-15T14:30:00Z"},
		{"UTC to UTC", "UTC", "UTC", "2023-01-15T14:30:00Z"},
		{"EST to PST", "America/New_York", "America/Los_Angeles", "2023-01-15T14:30:00-05:00"},
		{"Europe/London", "UTC", "Europe/London", "2023-07-15T14:30:00Z"},
		{"Asia/Tokyo", "UTC", "Asia/Tokyo", "2023-01-15T14:30:00Z"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := dateformatter.Config{
				InFormat:    "RFC3339",
				OutFormat:   "DateTime",
				InTimezone:  tc.inTimezone,
				OutTimezone: tc.outTimezone,
			}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(
				map[string]interface{}{"date": tc.dateStr},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for %s: %v", tc.name, output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data for %s", tc.name)
			}
			result, ok := output.Data["result"].(string)
			if !ok {
				t.Fatalf("expected result to be string for %s, got %T", tc.name, output.Data["result"])
			}
			if result == "" {
				t.Fatalf("expected non-empty result for %s", tc.name)
			}
		})
	}
}

// TestProcessWithInputTimezoneOnly tests Process with only input timezone specified
func TestProcessWithInputTimezoneOnly(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:   "DateTime",
		OutFormat:  "DateTime",
		InTimezone: "America/New_York",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15 14:30:00"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	if result == "" {
		t.Fatalf("expected non-empty result")
	}
}

// TestProcessWithOutputTimezoneOnly tests Process with only output timezone specified
func TestProcessWithOutputTimezoneOnly(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:    "RFC3339",
		OutFormat:   "DateTime",
		OutTimezone: "America/New_York",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T14:30:00Z"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("unexpected error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected output data")
	}
	result, ok := output.Data["result"].(string)
	if !ok {
		t.Fatalf("expected result to be string, got %T", output.Data["result"])
	}
	// Should be converted to EST (UTC-5)
	if result != "2023-01-15 09:30:00" {
		t.Fatalf("expected timezone conversion, got '%s'", result)
	}
}

// TestProcessErrorMessages tests that error messages contain expected information
func TestProcessErrorMessages(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	// Test ConfigError message
	config := dateformatter.Config{InFormat: "InvalidFormat", OutFormat: "DateOnly"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01-15T00:00:00Z"},
		rawConfig,
		0,
	)
	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}
	configErr, ok := output.Error.(*dateformatter.ConfigError)
	if !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}
	if configErr.NodeID != "test-node-1" {
		t.Fatalf("expected node ID in error, got '%s'", configErr.NodeID)
	}
	if configErr.Field != "in_format" {
		t.Fatalf("expected field 'in_format' in error, got '%s'", configErr.Field)
	}

	// Test ParseError message
	config = dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateOnly"}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{"date": "invalid-date"},
		rawConfig,
		5,
	)
	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}
	parseErr, ok := output.Error.(*dateformatter.ParseError)
	if !ok {
		t.Fatalf("expected ParseError, got %T", output.Error)
	}
	if parseErr.NodeID != "test-node-1" {
		t.Fatalf("expected node ID in error, got '%s'", parseErr.NodeID)
	}
	if parseErr.ItemIndex != 5 {
		t.Fatalf("expected ItemIndex 5 in error, got %d", parseErr.ItemIndex)
	}

	// Test InputError message
	input = createProcessInput(
		map[string]interface{}{"date": 12345},
		rawConfig,
		10,
	)
	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}
	inputErr, ok := output.Error.(*dateformatter.InputError)
	if !ok {
		t.Fatalf("expected InputError, got %T", output.Error)
	}
	if inputErr.NodeID != "test-node-1" {
		t.Fatalf("expected node ID in error, got '%s'", inputErr.NodeID)
	}
	if inputErr.ItemIndex != 10 {
		t.Fatalf("expected ItemIndex 10 in error, got %d", inputErr.ItemIndex)
	}
	if inputErr.Field != "date" {
		t.Fatalf("expected field 'date' in error, got '%s'", inputErr.Field)
	}

	// Test TimezoneError message
	config = dateformatter.Config{
		InFormat:   "RFC3339",
		OutFormat:  "DateOnly",
		InTimezone: "Invalid/Timezone",
	}
	rawConfig, _ = json.Marshal(config)
	input = createProcessInput(
		map[string]interface{}{"date": "2023-01-15T00:00:00Z"},
		rawConfig,
		15,
	)
	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}
	tzErr, ok := output.Error.(*dateformatter.TimezoneError)
	if !ok {
		t.Fatalf("expected TimezoneError, got %T", output.Error)
	}
	if tzErr.NodeID != "test-node-1" {
		t.Fatalf("expected node ID in error, got '%s'", tzErr.NodeID)
	}
	if tzErr.ItemIndex != 15 {
		t.Fatalf("expected ItemIndex 15 in error, got %d", tzErr.ItemIndex)
	}
	if tzErr.Timezone != "Invalid/Timezone" {
		t.Fatalf("expected timezone in error, got '%s'", tzErr.Timezone)
	}
}

// TestProcessDifferentNodeIDs tests Process with different node IDs
func TestProcessDifferentNodeIDs(t *testing.T) {
	testCases := []struct {
		nodeID string
	}{
		{"node-1"},
		{"node-abc-123"},
		{"test_node_with_underscores"},
		{"node.with.dots"},
		{"very-long-node-id-that-might-be-used-in-production"},
	}

	for _, tc := range testCases {
		t.Run(tc.nodeID, func(t *testing.T) {
			node := createTestNode(t, tc.nodeID)
			config := dateformatter.Config{
				InFormat:  "RFC3339",
				OutFormat: "DateOnly",
			}
			rawConfig, _ := json.Marshal(config)
			input := runtime.ProcessInput{
				Ctx:       context.Background(),
				Data:      map[string]interface{}{"date": "2023-01-15T00:00:00Z"},
				RawConfig: rawConfig,
				NodeId:    tc.nodeID,
				ItemIndex: 0,
			}

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for node ID %s: %v", tc.nodeID, output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data for node ID %s", tc.nodeID)
			}
		})
	}
}

// TestProcessComplexCombinations tests Process with complex combinations of options
func TestProcessComplexCombinations(t *testing.T) {
	node := createTestNode(t, "test-node-1")

	testCases := []struct {
		name      string
		config    dateformatter.Config
		dateStr   string
		shouldErr bool
	}{
		{
			name: "DateTime with both styles and timezones",
			config: dateformatter.Config{
				InFormat:    "RFC3339",
				OutFormat:   "DateTime",
				DateStyle:   string(dateformatter.DateStyleMM_DD_YYYY_Slash),
				TimeStyle:   string(dateformatter.TimeStyle12HourHM),
				InTimezone:  "UTC",
				OutTimezone: "America/New_York",
			},
			dateStr:   "2023-07-15T14:30:00Z",
			shouldErr: false,
		},
		{
			name: "DateOnly with date style and input timezone",
			config: dateformatter.Config{
				InFormat:   "DateTime",
				OutFormat:  "DateOnly",
				DateStyle:  string(dateformatter.DateStyleDD_MM_YYYY_Slash),
				InTimezone: "America/Los_Angeles",
			},
			dateStr:   "2023-01-15 10:30:00",
			shouldErr: false,
		},
		{
			name: "TimeOnly with time style and output timezone",
			config: dateformatter.Config{
				InFormat:    "RFC3339",
				OutFormat:   "TimeOnly",
				TimeStyle:   string(dateformatter.TimeStyle24HourHM),
				OutTimezone: "Europe/London",
			},
			dateStr:   "2023-01-15T14:30:00Z",
			shouldErr: false,
		},
		{
			name: "RFC3339Nano with DateTime output",
			config: dateformatter.Config{
				InFormat:  "RFC3339Nano",
				OutFormat: "DateTime",
			},
			dateStr:   "2023-01-15T14:30:00.123456789Z",
			shouldErr: false,
		},
		{
			name: "StampMilli with DateOnly output",
			config: dateformatter.Config{
				InFormat:  "StampMilli",
				OutFormat: "DateOnly",
			},
			dateStr:   "Jan 15 14:30:00.123",
			shouldErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rawConfig, _ := json.Marshal(tc.config)
			input := createProcessInput(
				map[string]interface{}{"date": tc.dateStr},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if tc.shouldErr {
				if output.Error == nil {
					t.Fatalf("expected error for %s", tc.name)
				}
			} else {
				if output.Error != nil {
					t.Fatalf("unexpected error for %s: %v", tc.name, output.Error)
				}
				if output.Data == nil {
					t.Fatalf("expected output data for %s", tc.name)
				}
				result := output.Data["result"]
				if result == nil {
					t.Fatalf("expected non-nil result for %s", tc.name)
				}
			}
		})
	}
}

// TestProcessInvalidDateTimePartialFormats tests invalid partial DateTime formats
func TestProcessInvalidDateTimePartialFormats(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "DateTime",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	// Test invalid partial format (too short)
	input := createProcessInput(
		map[string]interface{}{"date": "2023-01"},
		rawConfig,
		0,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid partial date format")
	}
	if _, ok := output.Error.(*dateformatter.ParseError); !ok {
		t.Fatalf("expected ParseError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessVariousNonStringTypes tests Process with various non-string date types
func TestProcessVariousNonStringTypes(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	testCases := []struct {
		name      string
		dateVal   interface{}
		shouldErr bool
	}{
		{"integer", 12345, true},
		{"float", 12345.67, true},
		{"boolean", true, true},
		{"array", []string{"2023-01-15"}, true},
		{"map", map[string]string{"date": "2023-01-15"}, true},
		{"nil", nil, false}, // nil is handled gracefully
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := createProcessInput(
				map[string]interface{}{"date": tc.dateVal},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if tc.shouldErr {
				if output.Error == nil {
					t.Fatalf("expected error for %s type", tc.name)
				}
				if _, ok := output.Error.(*dateformatter.InputError); !ok {
					t.Fatalf("expected InputError for %s type, got %T", tc.name, output.Error)
				}
			} else {
				if output.Error != nil {
					t.Fatalf("unexpected error for %s type: %v", tc.name, output.Error)
				}
			}
		})
	}
}

// TestProcessEmptyStringVariations tests various empty string variations
func TestProcessEmptyStringVariations(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := dateformatter.Config{
		InFormat:  "RFC3339",
		OutFormat: "DateOnly",
	}
	rawConfig, _ := json.Marshal(config)

	emptyStrings := []string{
		"",
		" ",
		"  ",
		"\t",
		"\n",
		"\r\n",
		"   \t  \n  ",
	}

	for _, emptyStr := range emptyStrings {
		t.Run("empty_"+emptyStr, func(t *testing.T) {
			input := createProcessInput(
				map[string]interface{}{"date": emptyStr},
				rawConfig,
				0,
			)

			output := node.Process(input)
			if output.Error != nil {
				t.Fatalf("unexpected error for empty string variation: %v", output.Error)
			}
			if output.Data == nil {
				t.Fatalf("expected output data")
			}
			result := output.Data["result"]
			if result != nil {
				t.Fatalf("expected nil result for empty string, got %v", result)
			}
		})
	}
}
