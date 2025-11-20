package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/dateformatter"
)

// Helper function to create date formatter configuration JSON
func createDateConfig(t *testing.T, operation string, params map[string]interface{}) []byte {
	t.Helper()

	paramsJSON, err := json.Marshal(params)
	require.NoError(t, err, "failed to marshal params")

	config := map[string]interface{}{
		"operation": operation,
		"params":    json.RawMessage(paramsJSON),
	}

	configJSON, err := json.Marshal(config)
	require.NoError(t, err, "failed to marshal config")

	return configJSON
}

// Helper function to create input JSON with date string
func createDateInput(t *testing.T, dateStr string) []byte {
	t.Helper()

	input := map[string]interface{}{
		"data": dateStr,
	}

	inputJSON, err := json.Marshal(input)
	require.NoError(t, err, "failed to marshal input")

	return inputJSON
}

// Helper function to execute date formatter and extract result
func executeDateFormat(t *testing.T, executor *dateformatter.Executor, config, input []byte) (string, error) {
	t.Helper()

	result, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: config,
		Input:         input,
	})

	if err != nil {
		return "", err
	}

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err, "failed to unmarshal output")

	dateStr, ok := output["result"].(string)
	require.True(t, ok, "output result field should be a string")

	return dateStr, nil
}

// TestDateFormatter_BasicConversion tests standard format-to-format conversions
func TestDateFormatter_BasicConversion(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		inFormat    string
		outFormat   string
		inputDate   string
		expected    string
		shouldError bool
	}{
		{
			name:      "RFC3339 to DateTime",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "2024-11-11 15:04:05",
		},
		{
			name:      "DateTime to RFC3339",
			inFormat:  "DateTime",
			outFormat: "RFC3339",
			inputDate: "2024-11-11 15:04:05",
			expected:  "2024-11-11T15:04:05Z",
		},
		{
			name:      "DateOnly to RFC1123",
			inFormat:  "DateOnly",
			outFormat: "RFC1123",
			inputDate: "2024-11-11",
			expected:  "Mon, 11 Nov 2024 00:00:00 UTC",
		},
		{
			name:      "RFC822 to DateTime",
			inFormat:  "RFC822",
			outFormat: "DateTime",
			inputDate: "11 Nov 24 15:04 UTC",
			expected:  "2024-11-11 15:04:00",
		},
		{
			name:      "ANSIC to RFC3339",
			inFormat:  "ANSIC",
			outFormat: "RFC3339",
			inputDate: "Mon Nov 11 15:04:05 2024",
			expected:  "2024-11-11T15:04:05Z",
		},
		{
			name:      "Kitchen to TimeOnly",
			inFormat:  "Kitchen",
			outFormat: "TimeOnly",
			inputDate: "3:04PM",
			expected:  "15:04:00",
		},
		{
			name:      "Stamp to DateTime",
			inFormat:  "Stamp",
			outFormat: "DateTime",
			inputDate: "Nov 11 15:04:05",
			expected:  "0000-11-11 15:04:05",
		},
		{
			name:      "StampMilli to DateTime",
			inFormat:  "StampMilli",
			outFormat: "DateTime",
			inputDate: "Nov 11 15:04:05.123",
			expected:  "0000-11-11 15:04:05",
		},
		{
			name:      "RFC1123Z to RFC3339",
			inFormat:  "RFC1123Z",
			outFormat: "RFC3339",
			inputDate: "Mon, 11 Nov 2024 15:04:05 +0000",
			expected:  "2024-11-11T15:04:05Z",
		},
		{
			name:      "UnixDate to DateTime",
			inFormat:  "UnixDate",
			outFormat: "DateTime",
			inputDate: "Mon Nov 11 15:04:05 UTC 2024",
			expected:  "2024-11-11 15:04:05",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createDateConfig(t, "format", map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
			})

			input := createDateInput(t, tt.inputDate)

			result, err := executeDateFormat(t, executor, config, input)

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDateFormatter_TimezoneConversion tests timezone transformations
func TestDateFormatter_TimezoneConversion(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		inFormat    string
		inTimezone  string
		outFormat   string
		outTimezone string
		inputDate   string
		expected    string
		shouldError bool
	}{
		{
			name:        "UTC to America/New_York",
			inFormat:    "RFC3339",
			inTimezone:  "",
			outFormat:   "DateTime",
			outTimezone: "America/New_York",
			inputDate:   "2024-11-11T15:04:05Z",
			expected:    "2024-11-11 10:04:05", // EST is UTC-5
		},
		{
			name:        "America/New_York to UTC",
			inFormat:    "DateTime",
			inTimezone:  "America/New_York",
			outFormat:   "RFC3339",
			outTimezone: "UTC",
			inputDate:   "2024-11-11 10:04:05",
			expected:    "2024-11-11T15:04:05Z",
		},
		{
			name:        "UTC to Asia/Tokyo",
			inFormat:    "RFC3339",
			inTimezone:  "",
			outFormat:   "DateTime",
			outTimezone: "Asia/Tokyo",
			inputDate:   "2024-11-11T15:04:05Z",
			expected:    "2024-11-12 00:04:05", // JST is UTC+9
		},
		{
			name:        "UTC to Europe/London",
			inFormat:    "RFC3339",
			inTimezone:  "",
			outFormat:   "DateTime",
			outTimezone: "Europe/London",
			inputDate:   "2024-11-11T15:04:05Z",
			expected:    "2024-11-11 15:04:05", // GMT is UTC+0 in winter
		},
		{
			name:        "America/Los_Angeles to Europe/Paris",
			inFormat:    "DateTime",
			inTimezone:  "America/Los_Angeles",
			outFormat:   "DateTime",
			outTimezone: "Europe/Paris",
			inputDate:   "2024-11-11 07:04:05",
			expected:    "2024-11-11 16:04:05", // PST (UTC-8) to CET (UTC+1) = +9 hours
		},
		{
			name:        "Same timezone input and output",
			inFormat:    "DateTime",
			inTimezone:  "America/New_York",
			outFormat:   "DateTime",
			outTimezone: "America/New_York",
			inputDate:   "2024-11-11 10:04:05",
			expected:    "2024-11-11 10:04:05",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
			}

			if tt.inTimezone != "" {
				params["in_timezone"] = tt.inTimezone
			}

			if tt.outTimezone != "" {
				params["out_timezone"] = tt.outTimezone
			}

			config := createDateConfig(t, "format", params)
			input := createDateInput(t, tt.inputDate)

			result, err := executeDateFormat(t, executor, config, input)

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDateFormatter_DateStyles tests date style variations
func TestDateFormatter_DateStyles(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		inFormat    string
		outFormat   string
		dateStyle   string
		inputDate   string
		expected    string
		shouldError bool
	}{
		{
			name:      "DateOnly with YYYY_MM_DD",
			inFormat:  "RFC3339",
			outFormat: "DateOnly",
			dateStyle: "YYYY_MM_DD",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "2024-11-11",
		},
		{
			name:      "DateOnly with DD_MM_YYYY",
			inFormat:  "RFC3339",
			outFormat: "DateOnly",
			dateStyle: "DD_MM_YYYY",
			inputDate: "2024-01-11T15:04:05Z",
			expected:  "11-01-2024",
		},
		{
			name:      "DateOnly with MM_DD_YYYY",
			inFormat:  "RFC3339",
			outFormat: "DateOnly",
			dateStyle: "MM_DD_YYYY",
			inputDate: "2024-01-11T15:04:05Z",
			expected:  "01-11-2024",
		},
		{
			name:      "DateOnly with YYYY_MM_DD_SLASH",
			inFormat:  "RFC3339",
			outFormat: "DateOnly",
			dateStyle: "YYYY_MM_DD_SLASH",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "2024/11/11",
		},
		{
			name:      "DateOnly with DD_MM_YYYY_SLASH",
			inFormat:  "RFC3339",
			outFormat: "DateOnly",
			dateStyle: "DD_MM_YYYY_SLASH",
			inputDate: "2024-01-11T15:04:05Z",
			expected:  "11/01/2024",
		},
		{
			name:      "DateOnly with MM_DD_YYYY_SLASH",
			inFormat:  "RFC3339",
			outFormat: "DateOnly",
			dateStyle: "MM_DD_YYYY_SLASH",
			inputDate: "2024-01-11T15:04:05Z",
			expected:  "01/11/2024",
		},
		{
			name:      "DateTime with DD_MM_YYYY",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			dateStyle: "DD_MM_YYYY",
			inputDate: "2024-01-11T15:04:05Z",
			expected:  "11-01-2024 15:04:05",
		},
		{
			name:        "date_style with RFC3339 should error",
			inFormat:    "RFC3339",
			outFormat:   "RFC3339",
			dateStyle:   "YYYY_MM_DD",
			inputDate:   "2024-11-11T15:04:05Z",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
			}

			if tt.dateStyle != "" {
				params["date_style"] = tt.dateStyle
			}

			config := createDateConfig(t, "format", params)
			input := createDateInput(t, tt.inputDate)

			result, err := executeDateFormat(t, executor, config, input)

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDateFormatter_TimeStyles tests time style variations
func TestDateFormatter_TimeStyles(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		inFormat    string
		outFormat   string
		timeStyle   string
		inputDate   string
		expected    string
		shouldError bool
	}{
		{
			name:      "TimeOnly with 24_HOUR",
			inFormat:  "RFC3339",
			outFormat: "TimeOnly",
			timeStyle: "24_HOUR",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "15:04:05",
		},
		{
			name:      "TimeOnly with 12_HOUR",
			inFormat:  "RFC3339",
			outFormat: "TimeOnly",
			timeStyle: "12_HOUR",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "03:04:05 PM",
		},
		{
			name:      "TimeOnly with 24_HOUR_HM",
			inFormat:  "RFC3339",
			outFormat: "TimeOnly",
			timeStyle: "24_HOUR_HM",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "15:04",
		},
		{
			name:      "TimeOnly with 12_HOUR_HM",
			inFormat:  "RFC3339",
			outFormat: "TimeOnly",
			timeStyle: "12_HOUR_HM",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "03:04 PM",
		},
		{
			name:      "DateTime with 12_HOUR",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			timeStyle: "12_HOUR",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "2024-11-11 03:04:05 PM",
		},
		{
			name:        "time_style with DateOnly should error",
			inFormat:    "RFC3339",
			outFormat:   "DateOnly",
			timeStyle:   "24_HOUR",
			inputDate:   "2024-11-11T15:04:05Z",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
			}

			if tt.timeStyle != "" {
				params["time_style"] = tt.timeStyle
			}

			config := createDateConfig(t, "format", params)
			input := createDateInput(t, tt.inputDate)

			result, err := executeDateFormat(t, executor, config, input)

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDateFormatter_CombinedStyles tests DateTime with both date and time styles
func TestDateFormatter_CombinedStyles(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name      string
		inFormat  string
		outFormat string
		dateStyle string
		timeStyle string
		inputDate string
		expected  string
	}{
		{
			name:      "YYYY_MM_DD + 24_HOUR",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			dateStyle: "YYYY_MM_DD",
			timeStyle: "24_HOUR",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "2024-11-11 15:04:05",
		},
		{
			name:      "MM_DD_YYYY + 12_HOUR",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			dateStyle: "MM_DD_YYYY",
			timeStyle: "12_HOUR",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "11-11-2024 03:04:05 PM",
		},
		{
			name:      "DD_MM_YYYY_SLASH + 12_HOUR_HM",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			dateStyle: "DD_MM_YYYY_SLASH",
			timeStyle: "12_HOUR_HM",
			inputDate: "2024-01-11T15:04:05Z",
			expected:  "11/01/2024 03:04 PM",
		},
		{
			name:      "YYYY_MM_DD_SLASH + 24_HOUR_HM",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			dateStyle: "YYYY_MM_DD_SLASH",
			timeStyle: "24_HOUR_HM",
			inputDate: "2024-11-11T08:30:45Z",
			expected:  "2024/11/11 08:30",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
				"date_style": tt.dateStyle,
				"time_style": tt.timeStyle,
			}

			config := createDateConfig(t, "format", params)
			input := createDateInput(t, tt.inputDate)

			result, err := executeDateFormat(t, executor, config, input)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDateFormatter_InputNormalization tests automatic input normalization
func TestDateFormatter_InputNormalization(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name      string
		inFormat  string
		outFormat string
		inputDate string
		expected  string
	}{
		{
			name:      "DateTime partial input - date only",
			inFormat:  "DateTime",
			outFormat: "RFC3339",
			inputDate: "2024-11-11",
			expected:  "2024-11-11T00:00:00Z",
		},
		{
			name:      "DateTime partial input - without seconds",
			inFormat:  "DateTime",
			outFormat: "RFC3339",
			inputDate: "2024-11-11 15:04",
			expected:  "2024-11-11T15:04:00Z",
		},
		{
			name:      "DateOnly compact format YYYYMMDD",
			inFormat:  "DateOnly",
			outFormat: "DateTime",
			inputDate: "20241111",
			expected:  "2024-11-11 00:00:00",
		},
		{
			name:      "Valid input should not be modified",
			inFormat:  "DateTime",
			outFormat: "RFC3339",
			inputDate: "2024-11-11 15:04:05",
			expected:  "2024-11-11T15:04:05Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createDateConfig(t, "format", map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
			})

			input := createDateInput(t, tt.inputDate)

			result, err := executeDateFormat(t, executor, config, input)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDateFormatter_ConfigValidation tests configuration validation errors
func TestDateFormatter_ConfigValidation(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		operation   string
		params      map[string]interface{}
		inputDate   string
		expectedErr string
	}{
		{
			name:        "empty operation",
			operation:   "",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": "DateTime"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "operation cannot be empty",
		},
		{
			name:        "invalid operation",
			operation:   "invalid",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": "DateTime"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "invalid operation",
		},
		{
			name:        "empty in_format",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "", "out_format": "DateTime"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "input format cannot be empty",
		},
		{
			name:        "invalid in_format",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "BadFormat", "out_format": "DateTime"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "invalid input format",
		},
		{
			name:        "empty out_format",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": ""},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "output format cannot be empty",
		},
		{
			name:        "invalid out_format",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": "BadFormat"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "invalid output format",
		},
		{
			name:        "date_style with RFC3339",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": "RFC3339", "date_style": "YYYY_MM_DD"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "date_style can only be used with DateOnly or DateTime",
		},
		{
			name:        "time_style with DateOnly",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": "DateOnly", "time_style": "24_HOUR"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "time_style can only be used with TimeOnly or DateTime",
		},
		{
			name:        "invalid date_style value",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": "DateOnly", "date_style": "INVALID"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "invalid date style",
		},
		{
			name:        "invalid time_style value",
			operation:   "format",
			params:      map[string]interface{}{"in_format": "RFC3339", "out_format": "TimeOnly", "time_style": "INVALID"},
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "invalid time style",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createDateConfig(t, tt.operation, tt.params)
			input := createDateInput(t, tt.inputDate)

			_, err := executeDateFormat(t, executor, config, input)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

// TestDateFormatter_InputValidation tests input validation errors
func TestDateFormatter_InputValidation(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		input       []byte
		expectedErr string
	}{
		{
			name:        "missing data field",
			input:       []byte(`{"other": "value"}`),
			expectedErr: "input must contain a 'data' field",
		},
		{
			name:        "empty data string",
			input:       []byte(`{"data": ""}`),
			expectedErr: "date string cannot be empty",
		},
		{
			name:        "invalid JSON",
			input:       []byte(`{invalid json`),
			expectedErr: "failed to parse input JSON",
		},
		{
			name:        "data field with wrong type",
			input:       []byte(`{"data": 12345}`),
			expectedErr: "'data' field must be a string value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createDateConfig(t, "format", map[string]interface{}{
				"in_format":  "RFC3339",
				"out_format": "DateTime",
			})

			_, err := executeDateFormat(t, executor, config, tt.input)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

// TestDateFormatter_ParseErrors tests date parsing failures
func TestDateFormatter_ParseErrors(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		inFormat    string
		outFormat   string
		inTimezone  string
		outTimezone string
		inputDate   string
		expectedErr string
	}{
		{
			name:        "wrong format for input",
			inFormat:    "RFC3339",
			outFormat:   "DateTime",
			inputDate:   "2024-11-11 15:04:05",
			expectedErr: "invalid date format",
		},
		{
			name:        "invalid month",
			inFormat:    "DateOnly",
			outFormat:   "DateTime",
			inputDate:   "2024-13-01",
			expectedErr: "invalid date format",
		},
		{
			name:        "invalid day",
			inFormat:    "DateOnly",
			outFormat:   "DateTime",
			inputDate:   "2024-11-32",
			expectedErr: "invalid date format",
		},
		{
			name:        "malformed date string",
			inFormat:    "RFC3339",
			outFormat:   "DateTime",
			inputDate:   "not-a-date",
			expectedErr: "invalid date format",
		},
		{
			name:        "invalid input timezone",
			inFormat:    "DateTime",
			inTimezone:  "BadTimezone",
			outFormat:   "DateTime",
			inputDate:   "2024-11-11 15:04:05",
			expectedErr: "invalid input timezone",
		},
		{
			name:        "invalid output timezone",
			inFormat:    "RFC3339",
			outFormat:   "DateTime",
			outTimezone: "BadTimezone",
			inputDate:   "2024-11-11T15:04:05Z",
			expectedErr: "invalid output timezone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
			}

			if tt.inTimezone != "" {
				params["in_timezone"] = tt.inTimezone
			}

			if tt.outTimezone != "" {
				params["out_timezone"] = tt.outTimezone
			}

			config := createDateConfig(t, "format", params)
			input := createDateInput(t, tt.inputDate)

			_, err := executeDateFormat(t, executor, config, input)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

// TestDateFormatter_EdgeCases tests boundary conditions and special dates
func TestDateFormatter_EdgeCases(t *testing.T) {
	executor := dateformatter.NewExecutor()

	tests := []struct {
		name        string
		inFormat    string
		outFormat   string
		inputDate   string
		expected    string
		shouldError bool
	}{
		{
			name:      "leap year - Feb 29, 2024",
			inFormat:  "DateOnly",
			outFormat: "DateTime",
			inputDate: "2024-02-29",
			expected:  "2024-02-29 00:00:00",
		},
		{
			name:        "non-leap year - Feb 29, 2023",
			inFormat:    "DateOnly",
			outFormat:   "DateTime",
			inputDate:   "2023-02-29",
			shouldError: true,
		},
		{
			name:      "midnight time",
			inFormat:  "DateTime",
			outFormat: "TimeOnly",
			inputDate: "2024-11-11 00:00:00",
			expected:  "00:00:00",
		},
		{
			name:      "end of day",
			inFormat:  "DateTime",
			outFormat: "TimeOnly",
			inputDate: "2024-11-11 23:59:59",
			expected:  "23:59:59",
		},
		{
			name:      "end of year",
			inFormat:  "DateOnly",
			outFormat: "RFC3339",
			inputDate: "2024-12-31",
			expected:  "2024-12-31T00:00:00Z",
		},
		{
			name:      "nanosecond precision",
			inFormat:  "RFC3339Nano",
			outFormat: "DateTime",
			inputDate: "2024-11-11T15:04:05.123456789Z",
			expected:  "2024-11-11 15:04:05",
		},
		{
			name:      "empty timezone uses default",
			inFormat:  "RFC3339",
			outFormat: "DateTime",
			inputDate: "2024-11-11T15:04:05Z",
			expected:  "2024-11-11 15:04:05",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createDateConfig(t, "format", map[string]interface{}{
				"in_format":  tt.inFormat,
				"out_format": tt.outFormat,
			})

			input := createDateInput(t, tt.inputDate)

			result, err := executeDateFormat(t, executor, config, input)

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
