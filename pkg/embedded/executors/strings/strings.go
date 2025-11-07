package strings

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/process/strings"
)

// Executor implements NodeExecutor for string operations
type Executor struct{}

// StringsConfig defines the configuration for string operations
type StringsConfig struct {
	Operation string                 `json:"operation"` // Operation name (e.g., "concatenate", "split", "trim", etc.)
	Params    map[string]interface{} `json:"params"`    // Operation-specific parameters
}

// NewExecutor creates a new strings executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a string operation
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var strConfig StringsConfig
	if err := json.Unmarshal(config.Configuration, &strConfig); err != nil {
		return nil, fmt.Errorf("failed to parse strings configuration: %w", err)
	}

	// Parse input
	var input map[string]interface{}
	if err := json.Unmarshal(config.Input, &input); err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}

	// Execute operation
	result, err := e.executeOperation(strConfig.Operation, strConfig.Params, input)
	if err != nil {
		return nil, err
	}

	// Marshal result
	output, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return output, nil
}

// executeOperation executes the specified string operation
func (e *Executor) executeOperation(operation string, params map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	switch operation {
	case "concatenate":
		separator := getString(params, "separator", "")
		parts := getStringSlice(params, "parts", nil)
		if parts == nil {
			// If parts not provided, use input values
			parts = extractStringValues(input)
		}
		result["value"] = strings.Concatenate(separator, parts...)

	case "split":
		str := getString(params, "string", getString(input, "string", ""))
		delimiter := getString(params, "delimiter", "")
		result["value"] = strings.Split(str, delimiter)

	case "join":
		items := getStringSlice(params, "items", getStringSlice(input, "items", []string{}))
		separator := getString(params, "separator", "")
		result["value"] = strings.Join(items, separator)

	case "trim":
		str := getString(params, "string", getString(input, "string", ""))
		cutset := getString(params, "cutset", "")
		result["value"] = strings.Trim(str, cutset)

	case "replace":
		str := getString(params, "string", getString(input, "string", ""))
		old := getString(params, "old", "")
		new := getString(params, "new", "")
		count := getInt(params, "count", -1)
		useRegex := getBool(params, "use_regex", false)
		replaced, err := strings.Replace(str, old, new, count, useRegex)
		if err != nil {
			return nil, fmt.Errorf("replace operation failed: %w", err)
		}
		result["value"] = replaced

	case "substring":
		str := getString(params, "string", getString(input, "string", ""))
		start := getInt(params, "start", 0)
		end := getInt(params, "end", 0)
		result["value"] = strings.Substring(str, start, end)

	case "to_upper":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.ToUpper(str)

	case "to_lower":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.ToLower(str)

	case "title_case":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.TitleCase(str)

	case "capitalize":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.Capitalize(str)

	case "contains":
		str := getString(params, "string", getString(input, "string", ""))
		sub := getString(params, "substring", "")
		useRegex := getBool(params, "use_regex", false)
		contains, err := strings.Contains(str, sub, useRegex)
		if err != nil {
			return nil, fmt.Errorf("contains operation failed: %w", err)
		}
		result["value"] = contains

	case "length":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.Length(str)

	case "regex_extract":
		str := getString(params, "string", getString(input, "string", ""))
		pattern := getString(params, "pattern", "")
		matches, err := strings.RegexExtract(str, pattern)
		if err != nil {
			return nil, fmt.Errorf("regex_extract operation failed: %w", err)
		}
		result["value"] = matches

	case "format":
		template := getString(params, "template", getString(input, "template", ""))
		data := getStringMap(params, "data", getStringMap(input, "data", map[string]string{}))
		result["value"] = strings.Format(template, data)

	case "base64_encode":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.Base64Encode(str)

	case "base64_decode":
		str := getString(params, "string", getString(input, "string", ""))
		decoded, err := strings.Base64Decode(str)
		if err != nil {
			return nil, fmt.Errorf("base64_decode operation failed: %w", err)
		}
		result["value"] = decoded

	case "uri_encode":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.URIEncode(str)

	case "uri_decode":
		str := getString(params, "string", getString(input, "string", ""))
		decoded, err := strings.URIDecode(str)
		if err != nil {
			return nil, fmt.Errorf("uri_decode operation failed: %w", err)
		}
		result["value"] = decoded

	case "normalize":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = strings.Normalize(str)

	default:
		return nil, fmt.Errorf("unknown string operation: %s", operation)
	}

	return result, nil
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-strings"
}

// Helper functions to extract values from maps

func getString(m map[string]interface{}, key string, defaultValue string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultValue
}

func getInt(m map[string]interface{}, key string, defaultValue int) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case float64:
			return int(val)
		case int64:
			return int(val)
		}
	}
	return defaultValue
}

func getBool(m map[string]interface{}, key string, defaultValue bool) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return defaultValue
}

func getStringSlice(m map[string]interface{}, key string, defaultValue []string) []string {
	if v, ok := m[key]; ok {
		if items, ok := v.([]interface{}); ok {
			result := make([]string, 0, len(items))
			for _, item := range items {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		}
		if items, ok := v.([]string); ok {
			return items
		}
	}
	return defaultValue
}

func getStringMap(m map[string]interface{}, key string, defaultValue map[string]string) map[string]string {
	if v, ok := m[key]; ok {
		if dataMap, ok := v.(map[string]interface{}); ok {
			result := make(map[string]string)
			for k, val := range dataMap {
				if s, ok := val.(string); ok {
					result[k] = s
				}
			}
			return result
		}
		if dataMap, ok := v.(map[string]string); ok {
			return dataMap
		}
	}
	return defaultValue
}

func extractStringValues(m map[string]interface{}) []string {
	result := make([]string, 0)
	for _, v := range m {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

