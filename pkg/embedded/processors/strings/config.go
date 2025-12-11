package strings

import "fmt"

// ManualInput describes a required manual input (name/type) per schema.
type ManualInput struct {
	Name string `json:"name"`
	Type string `json:"type"` // string | number | date
}

// Config defines configuration for strings operations.
type Config struct {
	Operation    string                 `json:"operation"`
	Params       map[string]interface{} `json:"params"`
	ManualInputs []ManualInput          `json:"manual_inputs"`
}

var supportedOperations = map[string]struct{}{
	"concatenate":   {},
	"split":         {},
	"join":          {},
	"trim":          {},
	"replace":       {},
	"substring":     {},
	"to_upper":      {},
	"to_lower":      {},
	"title_case":    {},
	"capitalize":    {},
	"contains":      {},
	"length":        {},
	"regex_extract": {},
	"format":        {},
	"base64_encode": {},
	"base64_decode": {},
	"uri_encode":    {},
	"uri_decode":    {},
	"normalize":     {},
}

// Validate checks if the configuration is valid.
func (c *Config) Validate(nodeID string) error {
	if c.Operation == "" {
		return NewConfigError(nodeID, "operation", "operation cannot be empty", nil)
	}
	if _, ok := supportedOperations[c.Operation]; !ok {
		return NewConfigError(nodeID, "operation", fmt.Sprintf("unsupported operation '%s'", c.Operation), nil)
	}

	for i, mi := range c.ManualInputs {
		if mi.Name == "" {
			return NewConfigError(nodeID, fmt.Sprintf("manual_inputs[%d].name", i), "name is required", nil)
		}
		if mi.Type == "" {
			return NewConfigError(nodeID, fmt.Sprintf("manual_inputs[%d].type", i), "type is required", nil)
		}
		switch mi.Type {
		case "string", "number", "date":
		default:
			return NewConfigError(nodeID, fmt.Sprintf("manual_inputs[%d].type", i), "type must be string|number|date", nil)
		}
	}
	return nil
}
