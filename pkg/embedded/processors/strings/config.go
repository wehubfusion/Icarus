package strings

import (
	"encoding/json"
	"fmt"
)

// ManualInput describes a required manual input (name/type) per schema.
type ManualInput struct {
	Name string `json:"name"`
	Type string `json:"type"` // string | number | date
}

// Config defines configuration for strings operations.
// Supports flat config from seed-node-schemas.sql: all keys except "operation"
// and "manual_inputs" are placed into Params.
type Config struct {
	Operation    string                 `json:"operation"`
	Params       map[string]interface{} `json:"params"`
	ManualInputs []ManualInput          `json:"manual_inputs"`
}

// UnmarshalJSON supports flat config (label, delimiter, separator, etc.) by
// putting every key other than "operation" and "manual_inputs" into Params.
func (c *Config) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	c.Params = make(map[string]interface{})
	for k, v := range raw {
		switch k {
		case "operation":
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return err
			}
			c.Operation = s
		case "manual_inputs":
			var inputs []ManualInput
			if err := json.Unmarshal(v, &inputs); err != nil {
				return err
			}
			c.ManualInputs = inputs
		case "params":
			var sub map[string]interface{}
			if err := json.Unmarshal(v, &sub); err != nil {
				return err
			}
			for k2, v2 := range sub {
				c.Params[k2] = v2
			}
		default:
			var val interface{}
			if err := json.Unmarshal(v, &val); err != nil {
				return err
			}
			c.Params[k] = val
		}
	}
	return nil
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

	for i := range c.ManualInputs {
		mi := &c.ManualInputs[i]
		if mi.Name == "" {
			return NewConfigError(nodeID, fmt.Sprintf("manual_inputs[%d].name", i), "name is required", nil)
		}
		if mi.Type == "" {
			mi.Type = "string" // seed-node-schemas.sql concatenate/join only define name
		}
		switch mi.Type {
		case "string", "number", "date":
		default:
			return NewConfigError(nodeID, fmt.Sprintf("manual_inputs[%d].type", i), "type must be string|number|date", nil)
		}
	}
	return nil
}
