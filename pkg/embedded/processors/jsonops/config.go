package jsonops

import (
	"encoding/json"
	"fmt"
)

// Config defines the configuration for JSON actions in embedded
type Config struct {
	// Action specifies which JSON action to perform
	// Valid values: "parse", "produce"
	Action string `json:"action"`

	// SchemaID is a reference to a schema in Morpheus (enriched by Elysium)
	SchemaID string `json:"schema_id"`

	// Schema is the Icarus schema definition (optional, used if schema_id is not enriched)
	Schema json.RawMessage `json:"schema,omitempty"`

	// ApplyDefaults applies default values from schema (parse action)
	// Default: true for parse, false for produce
	ApplyDefaults *bool `json:"apply_defaults,omitempty"`

	// StructureData removes fields not defined in schema
	// Default: false for parse, true for produce
	StructureData *bool `json:"structure_data,omitempty"`

	// StrictValidation maps to Mode=STRICT when true, making the processor
	// return a Go error (in addition to the result payload) when validation fails.
	// Deprecated: prefer setting the Mode field directly in ProcessOptions.
	// Default: false for parse, true for produce.
	StrictValidation *bool `json:"strict_validation,omitempty"`

	// Pretty formats the JSON before base64 encoding (produce action only)
	// Default: false
	Pretty bool `json:"pretty,omitempty"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Action is required
	if c.Action == "" {
		return fmt.Errorf("action field is required")
	}

	// Validate action value
	validActions := map[string]bool{
		"parse":   true,
		"produce": true,
	}

	if !validActions[c.Action] {
		return fmt.Errorf("invalid action '%s', must be one of: parse, produce", c.Action)
	}

	// Must have either schema_id or schema
	if c.SchemaID == "" && len(c.Schema) == 0 {
		return fmt.Errorf("either 'schema_id' or 'schema' must be provided")
	}

	return nil
}

// GetApplyDefaults returns the apply_defaults value with action-specific defaults
func (c *Config) GetApplyDefaults() bool {
	if c.ApplyDefaults != nil {
		return *c.ApplyDefaults
	}
	// Default to true for parse (to fill in missing fields), false for produce
	return c.Action == "parse"
}

// GetStructureData returns the structure_data value with action-specific defaults
func (c *Config) GetStructureData() bool {
	if c.StructureData != nil {
		return *c.StructureData
	}
	// Default to false for parse (allow extra fields), true for produce (clean output)
	return c.Action == "produce"
}

// GetStrictValidation returns whether strict (fail-on-invalid) mode should be used.
// Callers should convert the result to Mode: ValidationModeStrict in ProcessOptions.
func (c *Config) GetStrictValidation() bool {
	if c.StrictValidation != nil {
		return *c.StrictValidation
	}
	// Default: false for parse (lenient), true for produce (ensure valid output)
	return c.Action == "produce"
}
