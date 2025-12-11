package jsonops

import (
	"encoding/json"
	"fmt"
)

// Config defines the configuration for JSON operations in embeddedv2
type Config struct {
	// Operation specifies which JSON operation to perform
	// Valid values: "parse", "produce"
	Operation string `json:"operation"`

	// SchemaID is a reference to a schema in Morpheus (enriched by Elysium)
	SchemaID string `json:"schema_id"`

	// Schema is the Icarus schema definition (optional, used if schema_id is not enriched)
	Schema json.RawMessage `json:"schema,omitempty"`

	// ApplyDefaults applies default values from schema (parse operation)
	// Default: true for parse, false for produce
	ApplyDefaults *bool `json:"apply_defaults,omitempty"`

	// StructureData removes fields not defined in schema
	// Default: false for parse, true for produce
	StructureData *bool `json:"structure_data,omitempty"`

	// StrictValidation fails immediately on validation errors
	// Default: false for parse, true for produce
	StrictValidation *bool `json:"strict_validation,omitempty"`

	// Pretty formats the JSON before base64 encoding (produce operation only)
	// Default: false
	Pretty bool `json:"pretty,omitempty"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Operation is required
	if c.Operation == "" {
		return fmt.Errorf("operation field is required")
	}

	// Validate operation value
	validOperations := map[string]bool{
		"parse":   true,
		"produce": true,
	}

	if !validOperations[c.Operation] {
		return fmt.Errorf("invalid operation '%s', must be one of: parse, produce", c.Operation)
	}

	// Must have either schema_id or schema
	if c.SchemaID == "" && len(c.Schema) == 0 {
		return fmt.Errorf("either 'schema_id' or 'schema' must be provided")
	}

	return nil
}

// GetApplyDefaults returns the apply_defaults value with operation-specific defaults
func (c *Config) GetApplyDefaults() bool {
	if c.ApplyDefaults != nil {
		return *c.ApplyDefaults
	}
	// Default to true for parse (to fill in missing fields), false for produce
	return c.Operation == "parse"
}

// GetStructureData returns the structure_data value with operation-specific defaults
func (c *Config) GetStructureData() bool {
	if c.StructureData != nil {
		return *c.StructureData
	}
	// Default to false for parse (allow extra fields), true for produce (clean output)
	return c.Operation == "produce"
}

// GetStrictValidation returns the strict_validation value with operation-specific defaults
func (c *Config) GetStrictValidation() bool {
	if c.StrictValidation != nil {
		return *c.StrictValidation
	}
	// Default to false for parse (lenient), true for produce (ensure valid output)
	return c.Operation == "produce"
}
