package csv

import "github.com/wehubfusion/Icarus/pkg/schema/json"

// CSVSchema represents a typed CSV schema definition
type CSVSchema struct {
	Name          string                `json:"name,omitempty"`
	Delimiter     string                `json:"delimiter,omitempty"`
	ColumnHeaders map[string]*CSVColumn `json:"columnHeaders"`
	ColumnOrder   []string              `json:"-"` // preserves declared column order
}

// CSVColumn describes a single CSV column
type CSVColumn struct {
	Position    *int                  `json:"position,omitempty"`
	Type        json.SchemaType       `json:"type"`
	Required    *bool                 `json:"required,omitempty"`
	Default     interface{}           `json:"default,omitempty"`
	Description string                `json:"description,omitempty"`
	Validation  *json.ValidationRules `json:"validation,omitempty"`
}

// EffectiveDelimiter returns the configured delimiter or a default comma
func (c *CSVSchema) EffectiveDelimiter() string {
	if c == nil || c.Delimiter == "" {
		return ","
	}
	return c.Delimiter
}

// ToObjectSchema converts a CSV schema into an object schema for reuse in validation/transformers
func (c *CSVSchema) ToObjectSchema() *json.Schema {
	props := make(map[string]*json.Property)
	for name, col := range c.ColumnHeaders {
		props[name] = &json.Property{
			Type:        col.Type,
			Required:    col.Required,
			Default:     col.Default,
			Description: col.Description,
			Validation:  col.Validation,
		}
	}
	return &json.Schema{
		Type:       json.TypeObject,
		Properties: props,
	}
}
