package schema

import (
	"github.com/wehubfusion/Icarus/pkg/schema/csv"
	schemajson "github.com/wehubfusion/Icarus/pkg/schema/json"
)

// Transformer provides JSON and CSV transformation; it wraps the json transformer
// and adds CSV methods for backward compatibility.
type Transformer struct {
	j *schemajson.Transformer
}

// NewTransformer returns a new transformer (backward-compatible wrapper).
func NewTransformer() *Transformer {
	return &Transformer{j: schemajson.NewTransformer()}
}

// ApplyDefaults applies default values from a JSON schema to data.
func (t *Transformer) ApplyDefaults(data interface{}, s *Schema) (interface{}, error) {
	return t.j.ApplyDefaults(data, s)
}

// StructureData ensures data conforms to schema structure (JSON).
func (t *Transformer) StructureData(data interface{}, s *Schema) (interface{}, error) {
	return t.j.StructureData(data, s)
}

// TruncateFields truncates nested fields beyond the specified level.
func (t *Transformer) TruncateFields(data interface{}, level int) interface{} {
	return t.j.TruncateFields(data, level)
}

// ApplyCSVDefaults applies defaults to each CSV row using the CSV schema.
func (t *Transformer) ApplyCSVDefaults(rows []map[string]interface{}, s *CSVSchema) ([]map[string]interface{}, error) {
	return csv.ApplyCSVDefaults(rows, s)
}

// StructureCSVRows removes fields not present in the CSV schema.
func (t *Transformer) StructureCSVRows(rows []map[string]interface{}, s *CSVSchema) ([]map[string]interface{}, error) {
	return csv.StructureCSVRows(rows, s)
}
