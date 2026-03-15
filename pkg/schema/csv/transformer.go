package csv

import (
	"github.com/wehubfusion/Icarus/pkg/schema/json"
)

// ApplyCSVDefaults applies defaults to each CSV row using the CSV schema
func ApplyCSVDefaults(rows []map[string]interface{}, s *CSVSchema) ([]map[string]interface{}, error) {
	if s == nil {
		return rows, nil
	}
	t := json.NewTransformer()
	objSchema := s.ToObjectSchema()

	for i, row := range rows {
		processed, err := t.ApplyDefaults(row, objSchema)
		if err != nil {
			return nil, err
		}
		if cast, ok := processed.(map[string]interface{}); ok {
			rows[i] = cast
		}
	}

	return rows, nil
}

// StructureCSVRows removes fields not present in the CSV schema (when enabled)
func StructureCSVRows(rows []map[string]interface{}, s *CSVSchema) ([]map[string]interface{}, error) {
	if s == nil {
		return rows, nil
	}
	t := json.NewTransformer()
	objSchema := s.ToObjectSchema()

	for i, row := range rows {
		structured, err := t.StructureData(row, objSchema)
		if err != nil {
			return nil, err
		}
		if cast, ok := structured.(map[string]interface{}); ok {
			rows[i] = cast
		}
	}

	return rows, nil
}
