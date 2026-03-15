package csv

import (
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
	"github.com/wehubfusion/Icarus/pkg/schema/json"
)

// ValidateCSVRowsWithOptions validates rows against a CSV schema. When collectAllErrors is false,
// validation stops after the first error. When true, all errors are collected.
func ValidateCSVRowsWithOptions(rows []map[string]interface{}, s *CSVSchema, collectAllErrors bool) *contracts.ValidationResult {
	if s == nil {
		return &contracts.ValidationResult{Valid: true, Errors: nil}
	}
	v := json.NewValidator()
	objSchema := s.ToObjectSchema()
	var allErrs []contracts.ValidationError
	for i, row := range rows {
		if !collectAllErrors && len(allErrs) > 0 {
			break
		}
		rowPath := fmt.Sprintf("rows[%d]", i)
		res := v.ValidateWithRootPath(row, objSchema, rowPath, true)
		allErrs = append(allErrs, res.Errors...)
	}
	return &contracts.ValidationResult{
		Valid:  len(allErrs) == 0,
		Errors: allErrs,
	}
}

// ValidateCSVRows validates a JSON array of row objects against a CSV schema (collects all errors; backward compatible).
func ValidateCSVRows(rows []map[string]interface{}, s *CSVSchema) *contracts.ValidationResult {
	return ValidateCSVRowsWithOptions(rows, s, true)
}
