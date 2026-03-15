package csv

import (
	"fmt"
	"strings"

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
		res := v.ValidateWithOptions(row, objSchema, true)
		rowPath := fmt.Sprintf("rows[%d]", i)
		for _, e := range res.Errors {
			p := e.Path
			if strings.HasPrefix(p, "root.") {
				p = rowPath + "." + strings.TrimPrefix(p, "root.")
			} else if p == "root" {
				p = rowPath
			} else {
				p = rowPath + "." + p
			}
			allErrs = append(allErrs, contracts.ValidationError{Path: p, Message: e.Message, Code: e.Code})
		}
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
