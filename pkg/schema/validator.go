package schema

import (
	"encoding/base64"
	"fmt"
	"regexp"
)

// Validator validates data against schemas
type Validator struct {
	formatValidators map[string]FormatValidator
}

// NewValidator creates a new schema validator
func NewValidator() *Validator {
	v := &Validator{
		formatValidators: make(map[string]FormatValidator),
	}

	// Register default format validators
	v.RegisterFormat("email", validateEmail)
	v.RegisterFormat("uri", validateURI)
	v.RegisterFormat("uuid", validateUUID)
	v.RegisterFormat("date", validateDate)
	v.RegisterFormat("datetime", validateDateTime)

	return v
}

// RegisterFormat registers a custom format validator
func (v *Validator) RegisterFormat(format string, validator FormatValidator) {
	v.formatValidators[format] = validator
}

// validationState holds errors and whether to collect all or stop after first
type validationState struct {
	errors     []ValidationError
	collectAll bool
}

func (s *validationState) add(e ValidationError) bool {
	s.errors = append(s.errors, e)
	return !s.collectAll && len(s.errors) >= 1
}

func (s *validationState) shouldStop() bool {
	return !s.collectAll && len(s.errors) >= 1
}

// ValidateWithOptions validates data against a schema. When collectAllErrors is false (default),
// validation stops after the first error. When true, all errors are collected.
func (v *Validator) ValidateWithOptions(data interface{}, schema *Schema, collectAllErrors bool) *ValidationResult {
	state := &validationState{collectAll: collectAllErrors}
	prop := &Property{
		Type:       schema.Type,
		Properties: schema.Properties,
		Items:      schema.Items,
	}
	v.validateValueIntoState(data, prop, "root", state)
	return &ValidationResult{
		Valid:  len(state.errors) == 0,
		Errors: state.errors,
	}
}

// Validate validates data against a schema (collects all errors; backward compatible).
func (v *Validator) Validate(data interface{}, schema *Schema) *ValidationResult {
	return v.ValidateWithOptions(data, schema, true)
}

// ValidateCSVRowsWithOptions validates rows against a CSV schema. When collectAllErrors is false,
// validation stops after the first error. When true, all errors are collected.
func (v *Validator) ValidateCSVRowsWithOptions(rows []map[string]interface{}, schema *CSVSchema, collectAllErrors bool) *ValidationResult {
	state := &validationState{collectAll: collectAllErrors}
	if schema == nil {
		return &ValidationResult{Valid: true, Errors: state.errors}
	}
	prop := &Property{
		Type:       TypeObject,
		Properties: schema.ToObjectSchema().Properties,
	}
	for i, row := range rows {
		if state.shouldStop() {
			break
		}
		rowPath := fmt.Sprintf("rows[%d]", i)
		v.validateValueIntoState(row, prop, rowPath, state)
	}
	return &ValidationResult{
		Valid:  len(state.errors) == 0,
		Errors: state.errors,
	}
}

// ValidateCSVRows validates a JSON array of row objects against a CSV schema (collects all errors; backward compatible).
func (v *Validator) ValidateCSVRows(rows []map[string]interface{}, schema *CSVSchema) *ValidationResult {
	return v.ValidateCSVRowsWithOptions(rows, schema, true)
}

// validateValueIntoState validates a value and appends errors to state; stops when state is full (stop-on-first-error).
func (v *Validator) validateValueIntoState(value interface{}, prop *Property, path string, state *validationState) {
	if prop.Required && value == nil {
		if state.add(ValidationError{Path: path, Message: "field is required", Code: "REQUIRED"}) {
			return
		}
		return
	}
	if value == nil {
		return
	}
	switch prop.Type {
	case TypeString:
		if str, ok := value.(string); ok {
			for _, e := range v.validateString(str, prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
		} else {
			if state.add(ValidationError{Path: path, Message: fmt.Sprintf("expected string, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeNumber:
		var num float64
		switch val := value.(type) {
		case float64:
			num = val
		case int:
			num = float64(val)
		case int64:
			num = float64(val)
		case int32:
			num = float64(val)
		default:
			if state.add(ValidationError{Path: path, Message: fmt.Sprintf("expected number, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
			return
		}
		for _, e := range v.validateNumber(num, prop.Validation, path) {
			if state.add(e) {
				return
			}
		}
	case TypeBoolean:
		if _, ok := value.(bool); !ok {
			if state.add(ValidationError{Path: path, Message: fmt.Sprintf("expected boolean, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeArray:
		if arr, ok := value.([]interface{}); ok {
			v.validateArrayIntoState(arr, prop, path, state)
		} else {
			if state.add(ValidationError{Path: path, Message: fmt.Sprintf("expected array, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeObject:
		if obj, ok := value.(map[string]interface{}); ok {
			v.validateObjectIntoState(obj, prop, path, state)
		} else {
			if state.add(ValidationError{Path: path, Message: fmt.Sprintf("expected object, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeDate, TypeDateTime:
		if str, ok := value.(string); ok {
			for _, e := range v.validateString(str, prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
		} else {
			if state.add(ValidationError{Path: path, Message: fmt.Sprintf("expected string for date/datetime, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeByte:
		switch val := value.(type) {
		case string:
			for _, e := range v.validateByte(val, prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
		case []byte:
			for _, e := range v.validateByte(string(val), prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
		default:
			if state.add(ValidationError{Path: path, Message: fmt.Sprintf("expected string or bytes, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeAny:
		// no validation
	}
}

// validateArrayIntoState validates array and items, appending to state; stops when state is full.
func (v *Validator) validateArrayIntoState(arr []interface{}, prop *Property, path string, state *validationState) {
	if prop.Validation != nil {
		if prop.Validation.MinItems != nil && len(arr) < *prop.Validation.MinItems {
			if state.add(ValidationError{
				Path: path, Message: fmt.Sprintf("array length %d is less than minimum %d", len(arr), *prop.Validation.MinItems), Code: "MIN_ITEMS",
			}) {
				return
			}
		}
		if prop.Validation.MaxItems != nil && len(arr) > *prop.Validation.MaxItems {
			if state.add(ValidationError{
				Path: path, Message: fmt.Sprintf("array length %d exceeds maximum %d", len(arr), *prop.Validation.MaxItems), Code: "MAX_ITEMS",
			}) {
				return
			}
		}
		if prop.Validation.UniqueItems {
			seen := make(map[string]bool)
			for i, item := range arr {
				key := fmt.Sprintf("%v", item)
				if seen[key] {
					if state.add(ValidationError{Path: fmt.Sprintf("%s[%d]", path, i), Message: "duplicate item found", Code: "DUPLICATE_ITEM"}) {
						return
					}
					break
				}
				seen[key] = true
			}
		}
	}
	if prop.Items != nil {
		for i, item := range arr {
			if state.shouldStop() {
				return
			}
			v.validateValueIntoState(item, prop.Items, fmt.Sprintf("%s[%d]", path, i), state)
		}
	}
}

// validateObjectIntoState validates object properties, appending to state; stops when state is full.
func (v *Validator) validateObjectIntoState(obj map[string]interface{}, prop *Property, path string, state *validationState) {
	if prop.Properties == nil {
		return
	}
	for propName, propDef := range prop.Properties {
		if state.shouldStop() {
			return
		}
		value, exists := obj[propName]
		propPath := fmt.Sprintf("%s.%s", path, propName)
		if !exists && propDef.Required {
			if state.add(ValidationError{Path: propPath, Message: "required field missing", Code: "REQUIRED"}) {
				return
			}
			continue
		}
		if exists {
			v.validateValueIntoState(value, propDef, propPath, state)
		}
	}
}

// validateValue validates a value against a property definition (used when collectAllErrors is true).
func (v *Validator) validateValue(value interface{}, prop *Property, path string) []ValidationError {
	var errors []ValidationError

	// Check required
	if prop.Required && value == nil {
		errors = append(errors, ValidationError{
			Path:    path,
			Message: "field is required",
			Code:    "REQUIRED",
		})
		return errors
	}

	// If value is nil and not required, it's valid
	if value == nil {
		return errors
	}

	// Type-specific validation
	switch prop.Type {
	case TypeString:
		if str, ok := value.(string); ok {
			errors = append(errors, v.validateString(str, prop.Validation, path)...)
		} else {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected string, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeNumber:
		var num float64
		switch val := value.(type) {
		case float64:
			num = val
		case int:
			num = float64(val)
		case int64:
			num = float64(val)
		case int32:
			num = float64(val)
		default:
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected number, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
			return errors
		}
		errors = append(errors, v.validateNumber(num, prop.Validation, path)...)

	case TypeBoolean:
		if _, ok := value.(bool); !ok {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected boolean, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeArray:
		if arr, ok := value.([]interface{}); ok {
			errors = append(errors, v.validateArray(arr, prop, path)...)
		} else {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected array, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeObject:
		if obj, ok := value.(map[string]interface{}); ok {
			errors = append(errors, v.validateObject(obj, prop, path)...)
		} else {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected object, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeDate, TypeDateTime:
		// Dates/DateTimes are typically strings with format validation
		if str, ok := value.(string); ok {
			errors = append(errors, v.validateString(str, prop.Validation, path)...)
		} else {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected string for date/datetime, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeByte:
		// Byte data can be string (base64) or byte array
		switch val := value.(type) {
		case string:
			errors = append(errors, v.validateByte(val, prop.Validation, path)...)
		case []byte:
			errors = append(errors, v.validateByte(string(val), prop.Validation, path)...)
		default:
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected string or bytes, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeAny:
		// Any type - no validation needed
	}

	return errors
}

// validateString validates string-specific rules
func (v *Validator) validateString(value string, rules *ValidationRules, path string) []ValidationError {
	var errors []ValidationError

	if rules == nil {
		return errors
	}

	// MinLength validation
	if rules.MinLength != nil && len(value) < *rules.MinLength {
		errors = append(errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("length %d is less than minimum %d", len(value), *rules.MinLength),
			Code:    "MIN_LENGTH",
		})
	}

	// MaxLength validation
	if rules.MaxLength != nil && len(value) > *rules.MaxLength {
		errors = append(errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("length %d exceeds maximum %d", len(value), *rules.MaxLength),
			Code:    "MAX_LENGTH",
		})
	}

	// Pattern validation
	if rules.Pattern != "" {
		matched, err := regexp.MatchString(rules.Pattern, value)
		if err != nil {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("invalid regex pattern: %v", err),
				Code:    "INVALID_PATTERN",
			})
		} else if !matched {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("value does not match pattern '%s'", rules.Pattern),
				Code:    "PATTERN_MISMATCH",
			})
		}
	}

	// Format validation
	if rules.Format != "" {
		if validator, exists := v.formatValidators[rules.Format]; exists {
			if !validator(value) {
				errors = append(errors, ValidationError{
					Path:    path,
					Message: fmt.Sprintf("value does not match format '%s'", rules.Format),
					Code:    "FORMAT_MISMATCH",
				})
			}
		} else {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("unknown format validator: %s", rules.Format),
				Code:    "UNKNOWN_FORMAT",
			})
		}
	}

	// Enum validation
	if len(rules.Enum) > 0 {
		found := false
		for _, allowed := range rules.Enum {
			if value == allowed {
				found = true
				break
			}
		}
		if !found {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("value '%s' not in allowed values %v", value, rules.Enum),
				Code:    "ENUM_MISMATCH",
			})
		}
	}

	return errors
}

// validateNumber validates number-specific rules
func (v *Validator) validateNumber(value float64, rules *ValidationRules, path string) []ValidationError {
	var errors []ValidationError

	if rules == nil {
		return errors
	}

	// Minimum validation
	if rules.Minimum != nil && value < *rules.Minimum {
		errors = append(errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("value %f is less than minimum %f", value, *rules.Minimum),
			Code:    "MIN_VALUE",
		})
	}

	// Maximum validation
	if rules.Maximum != nil && value > *rules.Maximum {
		errors = append(errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("value %f exceeds maximum %f", value, *rules.Maximum),
			Code:    "MAX_VALUE",
		})
	}

	return errors
}

// validateByte validates byte-specific rules (expects base64-encoded string)
func (v *Validator) validateByte(value string, rules *ValidationRules, path string) []ValidationError {
	var errors []ValidationError

	if rules == nil {
		return errors
	}

	// Decode base64 to get actual byte length
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		// Try URL encoding if standard fails
		decoded, err = base64.URLEncoding.DecodeString(value)
		if err != nil {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("invalid base64 encoding: %v", err),
				Code:    "INVALID_BASE64",
			})
			return errors
		}
	}

	byteLength := len(decoded)

	// MinLength validation (on decoded bytes)
	if rules.MinLength != nil && byteLength < *rules.MinLength {
		errors = append(errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("byte length %d is less than minimum %d", byteLength, *rules.MinLength),
			Code:    "MIN_LENGTH",
		})
	}

	// MaxLength validation (on decoded bytes)
	if rules.MaxLength != nil && byteLength > *rules.MaxLength {
		errors = append(errors, ValidationError{
			Path:    path,
			Message: fmt.Sprintf("byte length %d exceeds maximum %d", byteLength, *rules.MaxLength),
			Code:    "MAX_LENGTH",
		})
	}

	return errors
}

// validateArray validates array-specific rules
func (v *Validator) validateArray(arr []interface{}, prop *Property, path string) []ValidationError {
	var errors []ValidationError

	// Array-level validation rules
	if prop.Validation != nil {
		// MinItems validation
		if prop.Validation.MinItems != nil && len(arr) < *prop.Validation.MinItems {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("array length %d is less than minimum %d", len(arr), *prop.Validation.MinItems),
				Code:    "MIN_ITEMS",
			})
		}

		// MaxItems validation
		if prop.Validation.MaxItems != nil && len(arr) > *prop.Validation.MaxItems {
			errors = append(errors, ValidationError{
				Path:    path,
				Message: fmt.Sprintf("array length %d exceeds maximum %d", len(arr), *prop.Validation.MaxItems),
				Code:    "MAX_ITEMS",
			})
		}

		// UniqueItems validation
		if prop.Validation.UniqueItems {
			seen := make(map[string]bool)
			for i, item := range arr {
				// Convert item to string for comparison
				key := fmt.Sprintf("%v", item)
				if seen[key] {
					errors = append(errors, ValidationError{
						Path:    fmt.Sprintf("%s[%d]", path, i),
						Message: "duplicate item found",
						Code:    "DUPLICATE_ITEM",
					})
					break
				}
				seen[key] = true
			}
		}
	}

	// Validate each item in the array
	if prop.Items != nil {
		for i, item := range arr {
			itemPath := fmt.Sprintf("%s[%d]", path, i)
			errors = append(errors, v.validateValue(item, prop.Items, itemPath)...)
		}
	}

	return errors
}

// validateObject validates object properties
func (v *Validator) validateObject(obj map[string]interface{}, prop *Property, path string) []ValidationError {
	var errors []ValidationError

	if prop.Properties == nil {
		return errors
	}

	// Validate each property defined in schema
	for propName, propDef := range prop.Properties {
		value, exists := obj[propName]
		propPath := fmt.Sprintf("%s.%s", path, propName)

		// Check required fields
		if !exists && propDef.Required {
			errors = append(errors, ValidationError{
				Path:    propPath,
				Message: "required field missing",
				Code:    "REQUIRED",
			})
			continue
		}

		// Validate existing fields
		if exists {
			errors = append(errors, v.validateValue(value, propDef, propPath)...)
		}
	}

	return errors
}
