package dateformatter

import "fmt"

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("config error [%s]: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("config error: %s", e.Message)
}

// NewConfigError creates a new configuration error
func NewConfigError(field, message string) *ConfigError {
	return &ConfigError{
		Field:   field,
		Message: message,
	}
}

// ParseError represents a date parsing error
type ParseError struct {
	Input   string
	Format  string
	Message string
	Err     error
}

func (e *ParseError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("parse error: failed to parse '%s' with format '%s': %s (%v)",
			e.Input, e.Format, e.Message, e.Err)
	}
	return fmt.Sprintf("parse error: failed to parse '%s' with format '%s': %s",
		e.Input, e.Format, e.Message)
}

func (e *ParseError) Unwrap() error {
	return e.Err
}

// NewParseError creates a new parse error
func NewParseError(input, format, message string, err error) *ParseError {
	return &ParseError{
		Input:   input,
		Format:  format,
		Message: message,
		Err:     err,
	}
}

// TimezoneError represents a timezone-related error
type TimezoneError struct {
	Timezone string
	Message  string
	Err      error
}

func (e *TimezoneError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("timezone error [%s]: %s (%v)", e.Timezone, e.Message, e.Err)
	}
	return fmt.Sprintf("timezone error [%s]: %s", e.Timezone, e.Message)
}

func (e *TimezoneError) Unwrap() error {
	return e.Err
}

// NewTimezoneError creates a new timezone error
func NewTimezoneError(timezone, message string, err error) *TimezoneError {
	return &TimezoneError{
		Timezone: timezone,
		Message:  message,
		Err:      err,
	}
}

// FormatError represents an invalid format error
type FormatError struct {
	Format  string
	Message string
}

func (e *FormatError) Error() string {
	return fmt.Sprintf("format error [%s]: %s", e.Format, e.Message)
}

// NewFormatError creates a new format error
func NewFormatError(format, message string) *FormatError {
	return &FormatError{
		Format:  format,
		Message: message,
	}
}

// InputError represents an invalid input error
type InputError struct {
	Field   string
	Message string
}

func (e *InputError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("input error [%s]: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("input error: %s", e.Message)
}

// NewInputError creates a new input error
func NewInputError(field, message string) *InputError {
	return &InputError{
		Field:   field,
		Message: message,
	}
}
