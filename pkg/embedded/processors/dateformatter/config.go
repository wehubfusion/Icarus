package dateformatter

import (
	"encoding/json"
	"fmt"
)

// Config defines the configuration for date formatter operations
type Config struct {
	// Operation specifies which date operation to perform
	// Currently supported: "format"
	Operation string `json:"operation"`

	// Params contains operation-specific parameters
	Params json.RawMessage `json:"params"`
}

// FormatParams defines parameters for the format operation
type FormatParams struct {
	// InFormat specifies the input date format (e.g., "RFC3339", "DateTime", "DateOnly")
	InFormat string `json:"in_format"`

	// InTimezone specifies the input timezone (optional, e.g., "UTC", "America/New_York")
	// If not provided, the timezone in the input string (if any) will be used,
	// or the time will be treated as local time
	InTimezone string `json:"in_timezone,omitempty"`

	// OutFormat specifies the output date format (e.g., "RFC3339", "DateTime", "DateOnly")
	OutFormat string `json:"out_format"`

	// OutTimezone specifies the output timezone (optional, e.g., "UTC", "America/New_York")
	// If not provided, uses the same timezone as the input
	OutTimezone string `json:"out_timezone,omitempty"`

	// DateStyle specifies the date formatting style for DateOnly and DateTime formats
	// Options: "YYYY_MM_DD", "DD_MM_YYYY", "MM_DD_YYYY" (with dash or slash variants)
	DateStyle string `json:"date_style,omitempty"`

	// TimeStyle specifies the time formatting style for TimeOnly and DateTime formats
	// Options: "24_HOUR", "12_HOUR", "24_HOUR_HM", "12_HOUR_HM"
	TimeStyle string `json:"time_style,omitempty"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Operation == "" {
		return NewConfigError("operation", "operation cannot be empty")
	}

	if c.Operation != "format" {
		return NewConfigError("operation",
			fmt.Sprintf("invalid operation '%s', only 'format' is supported", c.Operation))
	}

	// Validate operation-specific params
	var params FormatParams
	if err := json.Unmarshal(c.Params, &params); err != nil {
		return NewConfigError("params", fmt.Sprintf("invalid format params: %v", err))
	}

	return params.Validate()
}

// Validate checks if the format parameters are valid
func (p *FormatParams) Validate() error {
	// Validate input format
	if p.InFormat == "" {
		return NewConfigError("params.in_format", "input format cannot be empty")
	}

	if !IsValidTimeFormat(p.InFormat) {
		return NewConfigError("params.in_format",
			fmt.Sprintf("invalid input format '%s'", p.InFormat))
	}

	// Validate output format
	if p.OutFormat == "" {
		return NewConfigError("params.out_format", "output format cannot be empty")
	}

	if !IsValidTimeFormat(p.OutFormat) {
		return NewConfigError("params.out_format",
			fmt.Sprintf("invalid output format '%s'", p.OutFormat))
	}

	// Validate date style if provided
	if p.DateStyle != "" {
		if !IsValidDateStyle(p.DateStyle) {
			return NewConfigError("params.date_style",
				fmt.Sprintf("invalid date style '%s'", p.DateStyle))
		}

		// Check if date style is applicable to the output format
		outFormat := TimeFormat(p.OutFormat)
		if outFormat != FormatDateOnly && outFormat != FormatDateTime {
			return NewConfigError("params.date_style",
				"date_style can only be used with DateOnly or DateTime output formats")
		}
	}

	// Validate time style if provided
	if p.TimeStyle != "" {
		if !IsValidTimeStyle(p.TimeStyle) {
			return NewConfigError("params.time_style",
				fmt.Sprintf("invalid time style '%s'", p.TimeStyle))
		}

		// Check if time style is applicable to the output format
		outFormat := TimeFormat(p.OutFormat)
		if outFormat != FormatTimeOnly && outFormat != FormatDateTime {
			return NewConfigError("params.time_style",
				"time_style can only be used with TimeOnly or DateTime output formats")
		}
	}

	return nil
}

// GetInputLayout returns the layout string for parsing input dates
func (p *FormatParams) GetInputLayout() string {
	return GetTimeFormatLayout(TimeFormat(p.InFormat))
}

// GetOutputLayout returns the layout string for formatting output dates
// This takes into account date_style and time_style overrides
func (p *FormatParams) GetOutputLayout() string {
	outFormat := TimeFormat(p.OutFormat)
	baseLayout := GetTimeFormatLayout(outFormat)

	// Handle special formats with style overrides
	switch outFormat {
	case FormatDateOnly:
		if p.DateStyle != "" {
			return GetDateStyleLayout(DateStyle(p.DateStyle))
		}

	case FormatTimeOnly:
		if p.TimeStyle != "" {
			return GetTimeStyleLayout(TimeStyle(p.TimeStyle))
		}

	case FormatDateTime:
		// Combine date style and time style if provided
		dateLayout := baseLayout[:10] // Default: "2006-01-02"
		timeLayout := baseLayout[11:] // Default: "15:04:05"

		if p.DateStyle != "" {
			dateLayout = GetDateStyleLayout(DateStyle(p.DateStyle))
		}
		if p.TimeStyle != "" {
			timeLayout = GetTimeStyleLayout(TimeStyle(p.TimeStyle))
		}

		return dateLayout + " " + timeLayout
	}

	return baseLayout
}
