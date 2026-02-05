package dateformatter

import "fmt"

// Config defines configuration for the date formatter (format operation).
// Config keys match seed-node-schemas.sql Date Formatter: label, in_format, out_format.
type Config struct {
	InFormat    string `json:"in_format"`
	OutFormat   string `json:"out_format"`
	InTimezone  string `json:"in_timezone,omitempty"`
	OutTimezone string `json:"out_timezone,omitempty"`
	DateStyle   string `json:"date_style,omitempty"`
	TimeStyle   string `json:"time_style,omitempty"`
}

// Validate checks if the configuration is valid.
func (c *Config) Validate(nodeID string) error {
	if c.InFormat == "" {
		return NewConfigError(nodeID, "in_format", "input format cannot be empty")
	}
	if !IsValidTimeFormat(c.InFormat) {
		return NewConfigError(nodeID, "in_format", fmt.Sprintf("invalid input format '%s'", c.InFormat))
	}

	if c.OutFormat == "" {
		return NewConfigError(nodeID, "out_format", "output format cannot be empty")
	}
	if !IsValidTimeFormat(c.OutFormat) {
		return NewConfigError(nodeID, "out_format", fmt.Sprintf("invalid output format '%s'", c.OutFormat))
	}

	// Validate date style if provided (only for DateOnly/DateTime outputs).
	if c.DateStyle != "" {
		if !IsValidDateStyle(c.DateStyle) {
			return NewConfigError(nodeID, "date_style", fmt.Sprintf("invalid date style '%s'", c.DateStyle))
		}
		outFormat := TimeFormat(c.OutFormat)
		if outFormat != FormatDateOnly && outFormat != FormatDateTime {
			return NewConfigError(nodeID, "date_style", "date_style can only be used with DateOnly or DateTime output formats")
		}
	}

	// Validate time style if provided (only for TimeOnly/DateTime outputs).
	if c.TimeStyle != "" {
		if !IsValidTimeStyle(c.TimeStyle) {
			return NewConfigError(nodeID, "time_style", fmt.Sprintf("invalid time style '%s'", c.TimeStyle))
		}
		outFormat := TimeFormat(c.OutFormat)
		if outFormat != FormatTimeOnly && outFormat != FormatDateTime {
			return NewConfigError(nodeID, "time_style", "time_style can only be used with TimeOnly or DateTime output formats")
		}
	}

	return nil
}

// GetInputLayout returns the layout string for parsing input dates.
func (c *Config) GetInputLayout() string { return GetTimeFormatLayout(TimeFormat(c.InFormat)) }

// GetOutputLayout returns the layout string for formatting output dates (with style overrides).
func (c *Config) GetOutputLayout() string {
	outFormat := TimeFormat(c.OutFormat)
	baseLayout := GetTimeFormatLayout(outFormat)

	switch outFormat {
	case FormatDateOnly:
		if c.DateStyle != "" {
			return GetDateStyleLayout(DateStyle(c.DateStyle))
		}
	case FormatTimeOnly:
		if c.TimeStyle != "" {
			return GetTimeStyleLayout(TimeStyle(c.TimeStyle))
		}
	case FormatDateTime:
		dateLayout := baseLayout[:10] // "2006-01-02"
		timeLayout := baseLayout[11:] // "15:04:05"
		if c.DateStyle != "" {
			dateLayout = GetDateStyleLayout(DateStyle(c.DateStyle))
		}
		if c.TimeStyle != "" {
			timeLayout = GetTimeStyleLayout(TimeStyle(c.TimeStyle))
		}
		return dateLayout + " " + timeLayout
	}
	return baseLayout
}
