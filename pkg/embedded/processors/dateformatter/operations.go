package dateformatter

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// executeFormat performs date format conversion with optional timezone transformation
func executeFormat(input []byte, params FormatParams) ([]byte, error) {
	// Parse input JSON to extract date string
	var inputData map[string]interface{}
	if err := json.Unmarshal(input, &inputData); err != nil {
		return nil, NewInputError("", fmt.Sprintf("failed to parse input JSON: %v", err))
	}

	// Extract date string from "data" field
	dateStr, ok := inputData["data"].(string)
	if !ok {
		return nil, NewInputError("data", "input must contain a 'data' field with a string value")
	}

	if dateStr == "" {
		return nil, NewInputError("data", "date string cannot be empty")
	}

	// Normalize input date string (handle partial DateTime inputs)
	normalizedDateStr := normalizeInputDate(dateStr, TimeFormat(params.InFormat))

	// Get input and output layouts
	inputLayout := params.GetInputLayout()
	outputLayout := params.GetOutputLayout()

	// Parse the date with timezone handling
	parsedTime, err := parseDateTime(normalizedDateStr, inputLayout, params.InTimezone)
	if err != nil {
		return nil, err
	}

	// Convert to output timezone if specified
	if params.OutTimezone != "" {
		location, err := time.LoadLocation(params.OutTimezone)
		if err != nil {
			return nil, NewTimezoneError(params.OutTimezone, "invalid output timezone", err)
		}
		parsedTime = parsedTime.In(location)
	}

	// Format the date using output layout
	formattedDate := parsedTime.Format(outputLayout)

	// Build output JSON
	output := map[string]interface{}{
		"data": formattedDate,
	}

	result, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return result, nil
}

// parseDateTime parses a date string with timezone support
func parseDateTime(dateStr, layout, timezone string) (time.Time, error) {
	var parsedTime time.Time
	var err error

	if timezone != "" {
		// Parse with specific timezone
		location, locErr := time.LoadLocation(timezone)
		if locErr != nil {
			return time.Time{}, NewTimezoneError(timezone, "invalid input timezone", locErr)
		}
		parsedTime, err = time.ParseInLocation(layout, dateStr, location)
	} else {
		// Parse without specific timezone (use timezone from string or local)
		parsedTime, err = time.Parse(layout, dateStr)
	}

	if err != nil {
		return time.Time{}, NewParseError(dateStr, layout, "invalid date format", err)
	}

	return parsedTime, nil
}

// normalizeInputDate handles partial DateTime inputs and format-specific normalization
func normalizeInputDate(dateStr string, format TimeFormat) string {
	// Handle partial DateTime inputs (add missing time components)
	if format == FormatDateTime {
		// "2006-01-02" → "2006-01-02 00:00:00"
		if len(dateStr) == 10 && strings.Count(dateStr, "-") == 2 {
			return dateStr + " 00:00:00"
		}
		// "2006-01-02 15:04" → "2006-01-02 15:04:00"
		if len(dateStr) == 16 && strings.Count(dateStr, ":") == 1 {
			return dateStr + ":00"
		}
	}

	// Handle DateOnly format normalization
	if format == FormatDateOnly {
		// "20060102" (YYYYMMDD) → "2006-01-02"
		if len(dateStr) == 8 && !strings.Contains(dateStr, "-") && !strings.Contains(dateStr, "/") {
			return dateStr[:4] + "-" + dateStr[4:6] + "-" + dateStr[6:8]
		}
	}

	// Return unchanged if no normalization needed
	return dateStr
}

// Additional helper functions for common operations

// FormatDateWithTimezone is a convenience function for format operations
func FormatDateWithTimezone(dateStr, inFormat, inTZ, outFormat, outTZ string) (string, error) {
	params := FormatParams{
		InFormat:    inFormat,
		InTimezone:  inTZ,
		OutFormat:   outFormat,
		OutTimezone: outTZ,
	}

	if err := params.Validate(); err != nil {
		return "", err
	}

	input := map[string]interface{}{
		"data": dateStr,
	}

	inputJSON, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("failed to marshal input: %w", err)
	}

	output, err := executeFormat(inputJSON, params)
	if err != nil {
		return "", err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		return "", fmt.Errorf("failed to unmarshal output: %w", err)
	}

	formattedDate, ok := result["data"].(string)
	if !ok {
		return "", fmt.Errorf("unexpected output format")
	}

	return formattedDate, nil
}

// ValidateTimezone checks if a timezone string is valid
func ValidateTimezone(tz string) error {
	if tz == "" {
		return nil // Empty timezone is valid
	}

	_, err := time.LoadLocation(tz)
	if err != nil {
		return NewTimezoneError(tz, "invalid timezone", err)
	}

	return nil
}
