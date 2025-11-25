package schema

import (
	"regexp"
	"strings"
)

// FormatValidator is a function that validates a string format
type FormatValidator func(value string) bool

// validateEmail validates email format (RFC 5322 basic validation)
func validateEmail(email string) bool {
	if email == "" {
		return false
	}
	pattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	matched, err := regexp.MatchString(pattern, email)
	return err == nil && matched
}

// validateURI validates URI format (basic HTTP/HTTPS/FTP check)
func validateURI(uri string) bool {
	if uri == "" {
		return false
	}
	return strings.HasPrefix(uri, "http://") ||
		strings.HasPrefix(uri, "https://") ||
		strings.HasPrefix(uri, "ftp://") ||
		strings.HasPrefix(uri, "ws://") ||
		strings.HasPrefix(uri, "wss://")
}

// validateUUID validates UUID format (accepts v1-v5)
func validateUUID(uuid string) bool {
	if uuid == "" {
		return false
	}
	// Accept any UUID version (v1-v5), not just v4
	pattern := `^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
	matched, err := regexp.MatchString(pattern, strings.ToLower(uuid))
	return err == nil && matched
}

// validateDate validates ISO 8601 date format (YYYY-MM-DD)
func validateDate(date string) bool {
	if date == "" {
		return false
	}
	pattern := `^\d{4}-\d{2}-\d{2}$`
	matched, err := regexp.MatchString(pattern, date)
	return err == nil && matched
}

// validateDateTime validates ISO 8601 datetime format
func validateDateTime(datetime string) bool {
	if datetime == "" {
		return false
	}
	// Matches: 2025-01-09T10:30:00Z or 2025-01-09T10:30:00+00:00
	pattern := `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`
	matched, err := regexp.MatchString(pattern, datetime)
	return err == nil && matched
}

// GetFormatValidator returns a format validator by name
func GetFormatValidator(format string) (FormatValidator, bool) {
	validators := map[string]FormatValidator{
		"email":    validateEmail,
		"uri":      validateURI,
		"uuid":     validateUUID,
		"date":     validateDate,
		"datetime": validateDateTime,
	}

	validator, exists := validators[format]
	return validator, exists
}
