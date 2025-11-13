package pathutil

import (
	"strings"

	"github.com/tidwall/gjson"
)

// NavigatePath extracts a value from StandardOutput JSON with namespace awareness
// Supports:
// - Reserved namespaces: _meta/, _events/, _error/
// - Data namespace: Regular fields auto-resolved to data/
// - Direct path: data/field (explicit)
// Note: Converts slash notation to dot notation for gjson compatibility
func NavigatePath(output []byte, path string) (interface{}, bool) {
	if len(output) == 0 {
		return nil, false
	}

	// Normalize path
	path = strings.TrimPrefix(path, "/")

	if path == "" {
		// Return entire document
		result := gjson.ParseBytes(output)
		return result.Value(), result.Exists()
	}

	// Convert path from slash notation to dot notation for gjson
	gjsonPath := strings.ReplaceAll(path, "/", ".")

	// Check if accessing reserved namespace
	if strings.HasPrefix(path, "_meta") ||
		strings.HasPrefix(path, "_events") ||
		strings.HasPrefix(path, "_error") {
		// Direct access to reserved namespace
		result := gjson.GetBytes(output, gjsonPath)
		return result.Value(), result.Exists()
	}

	// Check if path explicitly starts with "data/"
	if strings.HasPrefix(path, "data/") || strings.HasPrefix(path, "data.") {
		// Direct access to data namespace
		result := gjson.GetBytes(output, gjsonPath)
		return result.Value(), result.Exists()
	}

	// Regular field access - auto-resolve to data namespace
	dataPath := "data." + gjsonPath
	result := gjson.GetBytes(output, dataPath)
	return result.Value(), result.Exists()
}

// NavigatePathString is a convenience wrapper that returns the string value
func NavigatePathString(output []byte, path string) string {
	value, exists := NavigatePath(output, path)
	if !exists {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

// NavigatePathInt is a convenience wrapper that returns the int value
func NavigatePathInt(output []byte, path string) int {
	value, exists := NavigatePath(output, path)
	if !exists {
		return 0
	}
	if num, ok := value.(float64); ok {
		return int(num)
	}
	return 0
}

// NavigatePathBool is a convenience wrapper that returns the bool value
func NavigatePathBool(output []byte, path string) bool {
	value, exists := NavigatePath(output, path)
	if !exists {
		return false
	}
	if b, ok := value.(bool); ok {
		return b
	}
	return false
}
