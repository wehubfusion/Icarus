package pathutil

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
)

// StandardOutput represents the standardized output structure for all nodes
// This is a local copy to avoid circular dependency with embedded package
// Must match the structure in embedded.StandardOutput
type StandardOutput struct {
	Meta   interface{} `json:"_meta"`
	Events interface{} `json:"_events"`
	Error  interface{} `json:"_error,omitempty"`
	Result interface{} `json:"result"`
}

// NavigatePath extracts a value from StandardOutput struct with namespace awareness
// For field mappings: Extracts result field first, then navigates paths in extracted result
// For event triggers: Uses full StandardOutput to access _events/ namespace
// Supports:
// - Reserved namespaces: _meta/, _events/, _error/ (uses full StandardOutput)
// - Field paths: /result, /name, /field, etc. (extracts result first, then navigates as-is)
// - Array iteration: //field (extracts from each array element)
// Note: Converts slash notation to dot notation for gjson compatibility
func NavigatePath(output *StandardOutput, path string) (interface{}, bool) {
	if output == nil {
		return nil, false
	}

	// Normalize path for checking
	pathNormalized := strings.TrimPrefix(path, "/")

	// Check if accessing reserved namespace - use full StandardOutput
	if strings.HasPrefix(pathNormalized, "_meta") ||
		strings.HasPrefix(pathNormalized, "_events") ||
		strings.HasPrefix(pathNormalized, "_error") {
		// Marshal StandardOutput to JSON for gjson navigation of reserved namespaces
		outputBytes, err := json.Marshal(output)
		if err != nil {
			return nil, false
		}
		gjsonPath := strings.ReplaceAll(pathNormalized, "/", ".")
		result := gjson.GetBytes(outputBytes, gjsonPath)
		return result.Value(), result.Exists()
	}

	// Extract result from StandardOutput FIRST
	if output.Result == nil {
		// Result is null (e.g., error case)
		return nil, false
	}

	// Marshal result field for gjson navigation
	resultBytes, err := json.Marshal(output.Result)
	if err != nil {
		return nil, false
	}

	// Navigate paths as-is in extracted result
	// No special handling - all paths navigate directly into the extracted result
	adjustedPath := path
	if path == "" || path == "/" {
		// Empty path -> return entire result
		adjustedPath = "/"
	}
	// All paths like "/result", "/field", "field" navigate directly into extracted result

	// Navigate in extracted result
	// For paths starting with //, don't normalize the leading slash
	adjustedPathNormalized := adjustedPath
	if !strings.HasPrefix(adjustedPath, "//") {
		adjustedPathNormalized = strings.TrimPrefix(adjustedPath, "/")
	} else {
		// Path starts with //, keep it as-is for array iteration handling
		adjustedPathNormalized = adjustedPath
	}

	if adjustedPathNormalized == "" || adjustedPathNormalized == "/" {
		// Return entire extracted result
		return output.Result, true
	}

	// Handle // notation for array iteration
	if strings.HasPrefix(adjustedPathNormalized, "//") || strings.Contains(adjustedPathNormalized, "//") {
		// Split on // to handle array paths
		parts := strings.SplitN(adjustedPathNormalized, "//", 2)
		if len(parts) == 2 {
			collectionPath := parts[0] // Everything before //
			fieldPath := parts[1]      // Everything after //

			// If collection path is empty or just "/", check if root is an array
			var collection gjson.Result
			if collectionPath == "" || collectionPath == "/" {
				// Root level - check if resultBytes itself is an array
				parsed := gjson.ParseBytes(resultBytes)
				if parsed.IsArray() {
					collection = parsed
				} else {
					return nil, false
				}
			} else {
				// Navigate to collection
				collectionPathClean := strings.TrimPrefix(collectionPath, "/")
				collectionGjsonPath := strings.ReplaceAll(collectionPathClean, "/", ".")
				collection = gjson.GetBytes(resultBytes, collectionGjsonPath)
			}

			if !collection.Exists() {
				return nil, false
			}

			// If collection is array, extract field from each item
			if collection.IsArray() {
				items := collection.Array()
				result := make([]interface{}, 0, len(items))
				fieldPathClean := strings.TrimPrefix(fieldPath, "/")
				fieldGjsonPath := strings.ReplaceAll(fieldPathClean, "/", ".")

				for _, item := range items {
					// Extract field from this item
					fieldValue := gjson.Get(item.Raw, fieldGjsonPath)
					if fieldValue.Exists() {
						result = append(result, fieldValue.Value())
					}
				}
				return result, len(result) > 0
			}

			// Not an array - navigate normally
			if collectionPath != "" && collectionPath != "/" {
				collectionPathClean := strings.TrimPrefix(collectionPath, "/")
				fieldPathClean := strings.TrimPrefix(fieldPath, "/")
				fullPath := collectionPathClean + "/" + fieldPathClean
				fullGjsonPath := strings.ReplaceAll(fullPath, "/", ".")
				result := gjson.GetBytes(resultBytes, fullGjsonPath)
				return result.Value(), result.Exists()
			}
			return nil, false
		}
	}

	// Regular path navigation in extracted result
	gjsonPath := strings.ReplaceAll(adjustedPathNormalized, "/", ".")
	result := gjson.GetBytes(resultBytes, gjsonPath)
	return result.Value(), result.Exists()
}

// NavigatePathString is a convenience wrapper that returns the string value
func NavigatePathString(output *StandardOutput, path string) string {
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
func NavigatePathInt(output *StandardOutput, path string) int {
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
func NavigatePathBool(output *StandardOutput, path string) bool {
	value, exists := NavigatePath(output, path)
	if !exists {
		return false
	}
	if b, ok := value.(bool); ok {
		return b
	}
	return false
}
