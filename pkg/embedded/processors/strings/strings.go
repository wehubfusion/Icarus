package strings

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	stdstrings "strings"
	"unicode"
	"unicode/utf8"

	"github.com/wehubfusion/Icarus/pkg/embedded"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Executor implements NodeExecutor for string operations
type Executor struct{}

// StringsConfig defines the configuration for string operations
type StringsConfig struct {
	Operation string                 `json:"operation"` // Operation name (e.g., "concatenate", "split", "trim", etc.)
	Params    map[string]interface{} `json:"params"`    // Operation-specific parameters
}

// NewExecutor creates a new strings executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a string operation
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var strConfig StringsConfig
	if err := json.Unmarshal(config.Configuration, &strConfig); err != nil {
		return nil, fmt.Errorf("failed to parse strings configuration: %w", err)
	}

	// Parse input
	var input map[string]interface{}
	if err := json.Unmarshal(config.Input, &input); err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}

	// Execute operation
	result, err := e.executeOperation(strConfig.Operation, strConfig.Params, input)
	if err != nil {
		return nil, err
	}

	// Marshal result
	output, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return output, nil
}

// executeOperation executes the specified string operation
func (e *Executor) executeOperation(operation string, params map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	switch operation {
	case "concatenate":
		separator := getString(params, "separator", "")
		parts := getStringSlice(params, "parts", nil)
		if parts == nil {
			// If parts not provided, use input values
			parts = extractStringValues(input)
		}
		result["value"] = concatenate(separator, parts...)

	case "split":
		str := getString(params, "string", getString(input, "string", ""))
		delimiter := getString(params, "delimiter", "")
		result["value"] = split(str, delimiter)

	case "join":
		items := getStringSlice(params, "items", getStringSlice(input, "items", []string{}))
		separator := getString(params, "separator", "")
		result["value"] = join(items, separator)

	case "trim":
		str := getString(params, "string", getString(input, "string", ""))
		cutset := getString(params, "cutset", "")
		result["value"] = trim(str, cutset)

	case "replace":
		str := getString(params, "string", getString(input, "string", ""))
		old := getString(params, "old", "")
		new := getString(params, "new", "")
		count := getInt(params, "count", -1)
		useRegex := getBool(params, "use_regex", false)
		replaced, err := replace(str, old, new, count, useRegex)
		if err != nil {
			return nil, fmt.Errorf("replace operation failed: %w", err)
		}
		result["value"] = replaced

	case "substring":
		str := getString(params, "string", getString(input, "string", ""))
		start := getInt(params, "start", 0)
		end := getInt(params, "end", 0)
		result["value"] = substring(str, start, end)

	case "to_upper":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = toUpper(str)

	case "to_lower":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = toLower(str)

	case "title_case":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = titleCase(str)

	case "capitalize":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = capitalize(str)

	case "contains":
		str := getString(params, "string", getString(input, "string", ""))
		sub := getString(params, "substring", "")
		useRegex := getBool(params, "use_regex", false)
		containsResult, err := contains(str, sub, useRegex)
		if err != nil {
			return nil, fmt.Errorf("contains operation failed: %w", err)
		}
		result["value"] = containsResult

	case "length":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = length(str)

	case "regex_extract":
		str := getString(params, "string", getString(input, "string", ""))
		pattern := getString(params, "pattern", "")
		matches, err := regexExtract(str, pattern)
		if err != nil {
			return nil, fmt.Errorf("regex_extract operation failed: %w", err)
		}
		result["value"] = matches

	case "format":
		template := getString(params, "template", getString(input, "template", ""))
		data := getStringMap(params, "data", getStringMap(input, "data", map[string]string{}))
		result["value"] = format(template, data)

	case "base64_encode":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = base64Encode(str)

	case "base64_decode":
		str := getString(params, "string", getString(input, "string", ""))
		decoded, err := base64Decode(str)
		if err != nil {
			return nil, fmt.Errorf("base64_decode operation failed: %w", err)
		}
		result["value"] = decoded

	case "uri_encode":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = uriEncode(str)

	case "uri_decode":
		str := getString(params, "string", getString(input, "string", ""))
		decoded, err := uriDecode(str)
		if err != nil {
			return nil, fmt.Errorf("uri_decode operation failed: %w", err)
		}
		result["value"] = decoded

	case "normalize":
		str := getString(params, "string", getString(input, "string", ""))
		result["value"] = normalize(str)

	default:
		return nil, fmt.Errorf("unknown string operation: %s", operation)
	}

	return result, nil
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-strings"
}

// String operation functions (inlined from pkg/process/strings)

// concatenate combines multiple strings with an optional separator.
func concatenate(separator string, parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	if separator == "" {
		return stdstrings.Join(parts, "")
	}
	return stdstrings.Join(parts, separator)
}

// split splits a string by the given delimiter.
func split(s, delimiter string) []string {
	if delimiter == "" {
		return []string{s}
	}
	return stdstrings.Split(s, delimiter)
}

// join joins an array of strings using the specified separator.
func join(items []string, separator string) string {
	return stdstrings.Join(items, separator)
}

// trim removes whitespace or provided cutset from both ends.
// If cutset is empty, it trims unicode whitespace.
func trim(s, cutset string) string {
	if cutset == "" {
		return stdstrings.TrimSpace(s)
	}
	return stdstrings.Trim(s, cutset)
}

// replace replaces occurrences of old with new. If useRegex is true, old is treated as a regex pattern.
// If count < 0, all occurrences are replaced.
func replace(s, old, new string, count int, useRegex bool) (string, error) {
	if !useRegex {
		return stdstrings.Replace(s, old, new, count), nil
	}
	// regex path
	re, err := compileRegex(old)
	if err != nil {
		return "", err
	}
	if count < 0 {
		return re.ReplaceAllString(s, new), nil
	}
	return replaceRegexCount(s, re, new, count), nil
}

// substring returns the substring between start and end rune indices [start, end).
// Negative indices are treated from the end. Out-of-bounds are clamped.
func substring(s string, start, end int) string {
	runes := []rune(s)
	n := len(runes)
	if start < 0 {
		start = n + start
	}
	if end <= 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end > n {
		end = n
	}
	if start > end {
		start, end = end, start
	}
	return string(runes[start:end])
}

// toUpper converts all characters to upper case.
func toUpper(s string) string { return stdstrings.ToUpper(s) }

// toLower converts all characters to lower case.
func toLower(s string) string { return stdstrings.ToLower(s) }

// titleCase capitalizes the first letter of each word using Unicode-aware rules.
func titleCase(s string) string {
	return cases.Title(language.Und).String(s)
}

// capitalize capitalizes the first letter of the string (rune-safe), leaves others unchanged.
func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	return stdstrings.ToUpper(string(r)) + s[size:]
}

// contains checks if substring or regex pattern exists in the string.
func contains(s, sub string, useRegex bool) (bool, error) {
	if !useRegex {
		return stdstrings.Contains(s, sub), nil
	}
	re, err := compileRegex(sub)
	if err != nil {
		return false, err
	}
	return re.MatchString(s), nil
}

// length returns the number of runes (user-perceived characters) in the string.
func length(s string) int { return utf8.RuneCountInString(s) }

// regexExtract returns all matches for the given regex pattern.
func regexExtract(s, pattern string) ([][]string, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, err
	}
	return re.FindAllStringSubmatch(s, -1), nil
}

// format replaces placeholders like {name} with values from data map.
// Example: format("Hello {name}", map[string]string{"name":"Alice"}) => "Hello Alice"
func format(template string, data map[string]string) string {
	result := template
	for k, v := range data {
		// Support {key} and ${key}
		result = stdstrings.ReplaceAll(result, "${"+k+"}", v)
		result = stdstrings.ReplaceAll(result, "{"+k+"}", v)
	}
	return result
}

// base64Encode encodes input to base64.
func base64Encode(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// base64Decode decodes base64 input. Returns decoded string and error, if any.
func base64Decode(s string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// uriEncode percent-encodes a string for safe inclusion in URLs.
func uriEncode(s string) string { return url.QueryEscape(s) }

// uriDecode decodes percent-encoded strings.
func uriDecode(s string) (string, error) { return url.QueryUnescape(s) }

// normalize removes diacritics and returns NFC normalized string for comparison.
func normalize(s string) string { return removeDiacritics(s) }

// Helper functions for regex operations

// compileRegex compiles pattern with RE2 syntax and anchors disabled by default.
func compileRegex(pattern string) (*regex, error) {
	return newRegex(pattern)
}

// replaceRegexCount replaces at most count matches using compiled regex.
func replaceRegexCount(s string, re *regex, replacement string, count int) string {
	if count < 0 {
		return re.ReplaceAllString(s, replacement)
	}
	// Manually iterate matches to limit count
	var out bytes.Buffer
	idxs := re.FindAllStringIndex(s, -1)
	if len(idxs) == 0 || count == 0 {
		return s
	}
	last := 0
	for i, pair := range idxs {
		if i >= count {
			break
		}
		start, end := pair[0], pair[1]
		out.WriteString(s[last:start])
		out.WriteString(replacement)
		last = end
	}
	out.WriteString(s[last:])
	return out.String()
}

// Minimal wrapper around Go's regexp to allow swapping if needed.
type regex struct{ r *regexp.Regexp }

func newRegex(p string) (*regex, error) {
	r, err := regexp.Compile(p)
	if err != nil {
		return nil, err
	}
	return &regex{r: r}, nil
}

func (re *regex) ReplaceAllString(s, repl string) string     { return re.r.ReplaceAllString(s, repl) }
func (re *regex) FindAllStringIndex(s string, n int) [][]int { return re.r.FindAllStringIndex(s, n) }
func (re *regex) MatchString(s string) bool                  { return re.r.MatchString(s) }
func (re *regex) FindAllStringSubmatch(s string, n int) [][]string {
	return re.r.FindAllStringSubmatch(s, n)
}

// removeDiacritics attempts to strip diacritics from common Latin characters.
// It also drops combining marks if present.
func removeDiacritics(s string) string {
	var b bytes.Buffer
	for _, r := range s {
		// Drop combining marks
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		switch r {
		case 'À', 'Á', 'Â', 'Ã', 'Ä', 'Å':
			b.WriteByte('A')
		case 'à', 'á', 'â', 'ã', 'ä', 'å':
			b.WriteByte('a')
		case 'Æ':
			b.WriteString("AE")
		case 'æ':
			b.WriteString("ae")
		case 'Ç':
			b.WriteByte('C')
		case 'ç':
			b.WriteByte('c')
		case 'È', 'É', 'Ê', 'Ë':
			b.WriteByte('E')
		case 'è', 'é', 'ê', 'ë':
			b.WriteByte('e')
		case 'Ì', 'Í', 'Î', 'Ï':
			b.WriteByte('I')
		case 'ì', 'í', 'î', 'ï':
			b.WriteByte('i')
		case 'Ñ':
			b.WriteByte('N')
		case 'ñ':
			b.WriteByte('n')
		case 'Ò', 'Ó', 'Ô', 'Õ', 'Ö', 'Ø':
			b.WriteByte('O')
		case 'ò', 'ó', 'ô', 'õ', 'ö', 'ø':
			b.WriteByte('o')
		case 'Ù', 'Ú', 'Û', 'Ü':
			b.WriteByte('U')
		case 'ù', 'ú', 'û', 'ü':
			b.WriteByte('u')
		case 'Ý':
			b.WriteByte('Y')
		case 'ý', 'ÿ':
			b.WriteByte('y')
		case 'ẞ':
			b.WriteString("SS")
		case 'ß':
			b.WriteString("ss")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Helper functions to extract values from config/input maps

func getString(m map[string]interface{}, key string, defaultValue string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultValue
}

func getInt(m map[string]interface{}, key string, defaultValue int) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case float64:
			return int(val)
		case int64:
			return int(val)
		}
	}
	return defaultValue
}

func getBool(m map[string]interface{}, key string, defaultValue bool) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return defaultValue
}

func getStringSlice(m map[string]interface{}, key string, defaultValue []string) []string {
	if v, ok := m[key]; ok {
		if items, ok := v.([]interface{}); ok {
			result := make([]string, 0, len(items))
			for _, item := range items {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		}
		if items, ok := v.([]string); ok {
			return items
		}
	}
	return defaultValue
}

func getStringMap(m map[string]interface{}, key string, defaultValue map[string]string) map[string]string {
	if v, ok := m[key]; ok {
		if dataMap, ok := v.(map[string]interface{}); ok {
			result := make(map[string]string)
			for k, val := range dataMap {
				if s, ok := val.(string); ok {
					result[k] = s
				}
			}
			return result
		}
		if dataMap, ok := v.(map[string]string); ok {
			return dataMap
		}
	}
	return defaultValue
}

func extractStringValues(m map[string]interface{}) []string {
	result := make([]string, 0)
	for _, v := range m {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}
