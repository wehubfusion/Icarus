package strings

import (
	"bytes"
	"encoding/base64"
	"net/url"
	"regexp"
	stdstrings "strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Concatenate combines multiple strings with an optional separator.
func Concatenate(separator string, parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	if separator == "" {
		return stdstrings.Join(parts, "")
	}
	return stdstrings.Join(parts, separator)
}

// Split splits a string by the given delimiter.
func Split(s, delimiter string) []string {
	if delimiter == "" {
		return []string{s}
	}
	return stdstrings.Split(s, delimiter)
}

// Join joins an array of strings using the specified separator.
func Join(items []string, separator string) string {
	return stdstrings.Join(items, separator)
}

// Trim removes whitespace or provided cutset from both ends.
// If cutset is empty, it trims unicode whitespace.
func Trim(s, cutset string) string {
	if cutset == "" {
		return stdstrings.TrimSpace(s)
	}
	return stdstrings.Trim(s, cutset)
}

// Replace replaces occurrences of old with new. If useRegex is true, old is treated as a regex pattern.
// If count < 0, all occurrences are replaced.
func Replace(s, old, new string, count int, useRegex bool) (string, error) {
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

// Substring returns the substring between start and end rune indices [start, end).
// Negative indices are treated from the end. Out-of-bounds are clamped.
func Substring(s string, start, end int) string {
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

// ToUpper converts all characters to upper case.
func ToUpper(s string) string { return stdstrings.ToUpper(s) }

// ToLower converts all characters to lower case.
func ToLower(s string) string { return stdstrings.ToLower(s) }

// TitleCase capitalizes the first letter of each word using Unicode-aware rules.
func TitleCase(s string) string {
	return cases.Title(language.Und).String(s)
}

// Capitalize capitalizes the first letter of the string (rune-safe), leaves others unchanged.
func Capitalize(s string) string {
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	return stdstrings.ToUpper(string(r)) + s[size:]
}

// Contains checks if substring or regex pattern exists in the string.
func Contains(s, sub string, useRegex bool) (bool, error) {
	if !useRegex {
		return stdstrings.Contains(s, sub), nil
	}
	re, err := compileRegex(sub)
	if err != nil {
		return false, err
	}
	return re.MatchString(s), nil
}

// Length returns the number of runes (user-perceived characters) in the string.
func Length(s string) int { return utf8.RuneCountInString(s) }

// RegexExtract returns all matches for the given regex pattern.
func RegexExtract(s, pattern string) ([][]string, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, err
	}
	return re.FindAllStringSubmatch(s, -1), nil
}

// Format replaces placeholders like {name} with values from data map.
// Example: Format("Hello {name}", map[string]string{"name":"Alice"}) => "Hello Alice"
func Format(template string, data map[string]string) string {
	result := template
	for k, v := range data {
		// Support {key} and ${key}
		result = stdstrings.ReplaceAll(result, "{"+k+"}", v)
		result = stdstrings.ReplaceAll(result, "${"+k+"}", v)
	}
	return result
}

// Base64Encode encodes input to base64.
func Base64Encode(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// Base64Decode decodes base64 input. Returns decoded string and error, if any.
func Base64Decode(s string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// URIEncode percent-encodes a string for safe inclusion in URLs.
func URIEncode(s string) string { return url.QueryEscape(s) }

// URIDecode decodes percent-encoded strings.
func URIDecode(s string) (string, error) { return url.QueryUnescape(s) }

// Normalize removes diacritics and returns NFC normalized string for comparison.
func Normalize(s string) string { return removeDiacritics(s) }

// Helpers

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
