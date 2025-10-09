package tests

import (
	"reflect"
	"testing"

	strutil "github.com/wehubfusion/Icarus/pkg/process/strings"
)

func TestConcatenate(t *testing.T) {
	tests := []struct {
		name  string
		sep   string
		parts []string
		want  string
	}{
		{"no parts", ",", nil, ""},
		{"empty sep", "", []string{"a", "b", "c"}, "abc"},
		{"with sep", "-", []string{"a", "b", "c"}, "a-b-c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := strutil.Concatenate(tt.sep, tt.parts...); got != tt.want {
				t.Errorf("Concatenate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		delim string
		want  []string
	}{
		{"empty delimiter returns whole string", "a,b,c", "", []string{"a,b,c"}},
		{"comma split", "a,b,c", ",", []string{"a", "b", "c"}},
		{"no match", "abc", ",", []string{"abc"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := strutil.Split(tt.s, tt.delim); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Split() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestJoin(t *testing.T) {
	if got := strutil.Join([]string{"a", "b", "c"}, "-"); got != "a-b-c" {
		t.Errorf("Join() = %v, want %v", got, "a-b-c")
	}
}

func TestTrim(t *testing.T) {
	tests := []struct {
		name string
		s    string
		cut  string
		want string
	}{
		{"trim space default", "  hello  ", "", "hello"},
		{"custom cutset", "--hello--", "-", "hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := strutil.Trim(tt.s, tt.cut); got != tt.want {
				t.Errorf("Trim() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplacePlain(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		old   string
		new   string
		count int
		want  string
	}{
		{"replace all count<0", "a a a", "a", "b", -1, "b b b"},
		{"replace first 1", "a a a", "a", "b", 1, "b a a"},
		{"replace none", "abc", "x", "y", -1, "abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := strutil.Replace(tt.s, tt.old, tt.new, tt.count, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Replace() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplaceRegex(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		pat   string
		repl  string
		count int
		want  string
	}{
		{"replace digits all", "a1b2c3", "[0-9]", "_", -1, "a_b_c_"},
		{"replace first two", "a1b2c3", "[0-9]", "_", 2, "a_b_c3"},
		{"no match", "abc", "[0-9]", "_", -1, "abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := strutil.Replace(tt.s, tt.pat, tt.repl, tt.count, true)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Replace() = %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("invalid regex returns error", func(t *testing.T) {
		_, err := strutil.Replace("abc", "[", "_", -1, true)
		if err == nil {
			t.Fatalf("expected error for invalid regex, got nil")
		}
	})
}

func TestSubstring(t *testing.T) {
	s := "héllo" // includes multi-byte
	tests := []struct {
		name       string
		start, end int
		want       string
	}{
		{"basic", 1, 4, "éll"},
		{"negative start", -2, len([]rune(s)), "lo"},
		{"negative end", 0, -1, "héll"},
		{"swap when start>end", 4, 1, "éll"},
		{"clamp bounds", -100, 100, s},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := strutil.Substring(s, tt.start, tt.end); got != tt.want {
				t.Errorf("Substring() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCaseTransforms(t *testing.T) {
	if got := strutil.ToUpper("Abc"); got != "ABC" {
		t.Errorf("ToUpper() = %v, want %v", got, "ABC")
	}
	if got := strutil.ToLower("AbC"); got != "abc" {
		t.Errorf("ToLower() = %v, want %v", got, "abc")
	}
}

func TestTitleCaseAndCapitalize(t *testing.T) {
	if got := strutil.TitleCase("hello world"); got != "Hello World" {
		t.Errorf("TitleCase() = %v, want %v", got, "Hello World")
	}
	if got := strutil.Capitalize("éclair"); got != "Éclair" {
		t.Errorf("Capitalize() = %v, want %v", got, "Éclair")
	}
}

func TestContains(t *testing.T) {
	// plain
	b, err := strutil.Contains("hello world", "world", false)
	if err != nil || !b {
		t.Fatalf("Contains plain failed: %v %v", b, err)
	}
	// regex
	b, err = strutil.Contains("abc123", "[0-9]+", true)
	if err != nil || !b {
		t.Fatalf("Contains regex failed: %v %v", b, err)
	}
	// invalid regex
	if _, err := strutil.Contains("abc", "[", true); err == nil {
		t.Fatalf("expected error for invalid regex")
	}
}

func TestLength(t *testing.T) {
	if got := strutil.Length("héllo"); got != 5 { // rune count
		t.Errorf("Length() = %v, want %v", got, 5)
	}
}

func TestRegexExtract(t *testing.T) {
	matches, err := strutil.RegexExtract("a1 b22 c333", "([a-z])(\\d+)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expect three matches with two groups each (full + two submatches)
	if len(matches) != 3 {
		t.Fatalf("expected 3 matches, got %d", len(matches))
	}
	if matches[0][0] != "a1" || matches[0][1] != "a" || matches[0][2] != "1" {
		t.Errorf("unexpected first match: %#v", matches[0])
	}

	if _, err := strutil.RegexExtract("abc", "["); err == nil {
		t.Fatalf("expected error for bad pattern")
	}
}

func TestFormat(t *testing.T) {
	got := strutil.Format("Hello {name}, ${name}!", map[string]string{"name": "Alice"})
	want := "Hello Alice, Alice!"
	if got != want {
		t.Errorf("Format() = %v, want %v", got, want)
	}
}

func TestBase64(t *testing.T) {
	enc := strutil.Base64Encode("hi")
	dec, err := strutil.Base64Decode(enc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec != "hi" {
		t.Errorf("Base64 roundtrip = %v, want %v", dec, "hi")
	}

	if _, err := strutil.Base64Decode("@@@"); err == nil {
		t.Fatalf("expected error for invalid base64")
	}
}

func TestURIEncodeDecode(t *testing.T) {
	s := "hello world?&=+#"
	enc := strutil.URIEncode(s)
	dec, err := strutil.URIDecode(enc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec != s {
		t.Errorf("URI roundtrip = %v, want %v", dec, s)
	}
}

func TestNormalize(t *testing.T) {
	// Using a string with diacritics; approximate mapping based on implementation
	got := strutil.Normalize("Àéîõü ß Æ")
	want := "Aeiou ss AE" // Based on removeDiacritics rules: ß -> ss, Æ -> AE
	if got != want {
		t.Errorf("Normalize() = %q, want %q", got, want)
	}
}
