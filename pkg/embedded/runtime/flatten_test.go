package runtime

import (
	"testing"
)

func TestParseNestedArrayPath_RootArray(t *testing.T) {
	testCases := []struct {
		path     string
		expected []ArrayPathSegment
	}{
		{
			path: "//name",
			expected: []ArrayPathSegment{
				{Path: RootArrayKey, IsArray: true},
				{Path: "name", IsArray: false},
			},
		},
		{
			path: "/data//name",
			expected: []ArrayPathSegment{
				{Path: "data", IsArray: true},
				{Path: "name", IsArray: false},
			},
		},
		{
			path: "//assignments//name",
			expected: []ArrayPathSegment{
				{Path: RootArrayKey, IsArray: true},
				{Path: "assignments", IsArray: true},
				{Path: "name", IsArray: false},
			},
		},
		{
			path: "/name",
			expected: []ArrayPathSegment{
				{Path: "name", IsArray: false},
			},
		},
		{
			path: "//contact/email",
			expected: []ArrayPathSegment{
				{Path: RootArrayKey, IsArray: true},
				{Path: "contact/email", IsArray: false},
			},
		},
		// Trailing slash tests - for primitive array iteration
		{
			path: "//chapters/",
			expected: []ArrayPathSegment{
				{Path: RootArrayKey, IsArray: true},
				{Path: "chapters", IsArray: true}, // trailing slash marks this as array
			},
		},
		{
			path: "//assignments//details/topics//chapters/",
			expected: []ArrayPathSegment{
				{Path: RootArrayKey, IsArray: true},
				{Path: "assignments", IsArray: true},
				{Path: "details/topics", IsArray: true},
				{Path: "chapters", IsArray: true}, // trailing slash marks this as array
			},
		},
		{
			path: "/data//items/",
			expected: []ArrayPathSegment{
				{Path: "data", IsArray: true},
				{Path: "items", IsArray: true}, // trailing slash
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			segments := ParseNestedArrayPath(tc.path)
			if len(segments) != len(tc.expected) {
				t.Errorf("ParseNestedArrayPath(%q) returned %d segments, expected %d: got %+v", tc.path, len(segments), len(tc.expected), segments)
				return
			}
			for i, seg := range segments {
				if seg.Path != tc.expected[i].Path || seg.IsArray != tc.expected[i].IsArray {
					t.Errorf("ParseNestedArrayPath(%q) segment %d: got {Path: %q, IsArray: %v}, expected {Path: %q, IsArray: %v}",
						tc.path, i, seg.Path, seg.IsArray, tc.expected[i].Path, tc.expected[i].IsArray)
				}
			}
		})
	}
}

func TestNormalizeRootArrayEndpoint(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		// Root-array patterns (should be normalized)
		{"//name", "/$items//name"},
		{"//chapters/", "/$items//chapters/"}, // Trailing slash preserved
		{"//assignments//name", "/$items//assignments//name"},
		{"//data//items/", "/$items//data//items/"}, // Trailing slash preserved

		// Non-root-array patterns (should remain unchanged)
		{"/name", "/name"},
		{"/data//name", "/data//name"},
		{"/data//items/", "/data//items/"}, // Trailing slash preserved, not root-array
		{"", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := NormalizeRootArrayEndpoint(tc.input)
			if result != tc.expected {
				t.Errorf("NormalizeRootArrayEndpoint(%q) = %q, expected %q", tc.input, result, tc.expected)
			}
		})
	}
}
