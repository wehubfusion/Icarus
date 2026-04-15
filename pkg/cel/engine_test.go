package cel

import "testing"

func TestDefaultAssertionFailureMessage(t *testing.T) {
	cases := []struct {
		rule InputRule
		path string
		want string
	}{
		{
			rule: InputRule{ID: "r1", Name: "Patient ID required"},
			path: "PID-3",
			want: `Assertion failed for validation rule "Patient ID required" (id: r1) at PID-3.`,
		},
		{
			rule: InputRule{ID: "x", Name: "Only name"},
			path: "",
			want: `Assertion failed for validation rule "Only name" (id: x).`,
		},
		{
			rule: InputRule{ID: "orphan"},
			path: "MSH-9",
			want: `Assertion failed for validation rule (id: orphan) at MSH-9.`,
		},
		{
			rule: InputRule{},
			path: "",
			want: `Assertion failed for validation rule (unknown rule).`,
		},
	}
	for _, tc := range cases {
		got := defaultAssertionFailureMessage(tc.rule, tc.path)
		if got != tc.want {
			t.Errorf("defaultAssertionFailureMessage(rule id=%q name=%q, path=%q) = %q, want %q",
				tc.rule.ID, tc.rule.Name, tc.path, got, tc.want)
		}
	}
}
