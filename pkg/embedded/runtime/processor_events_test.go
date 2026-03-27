package runtime

import "testing"

func TestExtractEventOutputs_EmbeddedNodesAndParentError(t *testing.T) {
	stdOut := StandardUnitOutput{
		"parent-/payload":             "large-data",
		"parent-/error":               true,
		"parent-/errorDescription":    "failed",
		"embedded-condition-/true":    map[string]interface{}{"id": "x"},
		"embedded-condition-/false":   nil,
		"embedded-transform-/value[0]": "a",
	}

	events := extractEventOutputs(stdOut, "parent")

	if _, ok := events["parent-/payload"]; ok {
		t.Fatalf("expected parent payload key to be excluded from events")
	}
	if _, ok := events["parent-/error"]; !ok {
		t.Fatalf("expected parent error key to be included in events")
	}
	if _, ok := events["embedded-condition-/true"]; !ok {
		t.Fatalf("expected embedded condition key to be included in events")
	}
	if _, ok := events["embedded-transform-/value[0]"]; !ok {
		t.Fatalf("expected embedded node key to be included in events")
	}
}
