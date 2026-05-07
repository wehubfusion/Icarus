package runtime

import (
	"testing"
)

// GetFieldMappings must include EVENT mappings that aren't gating triggers, so embedded
// nodes can read event values (like /error from a parent's pluginError section) as
// regular input data without being skipped.
func TestGetFieldMappings_IncludesNonTriggerEventMappings(t *testing.T) {
	cfg := EmbeddedNodeConfig{
		FieldMappings: []FieldMapping{
			{SourceNodeId: "p", SourceEndpoint: "/payload", DataType: "FIELD"},
			{SourceNodeId: "p", SourceEndpoint: "/error", SourceSectionId: SectionPluginError, DataType: "EVENT"},
			{SourceNodeId: "c", SourceEndpoint: "/false", DataType: "EVENT", IsEventTrigger: true},
		},
	}

	fields := cfg.GetFieldMappings()
	if len(fields) != 2 {
		t.Fatalf("expected 2 field mappings (FIELD + non-trigger EVENT); got %d: %#v", len(fields), fields)
	}

	hasField, hasNonTriggerEvent := false, false
	for _, m := range fields {
		if m.DataType == "FIELD" && m.SourceEndpoint == "/payload" {
			hasField = true
		}
		if m.DataType == "EVENT" && m.SourceEndpoint == "/error" && !m.IsEventTrigger {
			hasNonTriggerEvent = true
		}
	}
	if !hasField {
		t.Error("expected FIELD mapping in GetFieldMappings result")
	}
	if !hasNonTriggerEvent {
		t.Error("expected non-trigger EVENT mapping in GetFieldMappings result")
	}
}

// Event triggers must NOT appear in GetFieldMappings — they gate execution and are
// returned by GetEventMappings instead.
func TestGetFieldMappings_ExcludesEventTriggers(t *testing.T) {
	cfg := EmbeddedNodeConfig{
		FieldMappings: []FieldMapping{
			{SourceNodeId: "c", SourceEndpoint: "/false", DataType: "EVENT", IsEventTrigger: true},
		},
	}
	if got := cfg.GetFieldMappings(); len(got) != 0 {
		t.Fatalf("expected 0 field mappings (only an event trigger); got %d", len(got))
	}
}

func TestGetEventMappings_OnlyTriggers(t *testing.T) {
	cfg := EmbeddedNodeConfig{
		FieldMappings: []FieldMapping{
			{SourceNodeId: "p", SourceEndpoint: "/error", DataType: "EVENT"}, // non-trigger
			{SourceNodeId: "c", SourceEndpoint: "/false", DataType: "EVENT", IsEventTrigger: true},
		},
	}
	events := cfg.GetEventMappings()
	if len(events) != 1 {
		t.Fatalf("expected only 1 event trigger; got %d", len(events))
	}
	if events[0].SourceEndpoint != "/false" {
		t.Errorf("unexpected event mapping returned: %#v", events[0])
	}
}

// On the parent's success path, embedded nodes that consume /error from a pluginError
// section must see error=false. withImplicitNoErrorDefaults guarantees this without
// touching outputs that already carry an error flag (the failure path).
func TestWithImplicitNoErrorDefaults_AddsKeysWhenMissing(t *testing.T) {
	in := map[string]interface{}{"payload": "abc"}
	out := withImplicitNoErrorDefaults(in)
	if v, ok := out[ErrorOutputKeyError].(bool); !ok || v {
		t.Errorf("expected error=false; got %#v", out[ErrorOutputKeyError])
	}
	if v, ok := out[ErrorOutputKeyDescription].(string); !ok || v != "" {
		t.Errorf("expected errorDescription=''; got %#v", out[ErrorOutputKeyDescription])
	}
	if got := out["payload"]; got != "abc" {
		t.Errorf("expected payload preserved; got %#v", got)
	}
	// Original map must not be mutated (helper returns a shallow copy).
	if _, ok := in[ErrorOutputKeyError]; ok {
		t.Error("input map was mutated; expected withImplicitNoErrorDefaults to return a copy")
	}
}

func TestWithImplicitNoErrorDefaults_PreservesExistingError(t *testing.T) {
	in := map[string]interface{}{
		ErrorOutputKeyError:       true,
		ErrorOutputKeyDescription: "boom",
	}
	out := withImplicitNoErrorDefaults(in)
	if v, _ := out[ErrorOutputKeyError].(bool); !v {
		t.Errorf("expected error=true preserved; got %#v", out[ErrorOutputKeyError])
	}
	if v, _ := out[ErrorOutputKeyDescription].(string); v != "boom" {
		t.Errorf("expected errorDescription='boom' preserved; got %#v", out[ErrorOutputKeyDescription])
	}
}

func TestWithImplicitNoErrorDefaults_NilInput(t *testing.T) {
	out := withImplicitNoErrorDefaults(nil)
	if v, _ := out[ErrorOutputKeyError].(bool); v {
		t.Errorf("expected error=false on nil input; got %#v", out[ErrorOutputKeyError])
	}
}
