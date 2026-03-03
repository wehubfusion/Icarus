package runtime

import (
	"encoding/json"
	"testing"
)

func TestNormalizeRawConfig_PreservesConnection(t *testing.T) {
	// Config from Elysium enrichment: connection_id, connection, label, nodeSchema
	raw := []byte(`{
		"connection_id": "dd1f8d8e-6c45-49eb-8725-f5a41ae6aeac",
		"connection": {"url": "https://example.com", "method": "POST"},
		"label": "test",
		"manual_inputs": [{"key": "", "value": ""}],
		"nodeSchema": [{"key": "label", "value": "test"}]
	}`)
	result := NormalizeRawConfig(raw)
	if len(result) == 0 {
		t.Fatal("NormalizeRawConfig returned empty")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if m["connection_id"] != "dd1f8d8e-6c45-49eb-8725-f5a41ae6aeac" {
		t.Errorf("connection_id: got %v", m["connection_id"])
	}
	conn, ok := m["connection"].(map[string]interface{})
	if !ok {
		t.Fatalf("connection: got %T", m["connection"])
	}
	if conn["url"] != "https://example.com" {
		t.Errorf("connection.url: got %v", conn["url"])
	}
	if m["label"] != "test" {
		t.Errorf("label: got %v", m["label"])
	}
	if _, ok := m["nodeSchema"]; ok {
		t.Error("nodeSchema should be removed from output")
	}
}

func TestNormalizeRawConfig_NodeSchemaOnly(t *testing.T) {
	// Config with only nodeSchema (no connection keys)
	raw := []byte(`{
		"nodeSchema": [{"key": "script", "value": "x=1"}, {"key": "label", "value": "my-node"}]
	}`)
	result := NormalizeRawConfig(raw)
	if len(result) == 0 {
		t.Fatal("NormalizeRawConfig returned empty")
	}
	var m map[string]interface{}
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if m["script"] != "x=1" {
		t.Errorf("script: got %v", m["script"])
	}
	if m["label"] != "my-node" {
		t.Errorf("label: got %v", m["label"])
	}
	if _, ok := m["nodeSchema"]; ok {
		t.Error("nodeSchema should be removed from output")
	}
}

func TestNormalizeRawConfig_NoNodeSchema(t *testing.T) {
	// Config without nodeSchema - returns unchanged
	raw := []byte(`{"connection_id": "abc", "label": "flat"}`)
	result := NormalizeRawConfig(raw)
	if string(result) != string(raw) {
		t.Errorf("config without nodeSchema should be unchanged: got %s", string(result))
	}
}
