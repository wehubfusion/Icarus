package httpclient_test

import (
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/httpclient"
)

func TestConfigValidate_MissingConnectionID(t *testing.T) {
	cfg := httpclient.Config{ConnectionID: "", Connection: nil}
	err := cfg.Validate("node1")
	if err == nil {
		t.Fatal("expected error when connection_id and connection are empty")
	}
	if _, ok := err.(*httpclient.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T", err)
	}
}

func TestConfigValidate_ConnectionNotEnriched(t *testing.T) {
	cfg := httpclient.Config{ConnectionID: "conn-123", Connection: nil}
	err := cfg.Validate("node1")
	if err == nil {
		t.Fatal("expected error when connection_id present but connection not enriched")
	}
	if _, ok := err.(*httpclient.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T", err)
	}
}

func TestConfigValidate_MissingURL(t *testing.T) {
	cfg := httpclient.Config{
		Connection: map[string]interface{}{},
	}
	err := cfg.Validate("node1")
	if err == nil {
		t.Fatal("expected error when connection has no url")
	}
}

func TestConfigValidate_EmptyURL(t *testing.T) {
	cfg := httpclient.Config{
		Connection: map[string]interface{}{"url": ""},
	}
	err := cfg.Validate("node1")
	if err == nil {
		t.Fatal("expected error when url is empty string")
	}
}

func TestConfigValidate_Valid(t *testing.T) {
	cfg := httpclient.Config{
		Connection: map[string]interface{}{"url": "https://example.com/api"},
	}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

func TestConfigValidate_WithManualInputs(t *testing.T) {
	cfg := httpclient.Config{
		Connection:   map[string]interface{}{"url": "https://example.com"},
		ManualInputs: []httpclient.ManualInput{{Key: "X-Custom", Value: "value"}},
	}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid config with manual_inputs, got %v", err)
	}
}

func TestConfigValidate_URLSourceConfig_RequiresURLAndMethod(t *testing.T) {
	cfg := httpclient.Config{
		URL:    &httpclient.BindableText{Source: "config"},
		Method: "",
	}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatal("expected error when url.source=config but url.value/method missing")
	}
}

func TestConfigValidate_URLSourceConfig_Valid(t *testing.T) {
	cfg := httpclient.Config{
		URL:    &httpclient.BindableText{Source: "config", Value: "https://example.com"},
		Method: "GET",
	}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

func TestConfigValidate_URLSourceInput_RequiresMethod(t *testing.T) {
	cfg := httpclient.Config{
		URL:    &httpclient.BindableText{Source: "input", InputKey: "url"},
		Method: "",
	}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatal("expected error when url.source=input but method missing")
	}
}

func TestConfigValidate_URLSourceInput_RejectsNonUrlInputKey(t *testing.T) {
	cfg := httpclient.Config{
		URL:    &httpclient.BindableText{Source: "input", InputKey: "request_url"},
		Method: "GET",
	}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatal("expected error when url.inputKey is not \"url\"")
	}
}

func TestConfigUnmarshalJSON(t *testing.T) {
	raw := `{
		"label": "test",
		"connection_id": "conn-1",
		"manual_inputs": [{"key": "Accept", "value": "application/json"}],
		"connection": {
			"url": "https://api.example.com",
			"method": "POST"
		}
	}`
	var cfg httpclient.Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if cfg.Label != "test" {
		t.Errorf("label: got %q", cfg.Label)
	}
	if cfg.ConnectionID != "conn-1" {
		t.Errorf("connection_id: got %q", cfg.ConnectionID)
	}
	if len(cfg.ManualInputs) != 1 || cfg.ManualInputs[0].Key != "Accept" || cfg.ManualInputs[0].Value != "application/json" {
		t.Errorf("manual_inputs: got %+v", cfg.ManualInputs)
	}
	if cfg.Connection == nil {
		t.Fatal("connection should be populated")
	}
	if url, _ := cfg.Connection["url"].(string); url != "https://api.example.com" {
		t.Errorf("connection.url: got %q", url)
	}
	if method, _ := cfg.Connection["method"].(string); method != "POST" {
		t.Errorf("connection.method: got %q", method)
	}
}

func TestConfigUnmarshalJSON_DynamicManualInputs(t *testing.T) {
	raw := `{
		"label": "test",
		"url": {"source":"input","inputKey":"url"},
		"method": "GET",
		"headers": {"source":"input","inputKey":"manual_inputs"},
		"manual_inputs": [{"name":"X-Test"}]
	}`
	var cfg httpclient.Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if cfg.URL == nil || cfg.URL.Source != "input" || cfg.URL.InputKey != "url" {
		t.Fatalf("url: got %+v", cfg.URL)
	}
	if cfg.Method != "GET" {
		t.Fatalf("method: got %q", cfg.Method)
	}
	if cfg.Headers == nil || cfg.Headers.Source != "input" || cfg.Headers.InputKey != "manual_inputs" {
		t.Fatalf("headers: got %+v", cfg.Headers)
	}
	if len(cfg.ManualInputs) != 1 || cfg.ManualInputs[0].Name != "X-Test" {
		t.Fatalf("manual_inputs: got %+v", cfg.ManualInputs)
	}
}
