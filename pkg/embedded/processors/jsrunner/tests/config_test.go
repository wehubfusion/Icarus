package tests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
)

func TestConfigApplyDefaults(t *testing.T) {
	cfg := jsrunner.Config{}
	cfg.ApplyDefaults()

	if cfg.Timeout != 5*time.Second {
		t.Fatalf("expected default timeout 5s, got %v", cfg.Timeout)
	}
	if cfg.SecurityLevel != jsrunner.SecurityLevelStandard {
		t.Fatalf("expected default security level standard, got %s", cfg.SecurityLevel)
	}
	expectedUtils := jsrunner.DefaultUtilitiesByLevel[jsrunner.SecurityLevelStandard]
	if len(cfg.EnabledUtilities) != len(expectedUtils) {
		t.Fatalf("expected default utilities %v, got %v", expectedUtils, cfg.EnabledUtilities)
	}
	if cfg.MaxStackDepth != 100 {
		t.Fatalf("expected default max_stack_depth 100, got %d", cfg.MaxStackDepth)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := jsrunner.Config{Script: "console.log('ok')"}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}

	bad := []jsrunner.Config{
		{},                         // missing script
		{Script: "x", Timeout: -1}, // invalid timeout
		{Script: "x", Timeout: time.Second, SecurityLevel: "bad"}, // invalid security
		{Script: "x", Timeout: time.Second, MaxStackDepth: 0},     // invalid stack depth
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			t.Fatalf("expected validation error for config %d: %+v", i, c)
		}
	}
}

func TestConfigHasOutputSchema(t *testing.T) {
	cfg := jsrunner.Config{}
	if cfg.HasOutputSchema() {
		t.Fatalf("expected HasOutputSchema false by default")
	}
	cfg.OutputSchemaID = "id"
	if !cfg.HasOutputSchema() {
		t.Fatalf("expected HasOutputSchema true when OutputSchemaID set")
	}
	cfg = jsrunner.Config{OutputSchema: map[string]interface{}{"type": "object"}}
	if !cfg.HasOutputSchema() {
		t.Fatalf("expected HasOutputSchema true when OutputSchema set")
	}
}

func TestConfigUnmarshalJSONTimeout(t *testing.T) {
	data := []byte(`{"script":"x","timeout":"250ms"}`)
	var cfg jsrunner.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if cfg.Timeout != 250*time.Millisecond {
		t.Fatalf("expected timeout 250ms, got %v", cfg.Timeout)
	}
}





