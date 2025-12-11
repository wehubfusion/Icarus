package tests

import (
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
)

func TestConfigValidate(t *testing.T) {
	valid := jsonops.Config{Operation: "parse", SchemaID: "sid"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	cases := []jsonops.Config{
		{},                                  // missing operation
		{Operation: "bad", SchemaID: "sid"}, // invalid op
		{Operation: "parse"},                // missing schema id and schema
		{Operation: "produce", Schema: json.RawMessage{}}, // empty schema
	}
	for i, cfg := range cases {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected validation error for case %d: %+v", i, cfg)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	cfgParse := jsonops.Config{Operation: "parse", SchemaID: "sid"}
	if !cfgParse.GetApplyDefaults() {
		t.Fatalf("parse should default apply_defaults true")
	}
	if cfgParse.GetStructureData() {
		t.Fatalf("parse should default structure_data false")
	}
	if cfgParse.GetStrictValidation() {
		t.Fatalf("parse should default strict_validation false")
	}

	cfgProduce := jsonops.Config{Operation: "produce", SchemaID: "sid"}
	if cfgProduce.GetApplyDefaults() {
		t.Fatalf("produce should default apply_defaults false")
	}
	if !cfgProduce.GetStructureData() {
		t.Fatalf("produce should default structure_data true")
	}
	if !cfgProduce.GetStrictValidation() {
		t.Fatalf("produce should default strict_validation true")
	}
}

func TestConfigExplicitOverrides(t *testing.T) {
	apply := false
	structure := false
	strict := false
	cfg := jsonops.Config{
		Operation:        "produce",
		SchemaID:         "sid",
		ApplyDefaults:    &apply,
		StructureData:    &structure,
		StrictValidation: &strict,
	}

	if cfg.GetApplyDefaults() != apply {
		t.Fatalf("expected apply_defaults override")
	}
	if cfg.GetStructureData() != structure {
		t.Fatalf("expected structure_data override")
	}
	if cfg.GetStrictValidation() != strict {
		t.Fatalf("expected strict_validation override")
	}
}
