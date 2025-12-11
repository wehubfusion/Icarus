package strings_test

import (
	"testing"

	stringsproc "github.com/wehubfusion/Icarus/pkg/embedded/processors/strings"
)

func TestConfigValidate(t *testing.T) {
	cfg := stringsproc.Config{Operation: "concatenate"}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	cfg = stringsproc.Config{Operation: "bad"}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected invalid operation error")
	}

	cfg = stringsproc.Config{Operation: "concatenate", ManualInputs: []stringsproc.ManualInput{{Name: "", Type: "string"}}}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected manual input validation error")
	}

	cfg = stringsproc.Config{Operation: "concatenate", ManualInputs: []stringsproc.ManualInput{{Name: "id", Type: "number"}}}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid manual input config, got %v", err)
	}
}




