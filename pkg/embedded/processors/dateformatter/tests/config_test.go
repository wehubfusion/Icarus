package tests

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/dateformatter"
)

func TestConfigValidate(t *testing.T) {
	cfg := dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateOnly"}
	if err := cfg.Validate("node1"); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	// invalid format
	bad := dateformatter.Config{InFormat: "bad", OutFormat: "DateOnly"}
	if err := bad.Validate("node1"); err == nil {
		t.Fatalf("expected invalid input format error")
	}

	// invalid date style on non-date output
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "TimeOnly", DateStyle: string(dateformatter.DateStyleMM_DD_YYYY)}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected date_style validation error")
	}

	// invalid time style on non-time output
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateOnly", TimeStyle: string(dateformatter.TimeStyle12Hour)}
	if err := cfg.Validate("node1"); err == nil {
		t.Fatalf("expected time_style validation error")
	}
}

func TestConfigGetLayouts(t *testing.T) {
	cfg := dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateTime"}
	if got := cfg.GetInputLayout(); got == "" {
		t.Fatalf("expected input layout for RFC3339")
	}

	// DateTime with style overrides
	cfg.DateStyle = string(dateformatter.DateStyleMM_DD_YYYY_Slash)
	cfg.TimeStyle = string(dateformatter.TimeStyle12HourHM)
	outLayout := cfg.GetOutputLayout()
	if outLayout != "01/02/2006 03:04 PM" {
		t.Fatalf("unexpected output layout: %s", outLayout)
	}

	// DateOnly with date style
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "DateOnly", DateStyle: string(dateformatter.DateStyleYYYY_MM_DD_Slash)}
	if out := cfg.GetOutputLayout(); out != "2006/01/02" {
		t.Fatalf("expected date style layout, got %s", out)
	}

	// TimeOnly with time style
	cfg = dateformatter.Config{InFormat: "RFC3339", OutFormat: "TimeOnly", TimeStyle: string(dateformatter.TimeStyle24HourHM)}
	if out := cfg.GetOutputLayout(); out != "15:04" {
		t.Fatalf("expected time style layout, got %s", out)
	}
}
