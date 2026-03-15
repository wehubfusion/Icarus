package schema

import (
	"os"
	"path/filepath"
	"testing"
)

// findRepoRoot returns the Icarus module root (directory containing go.mod).
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// readTestFile reads a file from the Icarus repo root. Skips the test if the file is not found.
func readTestFile(t *testing.T, name string) []byte {
	t.Helper()
	root := findRepoRoot(t)
	if root == "" {
		t.Skip("could not find repo root (go.mod)")
	}
	path := filepath.Join(root, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("test file %q not found: %v", path, err)
	}
	return data
}

const (
	// Schema with MSH fields 1-12 and PID-3 so a typical ADT message validates without HL7_EXTRA_FIELD.
	validHL7Schema = `{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH-1", "dataType": "ST", "usage": "R"},
				{"position": "MSH-2", "dataType": "ST", "usage": "R"},
				{"position": "MSH-3", "dataType": "ST", "usage": "O"},
				{"position": "MSH-4", "dataType": "ST", "usage": "O"},
				{"position": "MSH-5", "dataType": "ST", "usage": "O"},
				{"position": "MSH-6", "dataType": "ST", "usage": "O"},
				{"position": "MSH-7", "dataType": "TS", "usage": "O"},
				{"position": "MSH-8", "dataType": "ST", "usage": "O"},
				{"position": "MSH-9", "dataType": "MSG", "usage": "R"},
				{"position": "MSH-10", "dataType": "ST", "usage": "O"},
				{"position": "MSH-11", "dataType": "PT", "usage": "O"},
				{"position": "MSH-12", "dataType": "VID", "usage": "O"}
			]},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID-3", "dataType": "CX", "usage": "R"}
			]}
		]
	}`
	validHL7Message = "MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5\rPID|||123"
)

func TestValidateHL7Only_ValidMessage(t *testing.T) {
	engine := NewEngine()
	result, err := engine.ValidateHL7Only([]byte(validHL7Message), []byte(validHL7Schema))
	if err != nil {
		t.Fatalf("ValidateHL7Only: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if !result.Valid {
		t.Errorf("expected valid=true, got false; errors: %v", result.Errors)
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %d: %v", len(result.Errors), result.Errors)
	}
}

func TestValidateHL7Only_InvalidMessage_MissingRequiredSegment(t *testing.T) {
	engine := NewEngine()
	// Message has MSH only; schema requires PID
	msg := []byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5")
	result, err := engine.ValidateHL7Only(msg, []byte(validHL7Schema))
	if err != nil {
		t.Fatalf("ValidateHL7Only: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if result.Valid {
		t.Error("expected valid=false for message missing required PID segment")
	}
	var found bool
	for _, e := range result.Errors {
		if e.Code == "HL7_MISSING_REQUIRED" && e.Path == "PID" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected HL7_MISSING_REQUIRED for PID, got errors: %v", result.Errors)
	}
}

func TestValidateHL7Only_InvalidMessage_RequiredFieldEmpty(t *testing.T) {
	schema := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": []},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID-3", "dataType": "CX", "usage": "R"}
			]}
		]
	}`)
	// PID-3 is required but empty (||||)
	msg := []byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5\rPID||||DOE^JOHN")
	engine := NewEngine()
	result, err := engine.ValidateHL7Only(msg, schema)
	if err != nil {
		t.Fatalf("ValidateHL7Only: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if result.Valid {
		t.Error("expected valid=false for message with empty required PID-3")
	}
	var found bool
	for _, e := range result.Errors {
		if e.Code == "HL7_REQUIRED" && e.Path == "PID-3" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected HL7_REQUIRED for PID-3, got errors: %v", result.Errors)
	}
}

func TestValidateHL7Only_InvalidSchema(t *testing.T) {
	engine := NewEngine()
	_, err := engine.ValidateHL7Only([]byte(validHL7Message), []byte(`{invalid json`))
	if err == nil {
		t.Error("expected error for invalid schema JSON")
	}
}

func TestValidateHL7Only_MalformedMessage(t *testing.T) {
	engine := NewEngine()
	// Not valid HL7 (no MSH or malformed)
	result, err := engine.ValidateHL7Only([]byte("PID|||123"), []byte(validHL7Schema))
	if err != nil {
		t.Fatalf("ValidateHL7Only: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if result.Valid {
		t.Error("expected valid=false for malformed message")
	}
	var found bool
	for _, e := range result.Errors {
		if e.Code == "HL7_INVALID_MSH" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected HL7_INVALID_MSH for malformed message, got errors: %v", result.Errors)
	}
}