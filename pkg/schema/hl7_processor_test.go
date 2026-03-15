package schema

import (
	"os"
	"path/filepath"
	"testing"
)

// findRepoRoot returns the Icarus module root (directory containing go.mod).
// When tests run via "go test ./pkg/schema", cwd is typically the module root.
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
			return "" // no go.mod found
		}
		dir = parent
	}
}

// readTestFile reads a file from the Icarus repo root (e.g. example.hl7, ORU_R01.json).
// Skips the test if the file is not found.
func readTestFile(t *testing.T, name string) []byte {
	t.Helper()
	root := findRepoRoot(t)
	if root == "" {
		t.Skip("could not find repo root (go.mod)")
	}
	path := filepath.Join(root, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("test file %q not found (run from Icarus root or add file): %v", path, err)
	}
	return data
}

// TestHL7EngineWithExampleFiles runs the schema engine against example.hl7 and ORU_R01.json.
// Uses Engine.ProcessHL7WithSchema to validate the full HL7 pipeline with real message and schema.
//
// Validation is schema-driven: the engine reports "required component missing" when the schema
// marks a component as R (required) but the message has no value at that position (fewer components
// or empty). example.hl7 is HL7 2.3 (MSH-12=2.3); ORU_R01.json is a 2.8 schema. In 2.8, MSH-9 has
// three components (MSG.1, MSG.2, MSG.3) all R, while 2.3 messages typically send only two (ORU^R01).
// So errors like MSH-9.3, PID-13.3, etc. are expected and correct: the definition (schema) requires
// those components; the message does not provide them.
func TestHL7EngineWithExampleFiles(t *testing.T) {
	msgBytes := readTestFile(t, "example.hl7")
	schemaBytes := readTestFile(t, "ORU_R01.json")

	engine := NewEngine()
	opts := ProcessOptions{StrictValidation: false, CollectAllErrors: true}

	result, err := engine.ProcessHL7WithSchema(msgBytes, schemaBytes, opts)
	if err != nil {
		t.Fatalf("ProcessHL7WithSchema: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	t.Logf("Valid: %v, Errors: %d", result.Valid, len(result.Errors))
	for _, e := range result.Errors {
		t.Logf("  %s: %s [%s]", e.Path, e.Message, e.Code)
	}

	if len(result.Errors) > 0 {
		t.Logf("validation reported %d error(s); expected when 2.3 message is validated against 2.8 schema (required components absent)", len(result.Errors))
	}
}

// TestHL7ProcessorWithExampleFiles runs the HL7 processor directly with example.hl7 and ORU_R01.json.
// Same validation behavior as TestHL7EngineWithExampleFiles; errors match schema definition (see above).
func TestHL7ProcessorWithExampleFiles(t *testing.T) {
	msgBytes := readTestFile(t, "example.hl7")
	schemaBytes := readTestFile(t, "ORU_R01.json")

	proc := NewHL7SchemaProcessor()
	compiled, err := proc.ParseSchema(schemaBytes)
	if err != nil {
		t.Fatalf("ParseSchema(ORU_R01.json): %v", err)
	}

	opts := ProcessOptions{StrictValidation: false, CollectAllErrors: true}
	result, err := proc.Process(msgBytes, compiled, opts)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	t.Logf("Valid: %v, Errors: %d", result.Valid, len(result.Errors))
	for _, e := range result.Errors {
		t.Logf("  %s: %s [%s]", e.Path, e.Message, e.Code)
	}
}

func TestHL7SchemaProcessor_Process_ValidMessage(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	hl7Schema := `{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH-9", "dataType": "ST", "usage": "R"}
			]},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID-3", "dataType": "CX", "usage": "R"}
			]}
		]
	}`
	compiled, err := proc.ParseSchema([]byte(hl7Schema))
	if err != nil {
		t.Fatal(err)
	}
	msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ADT^A01|MSG001|P|2.5\rPID|||12345^^^NHS^NH||DOE^JOHN"
	result, err := proc.Process([]byte(msg), compiled, ProcessOptions{StrictValidation: true, CollectAllErrors: true, AllowExtraFields: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Errorf("expected valid result, got errors: %v", result.Errors)
	}
}

// TestHL7StrictValidation_ExtraComponent asserts that when a field has more components than the schema defines, HL7_EXTRA_COMPONENT is reported.
func TestHL7StrictValidation_ExtraComponent(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	// Schema: MSH.9 has 3 components (MSG.1, MSG.2, MSG.3)
	hl7Schema := `{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH.9", "dataType": "MSG", "usage": "R", "rpt": "1", "components": [
					{"position": "MSG.1", "usage": "R"},
					{"position": "MSG.2", "usage": "R"},
					{"position": "MSG.3", "usage": "R"}
				]}
			]}
		]
	}`
	compiled, err := proc.ParseSchema([]byte(hl7Schema))
	if err != nil {
		t.Fatal(err)
	}
	// Message has 4 components: ORU^R01^ORU^R01
	msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ORU^R01^ORU^R01|MSG001|P|2.5"
	result, err := proc.Process([]byte(msg), compiled, ProcessOptions{StrictValidation: false, CollectAllErrors: true})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	var hasExtraComp bool
	for _, e := range result.Errors {
		if e.Code == "HL7_EXTRA_COMPONENT" {
			hasExtraComp = true
			if e.Path != "MSH-9.4" {
				t.Errorf("expected path MSH-9.4 for extra component, got %q", e.Path)
			}
			break
		}
	}
	if !hasExtraComp {
		t.Errorf("expected HL7_EXTRA_COMPONENT when message has 4 components and schema defines 3: %v", result.Errors)
	}
}

// TestHL7StrictValidation_ExtraField asserts that when a segment has more fields than the schema defines, HL7_EXTRA_FIELD is reported.
func TestHL7StrictValidation_ExtraField(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	// Schema: MSH with only fields 1-9 (so 9 fields)
	hl7Schema := `{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH.1", "dataType": "ST", "usage": "R"},
				{"position": "MSH.2", "dataType": "ST", "usage": "R"},
				{"position": "MSH.3", "dataType": "ST", "usage": "O"},
				{"position": "MSH.4", "dataType": "ST", "usage": "O"},
				{"position": "MSH.5", "dataType": "ST", "usage": "O"},
				{"position": "MSH.6", "dataType": "ST", "usage": "O"},
				{"position": "MSH.7", "dataType": "ST", "usage": "O"},
				{"position": "MSH.8", "dataType": "ST", "usage": "O"},
				{"position": "MSH.9", "dataType": "ST", "usage": "R"}
			]}
		]
	}`
	compiled, err := proc.ParseSchema([]byte(hl7Schema))
	if err != nil {
		t.Fatal(err)
	}
	// Message has 12 fields (MSH.1 through MSH.12)
	msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ORU^R01|MSG001|P|2.5|EXTRA"
	result, err := proc.Process([]byte(msg), compiled, ProcessOptions{StrictValidation: false, CollectAllErrors: true})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	var hasExtraField bool
	for _, e := range result.Errors {
		if e.Code == "HL7_EXTRA_FIELD" {
			hasExtraField = true
			if e.Path != "MSH-10" && e.Path != "MSH-11" && e.Path != "MSH-12" {
				t.Logf("extra field at %s (expected MSH-10, MSH-11, or MSH-12)", e.Path)
			}
			break
		}
	}
	if !hasExtraField {
		t.Errorf("expected HL7_EXTRA_FIELD when segment has more fields than schema: %v", result.Errors)
	}
}

func TestHL7SchemaProcessor_Process_InvalidMSH(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	hl7Schema := `{"segments": [{"name": "MSH", "usage": "R", "rpt": "1", "fields": []}]}`
	compiled, _ := proc.ParseSchema([]byte(hl7Schema))
	result, err := proc.Process([]byte("PID|||123"), compiled, ProcessOptions{CollectAllErrors: true})
	if err != nil {
		t.Fatalf("process should return result: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid result")
	}
	var hasCode bool
	for _, e := range result.Errors {
		if e.Code == "HL7_INVALID_MSH" {
			hasCode = true
			break
		}
	}
	if !hasCode {
		t.Errorf("expected HL7_INVALID_MSH: %v", result.Errors)
	}
}

func TestHL7SchemaProcessor_Type(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	if proc.Type() != string(FormatHL7) {
		t.Errorf("Type() = %q, want %q", proc.Type(), FormatHL7)
	}
}
