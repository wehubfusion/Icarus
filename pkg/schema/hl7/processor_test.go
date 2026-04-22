package hl7

import (
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

func TestParseHL7Schema_Valid(t *testing.T) {
	def := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH-1", "dataType": "ST", "usage": "R"},
				{"position": "MSH-9", "dataType": "MSG", "usage": "R"}
			]},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID-3", "dataType": "CX", "usage": "R"}
			]}
		]
	}`)
	schema, err := ParseHL7Schema(def)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Segments) != 2 {
		t.Errorf("expected 2 segments, got %d", len(schema.Segments))
	}
}

func TestMatchMessage_Order(t *testing.T) {
	def := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": []},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": []}
		]
	}`)
	compiled, _ := ParseHL7Schema(def)
	msg, _ := ParseMessage([]byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5\rPID|||123"))
	match := MatchMessage(msg, &CompiledHL7Schema{Schema: compiled})
	if len(match.Errors) != 0 {
		t.Errorf("unexpected errors: %v", match.Errors)
	}
	if len(match.Matched) != 2 {
		t.Errorf("expected 2 matched segments, got %d", len(match.Matched))
	}
}

// TestMatchMessage_ZSegmentPassthrough asserts that Z-segments not in the schema are skipped during matching and not reported as unexpected.
func TestMatchMessage_ZSegmentPassthrough(t *testing.T) {
	def := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": []},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": []}
		]
	}`)
	compiled, _ := ParseHL7Schema(def)
	// Message: MSH, ZPD (Z-segment), PID — ZPD should be skipped, PID matched
	msg, _ := ParseMessage([]byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5\rZPD|foo|bar\rPID|||123"))
	match := MatchMessage(msg, &CompiledHL7Schema{Schema: compiled})
	for _, e := range match.Errors {
		if e.Code == "HL7_UNEXPECTED_SEGMENT" {
			t.Errorf("Z-segment should not be reported as unexpected: %v", match.Errors)
		}
	}
	if len(match.Matched) != 2 {
		t.Errorf("expected 2 matched segments (MSH, PID), got %d", len(match.Matched))
	}
	// Message: MSH, PID, ZAL at end — ZAL should not be reported as unexpected
	msg2, _ := ParseMessage([]byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5\rPID|||123\rZAL|ext"))
	match2 := MatchMessage(msg2, &CompiledHL7Schema{Schema: compiled})
	for _, e := range match2.Errors {
		if e.Code == "HL7_UNEXPECTED_SEGMENT" {
			t.Errorf("Z-segment at end should not be reported as unexpected: %v", match2.Errors)
		}
	}
	if len(match2.Matched) != 2 {
		t.Errorf("expected 2 matched segments, got %d", len(match2.Matched))
	}
}

func TestMatchMessage_MissingRequired(t *testing.T) {
	def := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": []},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": []}
		]
	}`)
	compiled, _ := ParseHL7Schema(def)
	msg, _ := ParseMessage([]byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5"))
	match := MatchMessage(msg, &CompiledHL7Schema{Schema: compiled})
	var found bool
	for _, e := range match.Errors {
		if e.Code == "HL7_MISSING_REQUIRED" && e.Path == "PID" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected HL7_MISSING_REQUIRED for PID, got errors: %v", match.Errors)
	}
}

func TestValidateMatchResult_RequiredField(t *testing.T) {
	def := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH-9", "dataType": "ST", "usage": "R"}
			]},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID-3", "dataType": "CX", "usage": "R"}
			]}
		]
	}`)
	var schema HL7Schema
	if err := json.Unmarshal(def, &schema); err != nil {
		t.Fatal(err)
	}
	compiled := &CompiledHL7Schema{Schema: &schema}
	// Message with empty PID-3 (required)
	msg, _ := ParseMessage([]byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5\rPID||||DOE^JOHN"))
	match := MatchMessage(msg, compiled)
	errs := ValidateMatchResult(match, msg, true, nil)
	var found bool
	for _, e := range errs {
		if e.Code == "HL7_REQUIRED" && e.Path == "PID-3" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected HL7_REQUIRED for PID-3, got: %v", errs)
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
	result, err := proc.Process([]byte(msg), compiled, contracts.ProcessOptions{CollectAllErrors: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Valid {
		t.Errorf("expected valid result, got errors: %v", result.Errors)
	}
}

func TestHL7CustomSeverity_ExtraFieldWarning(t *testing.T) {
	proc := NewHL7SchemaProcessor()
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
	msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ORU^R01|MSG001|P|2.5|EXTRA"
	result, err := proc.Process([]byte(msg), compiled, contracts.ProcessOptions{
		CollectAllErrors: true,
		CodeSeverityOverrides: map[string]contracts.Severity{
			"HL7_EXTRA_FIELD": contracts.SeverityWarning,
		},
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	var hasExtraField bool
	for _, e := range result.Warnings {
		if e.Code == "HL7_EXTRA_FIELD" {
			hasExtraField = true
			break
		}
	}
	if !hasExtraField {
		t.Errorf("expected HL7_EXTRA_FIELD when segment has more fields than schema: errors=%v warnings=%v infos=%v", result.Errors, result.Warnings, result.Infos)
	}
}

func TestHL7SchemaProcessor_Process_InvalidMSH(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	hl7Schema := `{"segments": [{"name": "MSH", "usage": "R", "rpt": "1", "fields": []}]}`
	compiled, _ := proc.ParseSchema([]byte(hl7Schema))
	result, err := proc.Process([]byte("PID|||123"), compiled, contracts.ProcessOptions{CollectAllErrors: true})
	if err != nil {
		t.Fatalf("Process must never return a non-nil error; got: %v", err)
	}
	if result == nil || result.Valid {
		t.Fatalf("expected invalid result: result=%v", result)
	}
	var hasCode bool
	for _, e := range result.Errors {
		if e.Code == "HL7_INVALID_MSH" {
			hasCode = true
			break
		}
	}
	if !hasCode {
		t.Errorf("expected HL7_INVALID_MSH in errors: %v", result.Errors)
	}
}

func TestHL7SchemaProcessor_Type(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	if proc.Type() != string(contracts.FormatHL7) {
		t.Errorf("Type() = %q, want %q", proc.Type(), contracts.FormatHL7)
	}
}

func TestHL7CustomSeverity_CELRuntimeError_UsesRuleSeverity(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	schemaDef := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH.1", "dataType": "ST", "usage": "R"},
				{"position": "MSH.2", "dataType": "ST", "usage": "R"},
				{"position": "MSH.7", "dataType": "DTM", "usage": "O"},
				{"position": "MSH.9", "dataType": "ST", "usage": "R"}
			]},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID.3", "dataType": "CX", "usage": "R"},
				{"position": "PID.5", "dataType": "XPN", "usage": "O"}
			]}
		],
		"rules": [
			{
				"id": "bad-regex",
				"name": "intentional eval error",
				"when": "valued('PID-5')",
				"assert": "matchesPattern('PID-5', '[')",
				"message": "should not evaluate cleanly",
				"errorPath": "PID-5",
				"severity": "ERROR"
			}
		]
	}`)

	compiled, err := proc.ParseSchema(schemaDef)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	msg := []byte("MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ADT^A01|MSG001|P|2.8\rPID|||12345^^^NHS^NH||DOE^JOHN")
	result, err := proc.Process(msg, compiled, contracts.ProcessOptions{
		CollectAllErrors: true,
	})
	if err != nil {
		t.Fatalf("expected nil process error; got: %v", err)
	}
	if result == nil {
		t.Fatal("Process must return a non-nil result")
	}
	if result.Valid {
		t.Fatalf("expected valid:false; errors=%v", result.Errors)
	}
	var found bool
	for _, e := range result.Errors {
		if e.Code == "HL7_CUSTOM_RULE_RUNTIME_ERROR" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected HL7_CUSTOM_RULE_RUNTIME_ERROR in Errors, got errors=%v warnings=%v", result.Errors, result.Warnings)
	}
}

func TestHL7CustomSeverity_InvalidMSH_DroppedIsValid(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	hl7Schema := `{"segments": [{"name": "MSH", "usage": "R", "rpt": "1", "fields": []}]}`
	compiled, _ := proc.ParseSchema([]byte(hl7Schema))
	result, err := proc.Process([]byte("PID|||123"), compiled, contracts.ProcessOptions{
		CollectAllErrors: true,
		CodeSeverityOverrides: map[string]contracts.Severity{
			"HL7_INVALID_MSH": contracts.Severity("DROP"),
		},
	})
	if err != nil {
		t.Fatalf("Process must never return a non-nil error; got: %v", err)
	}
	if result == nil || !result.Valid {
		t.Fatalf("expected valid:true when only issue is dropped; result=%v", result)
	}
	for _, e := range append(append([]contracts.ValidationIssue{}, result.Errors...), append(result.Warnings, result.Infos...)...) {
		if e.Code == "HL7_INVALID_MSH" {
			t.Fatalf("expected HL7_INVALID_MSH to be dropped; got %+v", e)
		}
	}
}

func TestHL7CustomSeverity_DropSkipsIssue(t *testing.T) {
	proc := NewHL7SchemaProcessor()
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
	msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ORU^R01|MSG001|P|2.5|EXTRA"
	result, err := proc.Process([]byte(msg), compiled, contracts.ProcessOptions{
		CollectAllErrors: true,
		CodeSeverityOverrides: map[string]contracts.Severity{
			"HL7_EXTRA_FIELD": contracts.Severity("DROP"),
		},
	})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !result.Valid {
		t.Fatalf("expected valid:true when only dropped issue exists; errors=%v warnings=%v infos=%v", result.Errors, result.Warnings, result.Infos)
	}
	for _, e := range append(append([]contracts.ValidationIssue{}, result.Errors...), append(result.Warnings, result.Infos...)...) {
		if e.Code == "HL7_EXTRA_FIELD" {
			t.Fatalf("expected HL7_EXTRA_FIELD to be dropped; got %+v", e)
		}
	}
}

func TestMatchMessage_MissingRequired_WithBlockingSegment(t *testing.T) {
	def := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": []},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": []},
			{"name": "PV1", "usage": "R", "rpt": "1", "fields": []}
		]
	}`)
	compiled, _ := ParseHL7Schema(def)
	// PID missing; GT1 appears where PID was expected — should report unexpected GT1 and missing PID/PV1.
	msg, _ := ParseMessage([]byte("MSH|^~\\&|A|B|C|D|20250101120000||ADT^A01|1|P|2.5\rGT1|1\rPV1|1"))
	match := MatchMessage(msg, &CompiledHL7Schema{Schema: compiled})
	var unexpected, missing int
	for _, e := range match.Errors {
		switch e.Code {
		case "HL7_UNEXPECTED_SEGMENT":
			if e.Path == "GT1" {
				unexpected++
			}
		case "HL7_MISSING_REQUIRED":
			missing++
		}
	}
	if unexpected != 1 {
		t.Errorf("expected 1 HL7_UNEXPECTED_SEGMENT for GT1, got errors: %v", match.Errors)
	}
	if missing < 1 {
		t.Errorf("expected at least one HL7_MISSING_REQUIRED, got errors: %v", match.Errors)
	}
}

// TestVersionsMatch verifies that the normalizeVersion helper treats "v"/"V"-prefixed
// strings and bare strings as equivalent, and that trailing ".0" segments are still
// collapsed regardless of the presence of that prefix.
func TestVersionsMatch(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"2.5.1", "v2.5.1", true},
		{"v2.5.1", "2.5.1", true},
		{"V2.5.1", "2.5.1", true},
		{"v2.5.1", "V2.5.1", true},
		{"2.5", "2.5.0", true},
		{"v2.5", "2.5.0", true},
		{"2.5.1", "2.5.2", false},
		{"v2.5.1", "v2.5.2", false},
		{"2.5.1", "2.6", false},
		{"", "", true},
		{"", "2.5", false},
		{"2.5", "", false},
		{"v", "v", false},
		{"v", "2.5", false},
	}
	for _, tc := range cases {
		got := versionsMatch(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("versionsMatch(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestStrictMode_ReturnsErrorOnInvalid verifies that Mode=STRICT makes the HL7 processor
// return a non-nil Go error (in addition to a populated ProcessResult) when the message
// is invalid — consistent with the JSON and CSV processor behavior.
func TestStrictMode_ReturnsErrorOnInvalid(t *testing.T) {
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
	// Message is missing required PID segment — should produce errors.
	msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ADT^A01|MSG001|P|2.5"

	t.Run("NORMAL mode returns nil error even when invalid", func(t *testing.T) {
		result, err := proc.Process([]byte(msg), compiled, contracts.ProcessOptions{
			Mode:             contracts.ValidationModeNormal,
			CollectAllErrors: true,
		})
		if err != nil {
			t.Errorf("NORMAL mode: expected nil error, got: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.Valid {
			t.Error("expected invalid result")
		}
	})

	t.Run("STRICT mode returns non-nil error when invalid", func(t *testing.T) {
		result, err := proc.Process([]byte(msg), compiled, contracts.ProcessOptions{
			Mode:             contracts.ValidationModeStrict,
			CollectAllErrors: true,
		})
		if err == nil {
			t.Error("STRICT mode: expected non-nil error for invalid message, got nil")
		}
		if result == nil {
			t.Error("STRICT mode: expected populated ProcessResult alongside the error")
		}
	})

	t.Run("STRICT mode returns nil error when valid", func(t *testing.T) {
		validMsg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ADT^A01|MSG001|P|2.5\rPID|||12345^^^NHS^NH"
		result, err := proc.Process([]byte(validMsg), compiled, contracts.ProcessOptions{
			Mode: contracts.ValidationModeStrict,
		})
		if err != nil {
			t.Errorf("STRICT mode with valid message: expected nil error, got: %v", err)
		}
		if result == nil || !result.Valid {
			t.Error("expected valid result")
		}
	})
}
