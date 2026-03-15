package hl7

import (
	"encoding/json"
	"testing"
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
	errs := ValidateMatchResult(match, msg, true, false)
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
