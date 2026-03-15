package hl7

import (
	"testing"
)

func TestTokenize_ValidMSH(t *testing.T) {
	raw := []byte("MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ADT^A01|MSG001|P|2.5\rPID|||12345")
	d, segs, err := Tokenize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if d.Field != '|' || d.Component != '^' || d.Repetition != '~' || d.Escape != '\\' || d.Subcomponent != '&' {
		t.Errorf("unexpected delimiters: %c %c %c %c %c", d.Field, d.Component, d.Repetition, d.Escape, d.Subcomponent)
	}
	if len(segs) != 2 {
		t.Errorf("expected 2 segments, got %d", len(segs))
	}
	if segs[0][:3] != "MSH" || segs[1][:3] != "PID" {
		t.Errorf("unexpected segment names: %q, %q", segs[0][:3], segs[1][:3])
	}
}

func TestTokenize_EmptyMessage(t *testing.T) {
	_, _, err := Tokenize([]byte(""))
	if err != ErrEmptyMessage {
		t.Errorf("expected ErrEmptyMessage, got %v", err)
	}
}

func TestTokenize_InvalidMSH(t *testing.T) {
	_, _, err := Tokenize([]byte("PID|||123"))
	if err != ErrInvalidMSH {
		t.Errorf("expected ErrInvalidMSH, got %v", err)
	}
}

func TestParseMessage_Get(t *testing.T) {
	raw := []byte("MSH|^~\\&|SEND|FAC|RECV|FAC|20250305120000||ADT^A01|MSG001|P|2.5\rPID|||12345^^^NHS^NH")
	msg, err := ParseMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if v := msg.Get("MSH-9"); v != "ADT^A01" {
		t.Errorf("MSH-9: got %q", v)
	}
	if v := msg.Get("MSH-12"); v != "2.5" {
		t.Errorf("MSH-12: got %q", v)
	}
	if v := msg.Get("PID-3"); v != "12345^^^NHS^NH" {
		t.Errorf("PID-3: got %q", v)
	}
}
