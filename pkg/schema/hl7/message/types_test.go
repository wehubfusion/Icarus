package message

import (
	"testing"
)

func TestMessage_Get_nilReceiver(t *testing.T) {
	var m *Message
	if got := m.Get("MSH-9"); got != "" {
		t.Errorf("nil Get = %q, want empty", got)
	}
}

func TestMessage_SegmentByName_caseInsensitive(t *testing.T) {
	m := &Message{
		Delimiters: DefaultDelimiters(),
		Segments: []Segment{
			{Name: "PID", Fields: []Field{}},
		},
	}
	seg, _ := m.SegmentByName("pid")
	if seg == nil || seg.Name != "PID" {
		t.Fatalf("SegmentByName(pid) = %v, want PID segment", seg)
	}
}

func TestComponent_FormatWithDelimiters_customSubcomponent(t *testing.T) {
	d := Delimiters{
		Field:        '|',
		Component:    '^',
		Repetition:   '~',
		Escape:       '\\',
		Subcomponent: '!',
	}
	c := Component{Subcomponents: []Subcomponent{{Value: "a"}, {Value: "b"}}}
	if got := c.FormatWithDelimiters(d); got != "a!b" {
		t.Errorf("FormatWithDelimiters = %q, want a!b", got)
	}
}

func TestRepetition_FormatWithDelimiters_customComponent(t *testing.T) {
	d := Delimiters{
		Field:        '|',
		Component:    '%',
		Repetition:   '~',
		Escape:       '\\',
		Subcomponent: '&',
	}
	r := Repetition{Components: []Component{
		{Subcomponents: []Subcomponent{{Value: "1"}}},
		{Subcomponents: []Subcomponent{{Value: "2"}}},
	}}
	if got := r.FormatWithDelimiters(d); got != "1%2" {
		t.Errorf("FormatWithDelimiters = %q, want 1%%2", got)
	}
}
