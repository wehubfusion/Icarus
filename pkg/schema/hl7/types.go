package hl7

import "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"

// Re-export message model types so pkg/schema/hl7 remains the primary import path.
// The concrete definitions live in package message to avoid import cycles with pkg/cel/hl7.

type Delimiters = message.Delimiters
type Subcomponent = message.Subcomponent
type Component = message.Component
type Repetition = message.Repetition
type Field = message.Field
type Segment = message.Segment
type Message = message.Message

// DefaultDelimiters returns standard HL7 delimiters.
func DefaultDelimiters() message.Delimiters { return message.DefaultDelimiters() }

// LocationParts parses an HL7 location (e.g. "MSH-12", "PID-3(2).1") into segment name and 1-based indices.
func LocationParts(loc string) (seg string, field, rep, comp, sub int) {
	return message.LocationParts(loc)
}

// FieldValueOnSegment returns the value at HL7 location for a specific segment instance.
func FieldValueOnSegment(seg *message.Segment, location string) string {
	return message.FieldValueOnSegment(seg, location)
}
