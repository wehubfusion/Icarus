package hl7

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrEmptyMessage      = errors.New("hl7: empty message")
	ErrInvalidMSH        = errors.New("hl7: message must start with MSH segment")
	ErrInvalidDelimiters = errors.New("hl7: invalid or missing delimiters in MSH segment")
)

// UTF-8 BOM is the 3-byte sequence 0xEF, 0xBB, 0xBF.
var bomUTF8 = []byte{0xEF, 0xBB, 0xBF}

// Tokenize normalizes the raw message (BOM strip, line endings to \r), extracts delimiters from MSH,
// and returns delimiters plus segment strings. Call ParseMessageWithDelimiters with the result to get a Message.
func Tokenize(raw []byte) (Delimiters, []string, error) {
	s := string(raw)
	s = strings.ReplaceAll(s, "\r\n", "\r")
	s = strings.ReplaceAll(s, "\n", "\r")
	// Strip BOM first (before trimming leading whitespace).
	if len(s) >= 3 && s[0] == bomUTF8[0] && s[1] == bomUTF8[1] && s[2] == bomUTF8[2] {
		s = s[3:]
	}
	// Only strip leading and trailing CR/whitespace from the overall message.
	// Using TrimLeft so trailing spaces in the last field of the last segment are not lost.
	s = strings.TrimLeft(s, " \t\r\n")
	// Trim trailing CR/whitespace-only lines (e.g. a trailing \r at end of file) but not field data.
	// Walk from the end removing pure whitespace/CR until we hit non-whitespace.
	for len(s) > 0 && (s[len(s)-1] == '\r' || s[len(s)-1] == '\n' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		// Only trim if it's CR/LF; spaces at end belong to the last field — stop at first non-CR.
		if s[len(s)-1] == '\r' || s[len(s)-1] == '\n' {
			s = s[:len(s)-1]
		} else {
			break
		}
	}
	if len(s) == 0 {
		return Delimiters{}, nil, ErrEmptyMessage
	}
	if len(s) < 8 || s[0:3] != "MSH" {
		return Delimiters{}, nil, ErrInvalidMSH
	}
	d, err := parseDelimiters(s)
	if err != nil {
		return Delimiters{}, nil, err
	}
	segStrs := strings.Split(s, "\r")
	var out []string
	for _, seg := range segStrs {
		// Only strip leading whitespace artefacts (e.g. stray whitespace before segment name).
		// Do NOT trim trailing spaces — they are meaningful in HL7 field values.
		seg = strings.TrimLeft(seg, " \t")
		if seg != "" {
			out = append(out, seg)
		}
	}
	return d, out, nil
}

// ParseMessage parses raw HL7 message bytes into a Message.
func ParseMessage(raw []byte) (*Message, error) {
	d, segStrs, err := Tokenize(raw)
	if err != nil {
		return nil, err
	}
	return ParseMessageWithDelimiters(d, segStrs)
}

// ParseMessageWithDelimiters builds a Message from already-tokenized delimiters and segment strings.
func ParseMessageWithDelimiters(d Delimiters, segStrs []string) (*Message, error) {
	msg := &Message{Delimiters: d}
	for _, segStr := range segStrs {
		seg, err := parseSegment(segStr, d)
		if err != nil {
			return nil, fmt.Errorf("hl7: parse segment: %w", err)
		}
		msg.Segments = append(msg.Segments, seg)
	}
	if len(msg.Segments) > 0 {
		msg.CharacterSet = msg.Get("MSH-18")
	}
	return msg, nil
}

func parseDelimiters(data string) (Delimiters, error) {
	if len(data) < 8 {
		return Delimiters{}, ErrInvalidDelimiters
	}
	d := Delimiters{
		Field:        data[3],
		Component:    data[4],
		Repetition:   data[5],
		Escape:       data[6],
		Subcomponent: data[7],
	}
	if len(data) >= 9 && data[8] != d.Field {
		d.Truncation = data[8]
	}
	// HL7 §2.5.1 requires all delimiter characters to be distinct from one another.
	// Colliding delimiters (e.g. Component == Field) cause silent misparses that are
	// very hard to diagnose downstream. Reject them here. (BUG-25)
	chars := []struct {
		name  string
		value byte
	}{
		{"field", d.Field},
		{"component", d.Component},
		{"repetition", d.Repetition},
		{"escape", d.Escape},
		{"subcomponent", d.Subcomponent},
	}
	if d.Truncation != 0 {
		chars = append(chars, struct {
			name  string
			value byte
		}{"truncation", d.Truncation})
	}
	seen := make(map[byte]string, len(chars))
	for _, c := range chars {
		if prev, dup := seen[c.value]; dup {
			return Delimiters{}, fmt.Errorf("hl7: delimiter %q (0x%02X) used for both %s and %s", c.value, c.value, prev, c.name)
		}
		seen[c.value] = c.name
	}
	return d, nil
}

func parseSegment(segStr string, d Delimiters) (Segment, error) {
	if len(segStr) < 3 {
		return Segment{}, fmt.Errorf("segment too short: %s", segStr)
	}
	seg := Segment{Name: segStr[0:3]}
	if seg.Name == "MSH" {
		return parseMSHSegment(segStr, d)
	}
	if len(segStr) <= 3 {
		return seg, nil
	}
	for _, fieldStr := range strings.Split(segStr[4:], string(d.Field)) {
		seg.Fields = append(seg.Fields, parseField(fieldStr, d))
	}
	return seg, nil
}

func parseMSHSegment(segStr string, d Delimiters) (Segment, error) {
	seg := Segment{Name: "MSH"}
	seg.Fields = append(seg.Fields, Field{
		Repetitions: []Repetition{{
			Components: []Component{{
				Subcomponents: []Subcomponent{{Value: string(d.Field)}},
			}},
		}},
	})
	var encChars string
	if d.HasTruncation() {
		encChars = d.EncodingCharactersWithTruncation()
	} else {
		encChars = d.EncodingCharacters()
	}
	seg.Fields = append(seg.Fields, Field{
		Repetitions: []Repetition{{
			Components: []Component{{
				Subcomponents: []Subcomponent{{Value: encChars}},
			}},
		}},
	})
	headerLen := 4 + len(encChars)
	if len(segStr) > headerLen {
		for _, fieldStr := range strings.Split(segStr[headerLen+1:], string(d.Field)) {
			seg.Fields = append(seg.Fields, parseField(fieldStr, d))
		}
	}
	return seg, nil
}

func parseField(fieldStr string, d Delimiters) Field {
	if fieldStr == "" {
		return Field{
			Repetitions: []Repetition{{
				Components: []Component{{
					Subcomponents: []Subcomponent{{Value: ""}},
				}},
			}},
		}
	}
	var f Field
	for _, repStr := range splitWithEscape(fieldStr, d.Repetition, d.Escape) {
		f.Repetitions = append(f.Repetitions, parseRepetition(repStr, d))
	}
	return f
}

func parseRepetition(repStr string, d Delimiters) Repetition {
	var r Repetition
	for _, compStr := range splitWithEscape(repStr, d.Component, d.Escape) {
		r.Components = append(r.Components, parseComponent(compStr, d))
	}
	return r
}

func parseComponent(compStr string, d Delimiters) Component {
	var c Component
	for _, subStr := range splitWithEscape(compStr, d.Subcomponent, d.Escape) {
		c.Subcomponents = append(c.Subcomponents, Subcomponent{Value: unescape(subStr, d)})
	}
	return c
}

func splitWithEscape(s string, delim, escape byte) []string {
	var result []string
	var current strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == escape && i+1 < len(s) {
			current.WriteByte(s[i])
			i++
			if i < len(s) {
				current.WriteByte(s[i])
				i++
				for i < len(s) && s[i] != escape && s[i] != delim {
					current.WriteByte(s[i])
					i++
				}
				if i < len(s) && s[i] == escape {
					current.WriteByte(s[i])
					i++
				}
			}
		} else if s[i] == delim {
			result = append(result, current.String())
			current.Reset()
			i++
		} else {
			current.WriteByte(s[i])
			i++
		}
	}
	result = append(result, current.String())
	return result
}

func unescape(s string, d Delimiters) string {
	if !strings.ContainsRune(s, rune(d.Escape)) {
		return s
	}
	var result strings.Builder
	esc := string(d.Escape)
	i := 0
	for i < len(s) {
		if s[i] == d.Escape {
			end := strings.Index(s[i+1:], esc)
			if end == -1 {
				result.WriteByte(s[i])
				i++
				continue
			}
			seq := s[i+1 : i+1+end]
			if replacement, ok := decodeEscapeSequence(seq, d); ok {
				result.WriteString(replacement)
				i = i + 1 + end + 1
			} else {
				result.WriteByte(s[i])
				i++
			}
		} else {
			result.WriteByte(s[i])
			i++
		}
	}
	return result.String()
}

func decodeEscapeSequence(seq string, d Delimiters) (string, bool) {
	switch seq {
	case "F":
		return string(d.Field), true
	case "S":
		return string(d.Component), true
	case "T":
		return string(d.Subcomponent), true
	case "R":
		return string(d.Repetition), true
	case "E":
		return string(d.Escape), true
	case ".br", ".sp":
		return "\n", true
	case ".ce", ".sk", ".fi", ".nf", ".in", ".ti", "H", "N", "C", "M":
		return "", true
	default:
		if len(seq) >= 2 && (seq[0] == 'X' || seq[0] == 'x') {
			return decodeHexSequence(seq[1:]), true
		}
		if len(seq) >= 3 && seq[0] == '.' {
			if strings.HasPrefix(seq, ".sp") {
				if n, err := strconv.Atoi(seq[3:]); err == nil && n > 0 {
					return strings.Repeat("\n", n), true
				}
				return "\n", true
			}
			if strings.HasPrefix(seq, ".in") {
				return "", true
			}
		}
	}
	return "", false
}

// decodeHexSequence decodes a \Xhhhh\ HL7 escape sequence.
// Per HL7 §2.7.1, \Xhhhh...\ encodes Unicode code points in hex.
// If the hex string is 4+ digits (e.g. "0041" for U+0041 'A'), it is parsed as a single
// Unicode code point and encoded as UTF-8. For short (1-2 digit) hex, it decodes as a byte.
func decodeHexSequence(hexStr string) string {
	// Try as a single Unicode code point first (covers \X0041\, \X00E9\, \X1F600\, etc.)
	if val, err := strconv.ParseInt(hexStr, 16, 32); err == nil {
		return string(rune(val))
	}
	// Fallback: decode as pairs of hex bytes (legacy behaviour for malformed sequences)
	var result strings.Builder
	for i := 0; i+1 < len(hexStr); i += 2 {
		if val, err := strconv.ParseInt(hexStr[i:i+2], 16, 32); err == nil {
			result.WriteByte(byte(val))
		}
	}
	return result.String()
}
