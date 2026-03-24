package celhl7

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// parseHL7DTM parses HL7 DTM/TS (partial allowed) into UTC. Missing TZ assumed UTC.
func parseHL7DTM(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	// Strip timezone offset if present (+/-HHMM at end)
	body := s
	for _, sign := range []byte{'+', '-'} {
		if idx := strings.LastIndexByte(body, sign); idx > 0 && len(body)-idx >= 5 {
			// crude: last +/- followed by 4 digits
			tail := body[idx+1:]
			if len(tail) == 4 {
				if _, err := strconv.Atoi(tail); err == nil {
					body = body[:idx]
					// TODO: apply offset; for now treat as UTC per plan
					break
				}
			}
		}
	}
	if dot := strings.IndexByte(body, '.'); dot >= 0 {
		body = body[:dot]
	}
	// Pad to at least 14 digits (YYYYMMDDHHmmss)
	digits := make([]byte, 0, 14)
	for _, c := range body {
		if c >= '0' && c <= '9' {
			digits = append(digits, byte(c))
		}
	}
	for len(digits) < 14 {
		digits = append(digits, '0')
	}
	if len(digits) > 14 {
		digits = digits[:14]
	}
	y, _ := strconv.Atoi(string(digits[0:4]))
	mo, _ := strconv.Atoi(string(digits[4:6]))
	d, _ := strconv.Atoi(string(digits[6:8]))
	h, _ := strconv.Atoi(string(digits[8:10]))
	mi, _ := strconv.Atoi(string(digits[10:12]))
	sec, _ := strconv.Atoi(string(digits[12:14]))
	if y == 0 {
		return time.Time{}, fmt.Errorf("invalid HL7 DTM value: %q", s)
	}
	return time.Date(y, time.Month(mo), d, h, mi, sec, 0, time.UTC), nil
}
