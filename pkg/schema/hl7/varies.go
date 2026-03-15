package hl7

import "sync"

// VariesResolver resolves the effective datatype for a VARIES field (e.g. OBX-5 from OBX-2).
// The registry allows adding resolvers for other segment/field combinations in the future.
// seg is the current segment being validated (e.g. the specific OBX segment), so multi-segment
// messages resolve type from the correct segment instance, not the first match.
type VariesResolver interface {
	// ResolveDataType returns the effective datatype for the given segment/field using the current segment and message.
	// Returns empty string if unknown (validation may then skip or treat as string).
	ResolveDataType(segmentName string, fieldPosition string, seg *Segment, msg *Message) string
}

// VariesResolverFunc adapts a function to VariesResolver.
type VariesResolverFunc func(segmentName, fieldPosition string, seg *Segment, msg *Message) string

func (f VariesResolverFunc) ResolveDataType(segName, field string, seg *Segment, msg *Message) string {
	return f(segName, field, seg, msg)
}

// VariesRegistry holds resolvers keyed by "segment-field" (e.g. "OBX-5").
var (
	variesRegistry   = make(map[string]VariesResolver)
	variesRegistryMu sync.RWMutex
)

// RegisterVariesResolver registers a resolver for the given segment and field (e.g. "OBX", "5" for OBX-5).
func RegisterVariesResolver(segment, field string, r VariesResolver) {
	key := segment + "-" + field
	variesRegistryMu.Lock()
	defer variesRegistryMu.Unlock()
	variesRegistry[key] = r
}

// ResolveVariesDataType returns the effective datatype for a VARIES field, or "" if no resolver.
// seg is the current segment being validated (used e.g. so OBX-5 reads OBX-2 from the same OBX segment).
func ResolveVariesDataType(segmentName, fieldPosition string, seg *Segment, msg *Message) string {
	key := segmentName + "-" + fieldPosition
	variesRegistryMu.RLock()
	r, ok := variesRegistry[key]
	variesRegistryMu.RUnlock()
	if !ok {
		return ""
	}
	return r.ResolveDataType(segmentName, fieldPosition, seg, msg)
}

func init() {
	// OBX-5: value type is given in OBX-2 (Observation Value Type) of the same OBX segment.
	// Use the passed segment so multiple OBX segments each use their own OBX-2.
	RegisterVariesResolver("OBX", "5", VariesResolverFunc(func(segmentName, fieldPosition string, seg *Segment, msg *Message) string {
		if seg == nil {
			return ""
		}
		f, ok := seg.FieldAt(2)
		if !ok {
			return ""
		}
		return f.String()
	}))
}
