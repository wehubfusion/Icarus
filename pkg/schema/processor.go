package schema

import (
	"crypto/sha256"
	"encoding/hex"
)

// SchemaProcessor is the extension point for all schema formats.
// JSON, CSV, and HL7 each implement this interface; Engine routes by format.
type SchemaProcessor interface {
	// Type identifies this processor so the registry can route to it (e.g. "JSON", "CSV", "HL7").
	Type() string

	// ParseSchema validates and compiles the raw definition bytes into an
	// opaque compiled representation. Called once per (schemaID, definition).
	ParseSchema(definition []byte) (CompiledSchema, error)

	// Process is the full pipeline: parse input, apply defaults, validate, return structured result.
	// inputData is raw bytes in the format this processor expects
	// (JSON for JSON/CSV, UTF-8 HL7 pipe-encoding for HL7).
	Process(inputData []byte, schema CompiledSchema, opts ProcessOptions) (*ProcessResult, error)
}

// CompiledSchema is the opaque result of ParseSchema.
// Each processor defines its own concrete type behind this interface.
type CompiledSchema interface {
	SchemaType() string
	ContentHash() string // hex SHA-256 of definition bytes; used as cache key
}

// contentHash returns the SHA-256 hash of b as a hex string.
func contentHash(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
