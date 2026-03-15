package schema

import "github.com/wehubfusion/Icarus/pkg/schema/contracts"

// SchemaProcessor is the extension point for all schema formats (re-exported from contracts).
type SchemaProcessor = contracts.SchemaProcessor

// CompiledSchema is the opaque result of ParseSchema (re-exported from contracts).
type CompiledSchema = contracts.CompiledSchema
