package runtime

import (
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/logging"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/mapping"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/output"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/storage"
)

type (
	// Logging aliases
	Logger     = logging.Logger
	Field      = logging.Field
	NoOpLogger = logging.NoOpLogger

	// Mapping aliases
	FieldMapper = mapping.FieldMapper

	// Output aliases
	StandardOutput = output.StandardOutput
	MetaData       = output.MetaData
	EventEndpoints = output.EventEndpoints
	ErrorInfo      = output.ErrorInfo
	OutputRegistry = output.OutputRegistry

	// Storage aliases
	SmartStorage     = storage.SmartStorage
	StorageEntry     = storage.StorageEntry
	IterationContext = storage.IterationContext
)

var (
	NewFieldMapper               = mapping.NewFieldMapper
	NewSmartStorage              = storage.NewSmartStorage
	NewSmartStorageWithConsumers = storage.NewSmartStorageWithConsumers
	WrapSuccess                  = output.WrapSuccess
	WrapError                    = output.WrapError
	WrapSkipped                  = output.WrapSkipped
	NewOutputRegistry            = output.NewOutputRegistry
)
