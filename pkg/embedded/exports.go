package embedded

import "github.com/wehubfusion/Icarus/pkg/embedded/runtime"

type (
	Processor          = runtime.Processor
	NodeConfig         = runtime.NodeConfig
	NodeExecutor       = runtime.NodeExecutor
	ExecutorRegistry   = runtime.ExecutorRegistry
	EmbeddedNodeResult = runtime.EmbeddedNodeResult
	Logger             = runtime.Logger
	Field              = runtime.Field
	NoOpLogger         = runtime.NoOpLogger
	FieldMapper        = runtime.FieldMapper
	SmartStorage       = runtime.SmartStorage
	StorageEntry       = runtime.StorageEntry
	IterationContext   = runtime.IterationContext
	StandardOutput     = runtime.StandardOutput
	MetaData           = runtime.MetaData
	EventEndpoints     = runtime.EventEndpoints
	ErrorInfo          = runtime.ErrorInfo
	OutputRegistry     = runtime.OutputRegistry
)

var (
	NewExecutorRegistry          = runtime.NewExecutorRegistry
	NewProcessor                 = runtime.NewProcessor
	NewProcessorWithConfig       = runtime.NewProcessorWithConfig
	NewProcessorWithLimiter      = runtime.NewProcessorWithLimiter
	NewProcessorWithLogger       = runtime.NewProcessorWithLogger
	NewFieldMapper               = runtime.NewFieldMapper
	NewSmartStorage              = runtime.NewSmartStorage
	NewSmartStorageWithConsumers = runtime.NewSmartStorageWithConsumers
	WrapSuccess                  = runtime.WrapSuccess
	WrapError                    = runtime.WrapError
	WrapSkipped                  = runtime.WrapSkipped
	NewOutputRegistry            = runtime.NewOutputRegistry
)
