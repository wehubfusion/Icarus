package celhl7

import (
	"sync"

	icel "github.com/wehubfusion/Icarus/pkg/cel"
)

var (
	hl7EngineOnce sync.Once
	hl7Engine     *icel.Engine
	hl7EngineErr  error
)

// Engine returns the singleton HL7 CEL engine (stdlib + HL7 late-bound functions).
func Engine() (*icel.Engine, error) {
	hl7EngineOnce.Do(func() {
		hl7Engine, hl7EngineErr = NewHL7Engine()
	})
	return hl7Engine, hl7EngineErr
}
