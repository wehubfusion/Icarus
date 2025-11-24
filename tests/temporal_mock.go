package tests

import (
	"context"

	"github.com/wehubfusion/Icarus/pkg/client"
)

type noopTemporalSignaler struct{}

func (noopTemporalSignaler) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, data interface{}) error {
	return nil
}

func attachNoopTemporal(c *client.Client) {
	if c != nil {
		c.SetTemporalClient(noopTemporalSignaler{})
	}
}
