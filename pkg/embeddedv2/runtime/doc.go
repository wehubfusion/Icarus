// Package runtime provides the embedded node processing runtime for Icarus.
//
// The runtime handles the execution of embedded nodes within execution units,
// supporting both sequential and concurrent processing with integration to
// the concurrency package's Limiter for rate limiting.
//
// # Key Components
//
// EmbeddedProcessor: The main processor that handles embedded nodes within a unit.
// It supports array iteration with concurrent processing using a worker pool.
//
// EmbeddedNode: The interface all embedded nodes must implement. Each node
// has a single Process method that takes ProcessInput and returns ProcessOutput.
//
// SubflowProcessor: Processes embedded nodes for a single item (array element).
// Implements ItemProcessor for use with WorkerPool.
//
// WorkerPool: Manages concurrent processing with integration to the limiter.
//
// OutputResolver: Resolves field mappings from StandardUnitOutput for subsequent units.
//
// # Output Format
//
// The processor produces StandardUnitOutput with flattened keys:
//   - Single objects: "nodeId-/path" (e.g., "abc-/name")
//   - Array items: "nodeId-/arrayPath//field" (e.g., "abc-/data//name")
//
// # Example Usage
//
//	// Create factory and register nodes
//	factory := runtime.NewDefaultNodeFactory()
//	factory.Register("my-plugin", MyNodeCreator)
//
//	// Create processor with limiter
//	limiter := concurrency.NewLimiter(10)
//	processor := runtime.NewEmbeddedProcessorWithLimiter(factory, limiter)
//
//	// Process embedded nodes
//	output, err := processor.ProcessEmbeddedNodes(ctx, parentOutput, unit)
//
// # Implementing Custom Nodes
//
// Embed BaseNode in your custom node implementation:
//
//	type MyNode struct {
//	    runtime.BaseNode
//	}
//
//	func NewMyNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
//	    return &MyNode{
//	        BaseNode: runtime.NewBaseNode(config),
//	    }, nil
//	}
//
//	func (n *MyNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
//	    // Your processing logic here
//	    return runtime.SuccessOutput(map[string]interface{}{
//	        "result": "value",
//	    })
//	}
package runtime
