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
// Implements ItemProcessor for use with WorkerPool. Uses a two-phase approach:
// Phase 1 processes pre-iteration nodes, Phase 2 processes the entire subflow
// per-item for proper iteration handling.
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
// The StandardUnitOutput has two fields:
//   - Single: Contains pre-iteration outputs (shared across iterations)
//   - Array: Contains per-iteration outputs (one entry per array item)
//
// # Iteration Behavior
//
// Only ONE node needs iterate:true to start iteration over an array.
// All downstream nodes automatically inherit the iteration context. Processing
// follows a per-item subflow model:
//
//   - Phase 1: Pre-iteration nodes run once (before any iterate:true mapping)
//   - Phase 2: For each array item, ALL remaining nodes run in sequence
//
// This ensures Item[0] flows through all subflow nodes, then Item[1], etc.
// rather than processing all items at each node sequentially.
//
// Path notation determines how values are extracted during iteration:
//   - Array notation (/data//field): Extracts value at current index
//   - Direct path (/config/value): Passes full value (shared across iterations)
//
// Event triggers are evaluated per-item during iteration, allowing conditional
// execution of nodes based on each array item's values.
//
// # Per-Item Subflow Processing
//
// When a node has iterate:true on a mapping from an array source:
//
//  1. Pre-iteration nodes (before iterate:true) run once, output stored
//  2. For itemIndex := 0 to len(array):
//     - Create isolated itemStore inheriting pre-iteration outputs
//     - Store current array item in itemStore
//     - Process ALL remaining nodes for this item
//     - Each node's output stored in itemStore for downstream nodes
//  3. Results merged: shared output + per-item outputs in Array field
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
