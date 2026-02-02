// Package runtime provides the embedded node processing runtime for Icarus.
//
// The runtime handles the execution of embedded nodes within execution units,
// supporting concurrent processing at both parent-level and mid-flow iteration
// with a configurable worker pool for controlling concurrency.
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
// Implements ItemProcessor for use with WorkerPool. Nodes are grouped by depth
// and processed in parallel within each depth level.
//
// WorkerPool: Manages concurrent processing with configurable worker counts.
//
// OutputResolver: Resolves field mappings from StandardUnitOutput for subsequent units.
//
// # Output Format
//
// The processor produces StandardUnitOutput as a flat map with keys:
//   - Non-iterated: "nodeId-/path" (e.g., "abc-/name")
//   - Single-level iteration: "nodeId-/path[0]" (e.g., "abc-/name[0]", "abc-/name[1]")
//   - Nested iteration: "nodeId-/path[0][1][2]" (e.g., "abc-/chapter[0][0][0]")
//
// All output is in a single flat map. Array indices in keys indicate iteration.
// This format allows for flexible nested iteration while maintaining a simple structure.
//
// # Depth-Based Parallel Processing
//
// Embedded nodes are sorted by depth and processed level by level:
//
//	Depth 0: [Node A]           → processed first
//	Depth 1: [Node B]           → waits for depth 0
//	Depth 2: [Node C]           → waits for depth 1
//	Depth 3: [Node D, E, F]     → all run in parallel (same depth = no dependencies)
//	Depth 4: [Node G, H, I]     → all run in parallel after depth 3 completes
//
// This ensures:
//   - Dependencies are respected (deeper nodes wait for shallower ones)
//   - Maximum parallelism (nodes at same depth run concurrently)
//   - Proper data flow (outputs available before dependent nodes run)
//
// # Iteration Behavior
//
// Only ONE node needs iterate:true to start iteration over an array.
// All downstream nodes automatically inherit the iteration context. Processing
// follows a per-item subflow model with depth-based parallel execution:
//
//   - Phase 1: Process depth levels until iteration starts (parallel within depth)
//   - Phase 2: For each item, process remaining depths (parallel within depth)
//
// Path notation determines how values are extracted during iteration:
//   - Array notation (/data//field): Extracts value at current index
//   - Direct path (/config/value): Passes full value (shared across iterations)
//
// Event triggers are evaluated per-item during iteration, allowing conditional
// execution of nodes based on each array item's values.
//
// # Concurrency Model
//
// Three levels of concurrent processing:
//
// 1. Parent-Level Iteration (processWithConcurrency):
//   - Parent outputs array (e.g., {data: [...]})
//   - WorkerPool processes array items concurrently
//   - Each worker runs SubflowProcessor.ProcessItem() for one item
//
// 2. Mid-Flow Iteration (processMidFlowConcurrently):
//   - Embedded node produces array mid-flow
//   - SubflowProcessor creates internal worker pool
//   - Concurrent processing of mid-flow array items
//
// 3. Depth-Level Parallelism (processDepthLevelParallel):
//   - Nodes at same depth have no dependencies on each other
//   - All nodes at a depth level run in parallel
//   - Both pre-iteration and per-item processing use this
//
// Worker counts can be configured via WorkerPoolConfig (or env overrides
// ICARUS_EMBEDDED_WORKERS / ICARUS_EMBEDDED_WORKER_MULTIPLIER).
//
// # Per-Item Subflow Processing
//
// When a node has iterate:true on a mapping from an array source:
//
//  1. Pre-iteration depth levels run (parallel within depth), outputs stored
//  2. Worker pool processes items concurrently, each item:
//     - Creates isolated itemStore inheriting pre-iteration outputs
//     - Processes remaining depth levels (parallel within depth)
//     - Each node's output stored for downstream nodes at next depth
//  3. Results merged: shared output + per-item outputs in Array field
//
// # Example Usage
//
//	// Create factory and register nodes
//	factory := runtime.NewDefaultNodeFactory()
//	factory.Register("my-plugin", MyNodeCreator)
//
//	// Create processor with a tuned worker pool
//	cfg := runtime.DefaultProcessorConfig()
//	cfg.WorkerPool.NumWorkers = 16 // or rely on ICARUS_EMBEDDED_WORKERS/ICARUS_EMBEDDED_WORKER_MULTIPLIER
//	processor := runtime.NewEmbeddedProcessor(factory, cfg)
//
//	// Process embedded nodes
//	output, err := processor.ProcessEmbeddedNodes(ctx, parentOutput, unit, priorUnitOutputs)
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
