package message

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Workflow represents workflow execution information
type Workflow struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

// Node represents node information within a workflow
type Node struct {
	NodeID        string      `json:"nodeId"`
	Configuration interface{} `json:"configuration"`
}

// BlobReference contains information for fetching data from blob storage.
// When a payload is too large to send inline (>1.5MB), it is uploaded to Azure Blob Storage
// and a BlobReference is included instead of the raw data.
type BlobReference struct {
	URL       string `json:"url"`       // Direct blob URL (for metadata/logging)
	SizeBytes int    `json:"sizeBytes"` // Original data size in bytes
}

// Payload represents the message payload data
type Payload struct {
	Source        string         `json:"source"`
	Data          string         `json:"data"` // Inline data (for small payloads)
	Reference     string         `json:"reference"`
	BlobReference *BlobReference `json:"blobReference,omitempty"` // Reference to blob storage (for large payloads)
	FieldMappings []FieldMapping `json:"fieldMappings,omitempty"` // Field mappings for extracting data from blob
}

// Output represents output destination information
type Output struct {
	DestinationType string `json:"destinationType"`
}

// FieldMapping represents field mapping between nodes
type FieldMapping struct {
	SourceNodeID         string   `json:"sourceNodeId"`
	SourceEndpoint       string   `json:"sourceEndpoint"`
	DestinationEndpoints []string `json:"destinationEndpoints"`
	DataType             string   `json:"dataType"`
	Iterate              bool     `json:"iterate"`
	IsEventTrigger       bool     `json:"isEventTrigger,omitempty"` // True when this mapping is an event trigger (conditional execution)
}

// ConnectionDetails represents connection information
type ConnectionDetails struct {
	ConnectionID string          `json:"connectionId"`
	Type         string          `json:"type"`
	Config       json.RawMessage `json:"config"`
}

// SchemaDetails represents schema information
type SchemaDetails struct {
	SchemaID string          `json:"schemaId"`
	Name     string          `json:"name"`
	Fields   json.RawMessage `json:"fields"`
}

// EmbeddedNode represents a node to be executed within a parent node
type EmbeddedNode struct {
	NodeID         string             `json:"nodeId"`
	PluginType     string             `json:"pluginType"`
	Configuration  json.RawMessage    `json:"configuration"`
	Depth          int                `json:"depth"` // Dependency depth within the embedded context
	ExecutionOrder int                `json:"executionOrder"`
	FieldMappings  []FieldMapping     `json:"fieldMappings,omitempty"`
	Connection     *ConnectionDetails `json:"connection,omitempty"`
	Schema         *SchemaDetails     `json:"schema,omitempty"`
}

// Message represents a structured message that can be sent over JetStream.
// All messages are serialized to JSON for transmission and include timestamps.
// Messages published to JetStream are persisted according to the stream's configuration.
type Message struct {
	// CorrelationID is a unique identifier for tracking related messages across the system
	CorrelationID string `json:"correlationId,omitempty"`

	// Workflow contains workflow execution information
	Workflow *Workflow `json:"workflow,omitempty"`

	// Node contains node information within the workflow
	Node *Node `json:"node,omitempty"`

	// Payload contains the message data
	Payload *Payload `json:"payload,omitempty"`

	// Output contains output destination information
	Output *Output `json:"output,omitempty"`

	// Metadata holds additional key-value pairs for the message
	Metadata map[string]string `json:"metadata,omitempty"`

	// EmbeddedNodes contains child nodes to be executed within the parent node
	EmbeddedNodes []EmbeddedNode `json:"embeddedNodes,omitempty"`

	// Connection contains connection details for the parent node
	Connection *ConnectionDetails `json:"connection,omitempty"`

	// Schema contains schema details for the parent node
	Schema *SchemaDetails `json:"schema,omitempty"`

	// CreatedAt is the timestamp when the message was created
	CreatedAt string `json:"createdAt"`

	// UpdatedAt is the timestamp when the message was last updated
	UpdatedAt string `json:"updatedAt"`

	// natsMsg holds the original NATS message for acknowledgment (not serialized)
	natsMsg *nats.Msg `json:"-"`
}

// NewMessage creates a new message with timestamps
func NewMessage() *Message {
	now := time.Now().Format(time.RFC3339)
	return &Message{
		Metadata:  make(map[string]string),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// NewWorkflowMessage creates a new message with workflow information
func NewWorkflowMessage(workflowID, runID string) *Message {
	now := time.Now().Format(time.RFC3339)
	return &Message{
		Workflow: &Workflow{
			WorkflowID: workflowID,
			RunID:      runID,
		},
		Metadata:  make(map[string]string),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// WithCorrelationID sets the correlation ID for the message
func (m *Message) WithCorrelationID(correlationID string) *Message {
	m.CorrelationID = correlationID
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// WithMetadata adds metadata to the message
func (m *Message) WithMetadata(key, value string) *Message {
	if m.Metadata == nil {
		m.Metadata = make(map[string]string)
	}
	m.Metadata[key] = value
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// WithNode adds node information to the message
func (m *Message) WithNode(nodeID string, configuration interface{}) *Message {
	m.Node = &Node{
		NodeID:        nodeID,
		Configuration: configuration,
	}
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// WithPayload adds payload information to the message
func (m *Message) WithPayload(source, data, reference string) *Message {
	m.Payload = &Payload{
		Source:    source,
		Data:      data,
		Reference: reference,
	}
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// WithOutput adds output information to the message
func (m *Message) WithOutput(destinationType string) *Message {
	m.Output = &Output{
		DestinationType: destinationType,
	}
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// WithEmbeddedNodes adds embedded nodes to the message
func (m *Message) WithEmbeddedNodes(nodes []EmbeddedNode) *Message {
	m.EmbeddedNodes = nodes
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// WithConnection adds connection details to the message
func (m *Message) WithConnection(connection *ConnectionDetails) *Message {
	m.Connection = connection
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// WithSchema adds schema details to the message
func (m *Message) WithSchema(schema *SchemaDetails) *Message {
	m.Schema = schema
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// UpdateTimestamp updates the UpdatedAt timestamp to current time
func (m *Message) UpdateTimestamp() *Message {
	m.UpdatedAt = time.Now().Format(time.RFC3339)
	return m
}

// ToBytes serializes the message to JSON bytes
func (m *Message) ToBytes() ([]byte, error) {
	return json.Marshal(m)
}

// FromBytes deserializes a message from JSON bytes
func FromBytes(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// FromNATSMsg converts a NATS message to an SDK Message
func FromNATSMsg(natsMsg *nats.Msg) (*Message, error) {
	return FromBytes(natsMsg.Data)
}

// NATSMsg represents a JetStream message with additional metadata and acknowledgment methods.
// This wrapper provides access to JetStream-specific operations like Ack/Nak.
// Handlers MUST call Ack() or Nak() to indicate successful or failed processing.
type NATSMsg struct {
	// Message is the SDK message
	*Message

	// Subject is the JetStream subject the message was received on
	Subject string

	// Reply is the reply subject (if applicable)
	Reply string

	// natsMsg is the underlying NATS message for JetStream operations
	natsMsg *nats.Msg
}

// Ack acknowledges the message to JetStream, indicating successful processing.
// The message will not be redelivered after acknowledgment.
// Handlers should call this after successfully processing a message.
func (m *NATSMsg) Ack() error {
	if m.natsMsg == nil || m.natsMsg.Reply == "" {
		return nil
	}
	return m.natsMsg.Ack()
}

// Nak negatively acknowledges the message to JetStream, indicating processing failure.
// The message will be redelivered according to the consumer's configuration.
// Handlers should call this when processing fails and the message should be retried.
func (m *NATSMsg) Nak() error {
	if m.natsMsg == nil || m.natsMsg.Reply == "" {
		return nil
	}
	return m.natsMsg.Nak()
}

// InProgress indicates to JetStream that the message is still being processed.
// This extends the acknowledgment deadline to prevent redelivery.
// Use this for long-running message processing to avoid timeout-based redelivery.
func (m *NATSMsg) InProgress() error {
	if m.natsMsg == nil || m.natsMsg.Reply == "" {
		return nil
	}
	return m.natsMsg.InProgress()
}

// Term terminates delivery of the message to JetStream, removing it from the stream.
// Use this when a message cannot be processed and should not be retried.
func (m *NATSMsg) Term() error {
	if m.natsMsg == nil || m.natsMsg.Reply == "" {
		return nil
	}
	return m.natsMsg.Term()
}

// Respond sends a response message back to the reply subject.
// This is used in request-reply patterns where the sender expects a response.
func (m *NATSMsg) Respond(response *Message) error {
	if m.natsMsg == nil || m.Reply == "" {
		return nil
	}

	data, err := response.ToBytes()
	if err != nil {
		return err
	}

	return m.natsMsg.Respond(data)
}

// Ack acknowledges the message, indicating successful processing.
// This tells NATS that the message has been processed and should not be redelivered.
func (m *Message) Ack() error {
	if m.natsMsg == nil {
		return nil // No NATS message to acknowledge
	}
	return m.natsMsg.Ack()
}

// Nak negatively acknowledges the message, indicating processing failure.
// This tells NATS that the message processing failed and it may be redelivered.
func (m *Message) Nak() error {
	if m.natsMsg == nil {
		return nil // No NATS message to nak
	}
	return m.natsMsg.Nak()
}

// Term terminates the message, indicating it should not be redelivered.
// Use this when a message cannot be processed and should not be retried.
func (m *Message) Term() error {
	if m.natsMsg == nil {
		return nil // No NATS message to terminate
	}
	return m.natsMsg.Term()
}

// GetNATSMsg returns the underlying NATS message for acknowledgment purposes.
// Returns nil if this message was not created from a NATS message.
func (m *Message) GetNATSMsg() *nats.Msg {
	return m.natsMsg
}

// ValidateEmbeddedNodes validates the embedded nodes structure
func (m *Message) ValidateEmbeddedNodes() error {
	if len(m.EmbeddedNodes) == 0 {
		return nil // No embedded nodes is valid
	}

	// Check for duplicate node IDs
	seen := make(map[string]bool)
	for _, node := range m.EmbeddedNodes {
		if node.NodeID == "" {
			return fmt.Errorf("embedded node missing NodeID")
		}
		if node.PluginType == "" {
			return fmt.Errorf("embedded node %s missing PluginType", node.NodeID)
		}
		if seen[node.NodeID] {
			return fmt.Errorf("duplicate embedded node ID: %s", node.NodeID)
		}
		seen[node.NodeID] = true
	}

	// Validate execution order sequence
	for _, node := range m.EmbeddedNodes {
		if node.ExecutionOrder < 0 {
			return fmt.Errorf("embedded node %s has negative execution order", node.NodeID)
		}
	}

	// Validate field mappings reference valid nodes
	for _, node := range m.EmbeddedNodes {
		for _, mapping := range node.FieldMappings {
			if mapping.SourceNodeID != "" && mapping.SourceNodeID != m.Node.NodeID {
				// Check if source is another embedded node
				found := false
				for _, other := range m.EmbeddedNodes {
					if other.NodeID == mapping.SourceNodeID {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("field mapping references unknown source node: %s", mapping.SourceNodeID)
				}
			}
		}
	}

	return nil
}

// HasEmbeddedNodes returns true if this message represents a unit (has embedded nodes)
func (m *Message) HasEmbeddedNodes() bool {
	return len(m.EmbeddedNodes) > 0
}

// GetEmbeddedNodeByID returns an embedded node by its ID
func (m *Message) GetEmbeddedNodeByID(nodeID string) *EmbeddedNode {
	for i := range m.EmbeddedNodes {
		if m.EmbeddedNodes[i].NodeID == nodeID {
			return &m.EmbeddedNodes[i]
		}
	}
	return nil
}

// GetEmbeddedNodesByOrder returns embedded nodes sorted by execution order
func (m *Message) GetEmbeddedNodesByOrder() []EmbeddedNode {
	if len(m.EmbeddedNodes) == 0 {
		return nil
	}

	nodes := make([]EmbeddedNode, len(m.EmbeddedNodes))
	copy(nodes, m.EmbeddedNodes)

	// Sort by execution order
	for i := 0; i < len(nodes)-1; i++ {
		for j := i + 1; j < len(nodes); j++ {
			if nodes[i].ExecutionOrder > nodes[j].ExecutionOrder {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}

	return nodes
}

// HasConnection returns true if the message has connection details
func (m *Message) HasConnection() bool {
	return m.Connection != nil && m.Connection.ConnectionID != ""
}

// HasSchema returns true if the message has schema details
func (m *Message) HasSchema() bool {
	return m.Schema != nil && m.Schema.SchemaID != ""
}

// ResultMessage represents a unit execution result published to JetStream.
// This is a dedicated structure for result reporting, separate from execution request messages.
type ResultMessage struct {
	// Correlation ID for tracking related messages across the system
	CorrelationID string `json:"correlation_id,omitempty"`

	// Execution metadata
	ExecutionID string `json:"execution_id"` // Unique identifier for this execution
	WorkflowID  string `json:"workflow_id"`  // Workflow identifier
	RunID       string `json:"run_id"`       // Workflow run identifier
	NodeID      string `json:"node_id"`      // Node identifier

	// Execution status
	Status string `json:"status"` // "success", "failed", "skipped"

	// Result data - one of these will be populated based on result size
	InlineResult  json.RawMessage `json:"inline_result,omitempty"`  // Full result data for small results (<1.5MB)
	BlobReference *BlobReference  `json:"blob_reference,omitempty"` // Blob reference for large results (>1.5MB)

	// Error information (only present when status is "failed")
	Error *ResultError `json:"error,omitempty"`

	// Metadata
	PluginType      string `json:"plugin_type,omitempty"`       // Plugin type that processed the node
	ExecutionTimeMs int64  `json:"execution_time_ms,omitempty"` // Execution duration in milliseconds
	ResultSize      int    `json:"result_size,omitempty"`       // Size of result in bytes

	// Timestamps
	Timestamp time.Time `json:"timestamp"`  // When the result was generated
	CreatedAt string    `json:"created_at"` // ISO 8601 timestamp when message was created
	UpdatedAt string    `json:"updated_at"` // ISO 8601 timestamp when message was last updated
}

// ResultError contains error information for failed executions
type ResultError struct {
	Code      string `json:"code"`           // Error code (e.g., "INTERNAL_ERROR", "VALIDATION_ERROR")
	Message   string `json:"message"`        // Human-readable error message
	Retryable bool   `json:"retryable"`      // Whether the error is retryable (transient vs permanent)
	Type      string `json:"type,omitempty"` // Error type (e.g., "internal", "bad_request", "not_found")
}

// NewResultMessage creates a new result message with timestamps
func NewResultMessage(executionID, workflowID, runID, nodeID, status string) *ResultMessage {
	now := time.Now()
	return &ResultMessage{
		ExecutionID: executionID,
		WorkflowID:  workflowID,
		RunID:       runID,
		NodeID:      nodeID,
		Status:      status,
		Timestamp:   now,
		CreatedAt:   now.Format(time.RFC3339),
		UpdatedAt:   now.Format(time.RFC3339),
	}
}

// WithCorrelationID sets the correlation ID for the result message
func (r *ResultMessage) WithCorrelationID(correlationID string) *ResultMessage {
	r.CorrelationID = correlationID
	r.UpdatedAt = time.Now().Format(time.RFC3339)
	return r
}

// WithInlineResult sets the inline result data
func (r *ResultMessage) WithInlineResult(result json.RawMessage) *ResultMessage {
	r.InlineResult = result
	r.ResultSize = len(result)
	r.UpdatedAt = time.Now().Format(time.RFC3339)
	return r
}

// WithBlobReference sets the blob reference for large results
func (r *ResultMessage) WithBlobReference(blobRef *BlobReference) *ResultMessage {
	r.BlobReference = blobRef
	r.UpdatedAt = time.Now().Format(time.RFC3339)
	return r
}

// WithError sets the error information
func (r *ResultMessage) WithError(err *ResultError) *ResultMessage {
	r.Error = err
	r.Status = "failed"
	r.UpdatedAt = time.Now().Format(time.RFC3339)
	return r
}

// WithPluginType sets the plugin type
func (r *ResultMessage) WithPluginType(pluginType string) *ResultMessage {
	r.PluginType = pluginType
	r.UpdatedAt = time.Now().Format(time.RFC3339)
	return r
}

// WithExecutionTime sets the execution time in milliseconds
func (r *ResultMessage) WithExecutionTime(ms int64) *ResultMessage {
	r.ExecutionTimeMs = ms
	r.UpdatedAt = time.Now().Format(time.RFC3339)
	return r
}

// ToBytes serializes the result message to JSON bytes
func (r *ResultMessage) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// FromBytes deserializes a result message from JSON bytes
func ResultMessageFromBytes(data []byte) (*ResultMessage, error) {
	var msg ResultMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// FromNATSMsg converts a NATS message to a ResultMessage
func ResultMessageFromNATSMsg(natsMsg *nats.Msg) (*ResultMessage, error) {
	return ResultMessageFromBytes(natsMsg.Data)
}

// HasInlineResult returns true if the result is available inline
func (r *ResultMessage) HasInlineResult() bool {
	return len(r.InlineResult) > 0
}

// HasBlobReference returns true if the result is stored in blob storage
func (r *ResultMessage) HasBlobReference() bool {
	return r.BlobReference != nil && r.BlobReference.URL != ""
}

// IsSuccess returns true if the execution was successful
func (r *ResultMessage) IsSuccess() bool {
	return r.Status == "success"
}

// IsFailed returns true if the execution failed
func (r *ResultMessage) IsFailed() bool {
	return r.Status == "failed"
}

// IsSkipped returns true if the execution was skipped
func (r *ResultMessage) IsSkipped() bool {
	return r.Status == "skipped"
}

// IsRetryable returns true if the error is retryable (only meaningful for failed executions)
func (r *ResultMessage) IsRetryable() bool {
	return r.Error != nil && r.Error.Retryable
}
