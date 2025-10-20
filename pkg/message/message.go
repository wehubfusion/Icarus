package message

import (
	"encoding/json"
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

// Payload represents the message payload data
type Payload struct {
	Source    string `json:"source"`
	Data      string `json:"data"`
	Reference string `json:"reference"`
}

// Output represents output destination information
type Output struct {
	DestinationType string `json:"destinationType"`
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
