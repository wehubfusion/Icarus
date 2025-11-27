package message

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"go.uber.org/zap"
)

// JSContext defines the minimal subset of JetStream operations the service depends on.
// This allows tests to provide a mock without requiring a running NATS server.
type JSContext interface {
	Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
	PullSubscribe(subj, durable string, opts ...nats.SubOpt) (JSSubscription, error)
	StreamInfo(stream string) (*nats.StreamInfo, error)
	AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error)
	ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error)
	AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error)
}

// JSSubscription abstracts operations used by the SDK from a subscription.
// Implemented by the real nats.Subscription via adapter and by test doubles.
type JSSubscription interface {
	Unsubscribe() error
	Drain() error
	IsValid() bool
	Pending() (int, int, error)
	Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error)
}

// WrapNATSJetStream adapts a nats.JetStreamContext to the JSContext interface.
func WrapNATSJetStream(js nats.JetStreamContext) JSContext {
	return &natsJSAdapter{js: js}
}

type natsJSAdapter struct {
	js nats.JetStreamContext
}

func (a *natsJSAdapter) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return a.js.Publish(subj, data, opts...)
}

func (a *natsJSAdapter) PullSubscribe(subj, durable string, opts ...nats.SubOpt) (JSSubscription, error) {
	sub, err := a.js.PullSubscribe(subj, durable, opts...)
	if err != nil {
		return nil, err
	}
	return &natsSubAdapter{sub: sub}, nil
}

func (a *natsJSAdapter) StreamInfo(stream string) (*nats.StreamInfo, error) {
	return a.js.StreamInfo(stream)
}

func (a *natsJSAdapter) AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
	return a.js.AddStream(cfg)
}

func (a *natsJSAdapter) ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error) {
	return a.js.ConsumerInfo(stream, consumer)
}

func (a *natsJSAdapter) AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	return a.js.AddConsumer(stream, cfg)
}

type natsSubAdapter struct {
	sub *nats.Subscription
}

func (s *natsSubAdapter) Unsubscribe() error         { return s.sub.Unsubscribe() }
func (s *natsSubAdapter) Drain() error               { return s.sub.Drain() }
func (s *natsSubAdapter) IsValid() bool              { return s.sub.IsValid() }
func (s *natsSubAdapter) Pending() (int, int, error) { return s.sub.Pending() }
func (s *natsSubAdapter) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return s.sub.Fetch(batch, opts...)
}

// MessageService provides methods for publishing and managing messages over JetStream.
// All operations use JetStream exclusively with proper acknowledgment handling.
type MessageService struct {
	js                JSContext
	logger            *zap.Logger
	maxDeliver        int               // Maximum number of delivery attempts before giving up (default: 5)
	publishMaxRetries int               // Maximum number of retry attempts for publish operations (default: 3)
	resultStream      string            // JetStream stream name for publishing results (e.g., RESULTS_UAT)
	resultSubject     string            // Subject for publishing results (e.g., result.uat)
	temporalClient    TemporalSignaler  // Temporal client for signaling Zeus workflows
	blobStorage       BlobStorageClient // Blob storage for large results
}

// TemporalSignaler interface for signaling workflows
type TemporalSignaler interface {
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, data interface{}) error
}

// BlobStorageClient interface for storing large results
type BlobStorageClient interface {
	UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error)
	DownloadResult(ctx context.Context, blobURL string) ([]byte, error)
}

const (
	maxSignalSize = 1.5 * 1024 * 1024 // 1.5MB - Temporal signal payload limit (actual is 2MB, using 1.5MB for safety)
)

// SetTemporalClient sets the Temporal client for signaling
func (s *MessageService) SetTemporalClient(tc TemporalSignaler) {
	s.temporalClient = tc
}

// SetBlobStorage sets the blob storage client for large results
func (s *MessageService) SetBlobStorage(bs BlobStorageClient) {
	s.blobStorage = bs
}

// validateMessage performs strict validation on the message for callback operations
// Auto-populates CreatedAt and UpdatedAt if they are empty
func (s *MessageService) validateMessage(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	// Auto-populate timestamps if empty (framework responsibility, not service responsibility)
	now := time.Now().Format(time.RFC3339)
	if msg.CreatedAt == "" {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt == "" {
		msg.UpdatedAt = now
	}

	if msg.Workflow == nil {
		return fmt.Errorf("message Workflow is required")
	}

	// Node is optional when Workflow is present (for workflow-level callbacks)
	// But if Node is present, Workflow must also be present
	if msg.Node != nil && msg.Workflow == nil {
		return fmt.Errorf("message Workflow is required when Node is present")
	}

	if msg.Payload == nil {
		return fmt.Errorf("message Payload is required")
	}

	return nil
}

// NewMessageService creates a new message service with the given JetStream context.
// Any implementation that satisfies JSContext (including nats.JetStreamContext) can be used.
// The maxDeliver parameter controls the maximum number of delivery attempts for consumers.
// The publishMaxRetries parameter controls the maximum number of retry attempts for publish operations.
// The resultStream and resultSubject parameters configure where results are published (e.g., RESULTS_UAT, result.uat).
func NewMessageService(js JSContext, maxDeliver int, publishMaxRetries int, resultStream string, resultSubject string) (*MessageService, error) {
	if js == nil {
		return nil, fmt.Errorf("JetStream context cannot be nil")
	}

	// Use default if not set or invalid
	if maxDeliver == 0 {
		maxDeliver = 5 // Default: retry up to 5 times (2.5 minutes with 30s AckWait)
	}

	if publishMaxRetries == 0 {
		publishMaxRetries = 3 // Default: 3 retries for publish operations
	}

	if resultStream == "" {
		resultStream = "RESULTS" // Default result stream
	}

	if resultSubject == "" {
		resultSubject = "result" // Default result subject
	}

	logger, _ := zap.NewProduction()
	return &MessageService{
		js:                js,
		logger:            logger,
		maxDeliver:        maxDeliver,
		publishMaxRetries: publishMaxRetries,
		resultStream:      resultStream,
		resultSubject:     resultSubject,
	}, nil
}

// SetLogger sets a custom zap logger for the message service
func (s *MessageService) SetLogger(logger *zap.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

// EnsureStream creates the JetStream stream if it doesn't exist, or validates it exists.
// This is a public method that can be called by runners and other components.
func (s *MessageService) EnsureStream(streamName string) error {
	// Check if stream exists
	streamInfo, err := s.js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist, create it
		if err == nats.ErrStreamNotFound {
			s.logger.Info("Creating JetStream stream",
				zap.String("stream", streamName))

			streamConfig := &nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.*", streamName)},
				Storage:  nats.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s': %w", streamName, err)
			}

			s.logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.Strings("subjects", streamConfig.Subjects),
				zap.Duration("max_age", streamConfig.MaxAge),
				zap.Int64("max_msgs", streamConfig.MaxMsgs))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	} else {
		// Stream exists, log its status
		s.logger.Info("JetStream stream already exists",
			zap.String("stream", streamName),
			zap.Uint64("messages", streamInfo.State.Msgs))
	}

	return nil
}

// EnsureConsumer creates the JetStream consumer if it doesn't exist, or validates it exists.
// This is a public method that can be called by runners and other components.
func (s *MessageService) EnsureConsumer(streamName, consumerName string) error {
	// Try to get consumer info first
	consumerInfo, err := s.js.ConsumerInfo(streamName, consumerName)
	if err != nil {
		// Consumer doesn't exist, create it
		if err == nats.ErrConsumerNotFound {
			s.logger.Info("Creating JetStream consumer",
				zap.String("stream", streamName),
				zap.String("consumer", consumerName))

			consumerConfig := &nats.ConsumerConfig{
				Durable:       consumerName,
				AckPolicy:     nats.AckExplicitPolicy,
				DeliverPolicy: nats.DeliverAllPolicy,
				MaxAckPending: 1000,
				MaxDeliver:    s.maxDeliver,
			}

			_, err = s.js.AddConsumer(streamName, consumerConfig)
			if err != nil {
				return fmt.Errorf("failed to create consumer '%s' in stream '%s': %w", consumerName, streamName, err)
			}

			s.logger.Info("Successfully created JetStream consumer",
				zap.String("stream", streamName),
				zap.String("consumer", consumerName),
				zap.Int("max_deliver", s.maxDeliver))
		} else {
			return fmt.Errorf("failed to get consumer info for '%s' in stream '%s': %w", consumerName, streamName, err)
		}
	} else {
		// Consumer exists, log its status
		s.logger.Info("JetStream consumer already exists",
			zap.String("stream", streamName),
			zap.String("consumer", consumerName),
			zap.Uint64("pending", consumerInfo.NumPending))
	}

	return nil
}

// ensureStreamForSubject ensures a stream exists that can handle the given subject.
// For result subjects, it uses the configured resultStream.
// For other subjects, it extracts the stream name from the subject (first segment before dot)
// and creates the stream if it doesn't exist.
func (s *MessageService) ensureStreamForSubject(subject string) error {
	var streamName string
	var isResultSubject bool

	// Check if this is a result subject
	if s.resultSubject != "" && subject == s.resultSubject {
		// Use the configured result stream
		streamName = s.resultStream
		isResultSubject = true
		s.logger.Debug("Using configured result stream",
			zap.String("stream", streamName),
			zap.String("subject", subject))
	} else {
		// Extract stream name from subject (first part before dot)
		streamName = subject
		if idx := len(subject); idx > 0 {
			// Find first dot to extract stream name
			for i, c := range subject {
				if c == '.' {
					streamName = subject[:i]
					break
				}
			}
		}
	}

	// Check if stream exists
	_, err := s.js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist
		if err == nats.ErrStreamNotFound {
			// Create stream (both result streams and regular streams)
			s.logger.Info("Creating JetStream stream for subject",
				zap.String("stream", streamName),
				zap.String("subject", subject),
				zap.Bool("is_result_stream", isResultSubject))

			// For result subjects, use the configured subject pattern
			// For other subjects, derive pattern from subject
			var subjectPattern string
			if isResultSubject {
				// Result subjects use the configured subject with > wildcard
				// e.g., "result.uat.>" matches "result.uat", "result.uat.X", etc.
				subjectPattern = fmt.Sprintf("%s.>", subject)
			} else {
				// Regular subjects use stream name derived pattern
				// e.g., "HTTP_REQUESTS_UAT.>" for stream "HTTP_REQUESTS_UAT"
				subjectPattern = fmt.Sprintf("%s.>", streamName)
			}

			streamConfig := &nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{subjectPattern},
				Storage:  nats.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s' for subject '%s': %w", streamName, subject, err)
			}

			s.logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.String("subject_pattern", subjectPattern),
				zap.Bool("is_result_stream", isResultSubject))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	}

	return nil
}

// getMessageIdentifier creates a unique identifier for logging purposes
func (s *MessageService) getMessageIdentifier(msg *Message) string {
	// Prefer correlation ID if available
	if msg.CorrelationID != "" {
		return fmt.Sprintf("correlation:%s", msg.CorrelationID)
	}
	if msg.Workflow != nil {
		return fmt.Sprintf("workflow:%s/run:%s", msg.Workflow.WorkflowID, msg.Workflow.RunID)
	}
	if msg.Node != nil {
		return fmt.Sprintf("node:%s", msg.Node.NodeID)
	}
	if msg.Payload != nil && msg.Payload.Reference != "" {
		return fmt.Sprintf("payload:%s", msg.Payload.Reference)
	}
	return fmt.Sprintf("timestamp:%s", msg.CreatedAt)
}

// Publish publishes a message to the specified subject using JetStream.
// The message is persisted according to the stream's configuration.
// If no stream exists for the subject, one will be created automatically.
// Returns an error if the publish fails.
func (s *MessageService) Publish(ctx context.Context, subject string, msg *Message) error {
	if subject == "" {
		s.logger.Error("Publish failed: subject cannot be empty")
		return sdkerrors.NewValidationError("subject cannot be empty", "INVALID_SUBJECT", nil)
	}

	if msg == nil {
		s.logger.Error("Publish failed: message cannot be nil")
		return sdkerrors.NewValidationError("message cannot be nil", "INVALID_MESSAGE", nil)
	}

	// Ensure a stream exists for this subject
	if err := s.ensureStreamForSubject(subject); err != nil {
		s.logger.Error("Failed to ensure stream exists",
			zap.String("subject", subject),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to ensure stream exists", "STREAM_ENSURE_FAILED", err)
	}

	s.logger.Debug("Publishing message",
		zap.String("subject", subject),
		zap.String("message_identifier", s.getMessageIdentifier(msg)))

	data, err := msg.ToBytes()
	if err != nil {
		s.logger.Error("Failed to marshal message",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to marshal message", "MARSHAL_FAILED", err)
	}

	// Create a channel to handle publish result
	resultCh := make(chan error, 1)

	go func() {
		_, err := s.js.Publish(subject, data)
		resultCh <- err
	}()

	select {
	case <-ctx.Done():
		s.logger.Warn("Publish cancelled",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)),
			zap.Error(ctx.Err()))
		return fmt.Errorf("publish cancelled: %w", ctx.Err())
	case err := <-resultCh:
		if err != nil {
			s.logger.Error("Failed to publish message to JetStream",
				zap.String("subject", subject),
				zap.String("message_identifier", s.getMessageIdentifier(msg)),
				zap.Error(err))
			return sdkerrors.NewInternalError("", "failed to publish message to JetStream", "PUBLISH_FAILED", err)
		}
		s.logger.Info("Message published successfully",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)))
		return nil
	}
}

// PullMessages pulls messages from a JetStream pull-based consumer.
// This method fetches messages in batches on demand, providing explicit flow control.
//
// Messages are NOT automatically acknowledged - the caller must handle acknowledgment
// by calling Ack(), Nak(), or Term() on the returned messages as appropriate.
// Use this method when you want explicit control over when messages are fetched and acknowledged.
//
// Parameters:
//   - stream: The name of the JetStream stream
//   - consumer: The name of the durable consumer
//   - batchSize: The maximum number of messages to fetch (defaults to 10 if <= 0)
//
// Returns the fetched messages or an error if the operation fails.
// Note: Returns empty slice (not error) when no messages are available within timeout.
func (s *MessageService) PullMessages(ctx context.Context, stream, consumer string, batchSize int) ([]*Message, error) {
	if stream == "" || consumer == "" {
		s.logger.Error("PullMessages failed: stream and consumer names are required")
		return nil, fmt.Errorf("stream and consumer names are required")
	}

	if batchSize <= 0 {
		batchSize = 10
	}

	s.logger.Debug("Pulling messages",
		zap.String("stream", stream),
		zap.String("consumer", consumer),
		zap.Int("batch_size", batchSize))

	// Create a channel to handle pull result
	type result struct {
		msgs []*Message
		err  error
	}
	resultCh := make(chan result, 1)

	go func() {
		// Bind to existing consumer
		sub, err := s.js.PullSubscribe("", consumer, nats.Bind(stream, consumer))
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer sub.Unsubscribe()

		// Fetch messages with timeout - use context deadline if available, otherwise default to 3 seconds
		timeout := 3 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
				timeout = remaining
			}
		}

		natsMessages, err := sub.Fetch(batchSize, nats.MaxWait(timeout))
		if err != nil {
			// Check if this is a timeout error - this is normal when no messages are available
			if err == nats.ErrTimeout {
				// Return empty slice for timeout, not an error
				resultCh <- result{msgs: []*Message{}}
				return
			}
			resultCh <- result{err: err}
			return
		}

		messages := make([]*Message, 0, len(natsMessages))
		for _, natsMsg := range natsMessages {
			msg, err := FromNATSMsg(natsMsg)
			if err != nil {
				// Nak malformed messages
				_ = natsMsg.Nak()
				continue
			}
			// Do NOT acknowledge - let the application handle acknowledgment
			// Store the NATS message reference in the Message for later acknowledgment
			msg.natsMsg = natsMsg
			messages = append(messages, msg)
		}

		resultCh <- result{msgs: messages}
	}()

	select {
	case <-ctx.Done():
		// Use debug level for graceful shutdown, warn for unexpected cancellation
		if ctx.Err() == context.Canceled {
			s.logger.Debug("Pull messages cancelled during shutdown",
				zap.String("stream", stream),
				zap.String("consumer", consumer))
		} else {
			s.logger.Warn("Pull messages cancelled",
				zap.String("stream", stream),
				zap.String("consumer", consumer),
				zap.Error(ctx.Err()))
		}
		return nil, fmt.Errorf("pull cancelled: %w", ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
			s.logger.Error("Failed to pull messages from JetStream",
				zap.String("stream", stream),
				zap.String("consumer", consumer),
				zap.Error(res.err))
			return nil, sdkerrors.NewInternalError("", "failed to pull messages from JetStream", "PULL_FAILED", res.err)
		}
		return res.msgs, nil
	}
}

// ResultType represents different types of results that can be published
type ResultType string

const (
	ResultTypeSuccess ResultType = "success"
	ResultTypeError   ResultType = "error"
	ResultTypeWarning ResultType = "warning"
	ResultTypeInfo    ResultType = "info"
)

// ReportSuccess signals Zeus workflow with execution result using Temporal Signals.
// For results <1.5MB, sends full payload via signal. For larger results, uploads to
// Azure Blob Storage and signals with reference.
func (s *MessageService) ReportSuccess(ctx context.Context, resultMessage Message, msg *nats.Msg) error {
	startTime := time.Now()
	resultMessage.WithMetadata("result_type", string(ResultTypeSuccess))

	// Validate message
	if err := s.validateMessage(&resultMessage); err != nil {
		s.logger.Error("Success report validation failed", zap.Error(err))
		return fmt.Errorf("validation failed: %w", err)
	}

	// Extract Temporal callback info
	workflowID := resultMessage.Metadata["temporal_workflow_id"]
	runID := resultMessage.Metadata["temporal_run_id"]
	signalName := resultMessage.Metadata["temporal_signal_name"]
	executionID := resultMessage.Metadata["execution_id"]

	if workflowID == "" || runID == "" || signalName == "" {
		s.logger.Error("Missing Temporal callback metadata",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("signal_name", signalName))
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("missing Temporal callback metadata")
	}

	if s.temporalClient == nil {
		s.logger.Error("Temporal client not initialized")
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("temporal client not initialized")
	}

	s.logger.Debug("Preparing to signal Zeus workflow",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("signal_name", signalName),
		zap.String("execution_id", executionID))

	// Parse result from payload
	var nodeResult map[string]interface{}
	if err := json.Unmarshal([]byte(resultMessage.Payload.Data), &nodeResult); err != nil {
		s.logger.Error("Failed to unmarshal result, will report as error",
			zap.String("execution_id", executionID),
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.Error(err))

		// Report unmarshal failure as error to Temporal if we have executionID
		if executionID != "" {
			unmarshalErr := fmt.Errorf("failed to unmarshal result: %w", err)
			if reportErr := s.ReportError(ctx, executionID, workflowID, runID, unmarshalErr, msg); reportErr != nil {
				s.logger.Error("Failed to report unmarshal error to workflow",
					zap.String("execution_id", executionID),
					zap.String("workflow_id", workflowID),
					zap.Error(reportErr))
			}
		}

		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("failed to unmarshal result: %w", err)
	}

	var existingBlobRef *BlobReference
	if resultMessage.Payload != nil && resultMessage.Payload.BlobReference != nil && resultMessage.Payload.BlobReference.URL != "" {
		existingBlobRef = resultMessage.Payload.BlobReference
		nodeResult["blob_reference"] = map[string]interface{}{
			"url":        existingBlobRef.URL,
			"size_bytes": existingBlobRef.SizeBytes,
		}
	}

	// Check payload size
	resultBytes, _ := json.Marshal(nodeResult)
	resultSize := len(resultBytes)

	var signalPayload interface{}

	// Size-based routing
	if existingBlobRef != nil {
		s.logger.Info("Reusing existing blob reference for large result",
			zap.String("execution_id", executionID),
			zap.Int("size_bytes", existingBlobRef.SizeBytes))

		signalPayload = map[string]interface{}{
			"execution_id": executionID,
			"workflow_id":  workflowID,
			"run_id":       runID,
			"node_id":      nodeResult["node_id"],
			"status":       nodeResult["status"],
			"large_result": true,
			"blob_url":     existingBlobRef.URL,
			"result_size":  existingBlobRef.SizeBytes,
			"timestamp":    time.Now(),
		}
	} else if resultSize <= maxSignalSize {
		// FAST PATH: Direct signal
		s.logger.Info("Sending result via direct signal",
			zap.String("execution_id", executionID),
			zap.Int("size_bytes", resultSize))

		signalPayload = nodeResult

	} else {
		// LARGE RESULT PATH: Azure Blob Storage
		s.logger.Info("Result too large, storing in Azure Blob Storage",
			zap.String("execution_id", executionID),
			zap.Int("size_bytes", resultSize),
			zap.Int("threshold", maxSignalSize))

		if s.blobStorage == nil {
			s.logger.Error("Blob storage not initialized for large result",
				zap.Int("size_bytes", resultSize))
			if msg != nil {
				_ = msg.Nak()
			}
			return fmt.Errorf("blob storage not initialized but result size %d exceeds limit", resultSize)
		}

		// Build blob path
		blobPath := fmt.Sprintf("results/%s/%s/%s.json",
			workflowID,
			runID,
			executionID)

		// Upload to Azure Blob Storage
		blobURL, err := s.blobStorage.UploadResult(ctx, blobPath, resultBytes, map[string]string{
			"workflow_id":  workflowID,
			"run_id":       runID,
			"execution_id": executionID,
			"node_id":      fmt.Sprintf("%v", nodeResult["node_id"]),
			"status":       fmt.Sprintf("%v", nodeResult["status"]),
		})

		if err != nil {
			s.logger.Error("Failed to upload result to blob storage, will report as error",
				zap.String("execution_id", executionID),
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.Error(err))

			// Report the blob upload failure as an error to Temporal
			blobErr := fmt.Errorf("blob upload failed: %w", err)
			if reportErr := s.ReportError(ctx, executionID, workflowID, runID, blobErr, msg); reportErr != nil {
				s.logger.Error("Failed to report blob upload error to workflow",
					zap.String("execution_id", executionID),
					zap.String("workflow_id", workflowID),
					zap.Error(reportErr))
			}

			if msg != nil {
				_ = msg.Nak()
			}
			return blobErr
		}

		s.logger.Info("Result uploaded to blob storage",
			zap.String("execution_id", executionID),
			zap.String("blob_url", blobURL),
			zap.Int("size_bytes", resultSize))

		nodeResult["blob_reference"] = map[string]interface{}{
			"url":        blobURL,
			"size_bytes": resultSize,
		}

		// Send compact reference signal
		signalPayload = map[string]interface{}{
			"execution_id": executionID,
			"workflow_id":  workflowID,
			"run_id":       runID,
			"node_id":      nodeResult["node_id"],
			"status":       nodeResult["status"],
			"large_result": true,
			"blob_url":     blobURL,
			"result_size":  resultSize,
			"timestamp":    time.Now(),
		}
	}

	// Signal Zeus workflow with timeout (30s for reliability)
	signalCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := s.temporalClient.SignalWorkflow(signalCtx, workflowID, runID, signalName, signalPayload); err != nil {
		s.logger.Error("Failed to signal workflow with success result, will report as error",
			zap.String("workflow_id", workflowID),
			zap.String("execution_id", executionID),
			zap.String("run_id", runID),
			zap.Error(err))

		// Report the signal failure as an error to Temporal
		signalErr := fmt.Errorf("failed to signal workflow: %w", err)
		if reportErr := s.ReportError(ctx, executionID, workflowID, runID, signalErr, msg); reportErr != nil {
			s.logger.Error("Failed to report signal error to workflow (cascading failure)",
				zap.String("execution_id", executionID),
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.Error(reportErr))
		}

		if msg != nil {
			_ = msg.Nak() // Retry on signal failure
		}
		return signalErr
	}

	signalDuration := time.Since(startTime)
	s.logger.Info("Successfully signaled Zeus workflow",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("execution_id", executionID),
		zap.Duration("signal_duration", signalDuration),
		zap.Int("payload_size", resultSize),
		zap.Bool("used_blob_storage", resultSize > maxSignalSize))

	// Acknowledge the source message
	if msg != nil {
		if err := msg.Ack(); err != nil {
			s.logger.Error("Failed to acknowledge source message",
				zap.String("execution_id", executionID),
				zap.Error(err))
			return fmt.Errorf("failed to acknowledge: %w", err)
		}
	}

	return nil
}

// ReportError signals Zeus workflow with error result using Temporal Signals.
//
// Error Classification:
//   - Internal errors (transient failures): NAK the message for retry
//   - Other errors (permanent failures like BadRequest, NotFound, etc.): ACK the message to prevent redelivery
//
// Parameters:
//   - executionID: The unique identifier for this execution (required for signaling)
//   - workflowID: The unique identifier of the workflow that failed
//   - runID: The unique identifier of this specific workflow execution run
//   - err: The error that occurred (can be *AppError or regular error)
//   - msg: NATS message to acknowledge/nak after error reporting (can be nil)
//
// Returns an error if the message cannot be published.
func (s *MessageService) ReportError(ctx context.Context, executionID, workflowID, runID string, err error, msg *nats.Msg) error {
	startTime := time.Now()

	if executionID == "" {
		s.logger.Warn("Missing executionID for error report")
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("missing executionID")
	}

	// Determine if transient or permanent
	isTransient := true
	errorMsg := err.Error()

	if appErr, ok := err.(*sdkerrors.AppError); ok {
		isTransient = (appErr.Type == sdkerrors.Internal)
	}

	s.logger.Info("Reporting error via signal",
		zap.String("execution_id", executionID),
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.Bool("is_transient", isTransient))

	if s.temporalClient == nil {
		s.logger.Error("Temporal client not initialized")
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("temporal client unavailable")
	}

	// Build error result
	errorResult := map[string]interface{}{
		"execution_id": executionID,
		"workflow_id":  workflowID,
		"run_id":       runID,
		"status":       "failed",
		"error":        errorMsg,
		"timestamp":    time.Now(),
		"is_transient": isTransient,
	}

	// Signal with timeout (30s for reliability, with retry)
	signalName := fmt.Sprintf("unit-result-%s", executionID)

	// Retry logic for critical error reporting
	var signalErr error
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		signalCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		signalErr = s.temporalClient.SignalWorkflow(signalCtx, workflowID, runID, signalName, errorResult)
		cancel()

		if signalErr == nil {
			break
		}

		if attempt < maxRetries {
			s.logger.Warn("Failed to signal workflow with error, retrying",
				zap.String("execution_id", executionID),
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", maxRetries),
				zap.Error(signalErr))
			// Brief backoff before retry
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	if signalErr != nil {
		s.logger.Error("Failed to signal workflow with error after all retries",
			zap.String("execution_id", executionID),
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.Int("attempts", maxRetries),
			zap.Error(signalErr))
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("failed to signal after %d attempts: %w", maxRetries, signalErr)
	}

	s.logger.Info("Successfully signaled error to Zeus",
		zap.String("execution_id", executionID),
		zap.String("workflow_id", workflowID),
		zap.Duration("duration", time.Since(startTime)))

	// Ack/Nak based on error type
	if msg != nil {
		if isTransient {
			_ = msg.Nak()
		} else {
			_ = msg.Ack()
		}
	}

	return nil
}
