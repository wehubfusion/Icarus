package errors

import (
	"errors"
	"fmt"
)

var (
	// ErrNotConnected indicates that the client is not connected to NATS
	ErrNotConnected = errors.New("not connected to NATS")

	// ErrInvalidSubject indicates that the provided subject is invalid
	ErrInvalidSubject = errors.New("invalid subject")

	// ErrInvalidMessage indicates that the message is invalid
	ErrInvalidMessage = errors.New("invalid message")

	// ErrTimeout indicates that an operation timed out
	ErrTimeout = errors.New("operation timed out")

	// ErrNoResponse indicates that no response was received for a request
	ErrNoResponse = errors.New("no response received")

	// ErrSubscriptionFailed indicates that a subscription could not be created
	ErrSubscriptionFailed = errors.New("subscription failed")

	// ErrPublishFailed indicates that a message could not be published
	ErrPublishFailed = errors.New("publish failed")

	// ErrInvalidHandler indicates that a handler is invalid
	ErrInvalidHandler = errors.New("invalid handler")

	// ErrConsumerNotFound indicates that a consumer was not found
	ErrConsumerNotFound = errors.New("consumer not found")
)

// Error represents a structured SDK error
type Error struct {
	// Code is a machine-readable error code
	Code string

	// Message is a human-readable error message
	Message string

	// Err is the underlying error, if any
	Err error
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap returns the underlying error
func (e *Error) Unwrap() error {
	return e.Err
}

// NewError creates a new SDK error
func NewError(code, message string, err error) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// IsTimeout checks if an error is a timeout error
func IsTimeout(err error) bool {
	return errors.Is(err, ErrTimeout)
}

// IsNotConnected checks if an error is a not connected error
func IsNotConnected(err error) bool {
	return errors.Is(err, ErrNotConnected)
}

