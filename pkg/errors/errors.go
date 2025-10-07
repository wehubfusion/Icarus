package errors

import (
	"fmt"

	"github.com/getsentry/sentry-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorType represents different types of errors
type ErrorType int

const (
	Internal ErrorType = iota
	NotFound
	BadRequest
	Unauthorized
	Conflict
	ValidationFailed
)

// AppError represents a custom application error
type AppError struct {
	Type    ErrorType `json:"type"`
	Message string    `json:"message"`
	Code    string    `json:"code,omitempty"`
	Err     error     `json:"-"` // underlying error for debugging
}

// Error implements the error interface
func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Unwrap returns the underlying error
func (e *AppError) Unwrap() error {
	return e.Err
}

func NewNotFoundError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    NotFound,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

func NewBadRequestError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    BadRequest,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

func NewValidationError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    ValidationFailed,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

func NewConflictError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    Conflict,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

func NewUnauthorizedError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    Unauthorized,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// NewAppErrorWithCause creates an AppError with an underlying cause
func NewInternalError(metaData string, message string, code string, cause error) *AppError {
	var errorMessage string
	if metaData != "" {
		errorMessage = fmt.Sprintf("MetaData: %s\nMessage: %s\nCode: %s\nError: %v", metaData, message, code, cause)
	} else {
		errorMessage = fmt.Sprintf("Message: %s\nCode: %s\nError: %v", message, code, cause)
	}
	sentry.CaptureMessage(errorMessage)
	return &AppError{
		Type:    Internal,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// ToGRPCError converts an AppError to a gRPC error
func (e *AppError) ToGRPCError() error {
	var grpcCode codes.Code
	switch e.Type {
	case NotFound:
		grpcCode = codes.NotFound
	case BadRequest:
		grpcCode = codes.InvalidArgument
	case Unauthorized:
		grpcCode = codes.Unauthenticated
	case Conflict:
		grpcCode = codes.AlreadyExists
	case ValidationFailed:
		grpcCode = codes.InvalidArgument
	case Internal:
		fallthrough
	default:
		grpcCode = codes.Internal
	}

	return status.Error(grpcCode, e.Message)
}
