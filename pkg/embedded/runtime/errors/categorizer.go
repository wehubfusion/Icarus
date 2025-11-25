package errors

import (
	"context"
	stdErrors "errors"
	"net"
	"strings"
	"time"

	icarusErrors "github.com/wehubfusion/Icarus/pkg/errors"
)

// Error code constants
const (
	ErrorCodeUnknown        = "UNKNOWN_ERROR"
	ErrorCodeTimeout        = "TIMEOUT_ERROR"
	ErrorCodeNetwork        = "NETWORK_ERROR"
	ErrorCodeValidation     = "VALIDATION_ERROR"
	ErrorCodeNotFound       = "NOT_FOUND_ERROR"
	ErrorCodeUnauthorized   = "UNAUTHORIZED_ERROR"
	ErrorCodeForbidden      = "FORBIDDEN_ERROR"
	ErrorCodeBadRequest     = "BAD_REQUEST_ERROR"
	ErrorCodeInternal       = "INTERNAL_ERROR"
	ErrorCodeFieldMapping   = "FIELD_MAPPING_ERROR"
	ErrorCodeExecution      = "EXECUTION_ERROR"
	ErrorCodeIteration      = "ITERATION_ERROR"
	ErrorCodeConfiguration  = "CONFIGURATION_ERROR"
	ErrorCodeCircuitBreaker = "CIRCUIT_BREAKER_ERROR"
	ErrorCodeRateLimit      = "RATE_LIMIT_ERROR"
)

// CategorizeError maps an error to a standardized error code
func CategorizeError(err error) string {
	if err == nil {
		return ""
	}

	// Check for Icarus error types
	var appErr *icarusErrors.AppError
	if stdErrors.As(err, &appErr) {
		switch appErr.Type {
		case icarusErrors.ValidationFailed:
			return ErrorCodeValidation
		case icarusErrors.NotFound:
			return ErrorCodeNotFound
		case icarusErrors.Unauthorized:
			return ErrorCodeUnauthorized
		case icarusErrors.BadRequest:
			return ErrorCodeBadRequest
		case icarusErrors.Internal:
			return ErrorCodeInternal
		case icarusErrors.PermissionDenied:
			return ErrorCodeForbidden
		}
	}

	// Check for timeout errors
	if stdErrors.Is(err, context.DeadlineExceeded) {
		return ErrorCodeTimeout
	}

	// Check for network errors
	var netErr net.Error
	if stdErrors.As(err, &netErr) {
		if netErr.Timeout() {
			return ErrorCodeTimeout
		}
		return ErrorCodeNetwork
	}

	// Check error message for common patterns
	errMsg := strings.ToLower(err.Error())

	if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "timed out") {
		return ErrorCodeTimeout
	}

	if strings.Contains(errMsg, "network") || strings.Contains(errMsg, "connection") {
		return ErrorCodeNetwork
	}

	if strings.Contains(errMsg, "validation") || strings.Contains(errMsg, "invalid") {
		return ErrorCodeValidation
	}

	if strings.Contains(errMsg, "not found") {
		return ErrorCodeNotFound
	}

	if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "authentication") {
		return ErrorCodeUnauthorized
	}

	if strings.Contains(errMsg, "forbidden") || strings.Contains(errMsg, "permission") {
		return ErrorCodeForbidden
	}

	if strings.Contains(errMsg, "field mapping") {
		return ErrorCodeFieldMapping
	}

	if strings.Contains(errMsg, "iteration") {
		return ErrorCodeIteration
	}

	if strings.Contains(errMsg, "configuration") || strings.Contains(errMsg, "config") {
		return ErrorCodeConfiguration
	}

	if strings.Contains(errMsg, "circuit breaker") {
		return ErrorCodeCircuitBreaker
	}

	if strings.Contains(errMsg, "rate limit") {
		return ErrorCodeRateLimit
	}

	return ErrorCodeUnknown
}

// IsRetryable determines if an error is transient and should be retried
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for timeout errors (usually retryable)
	if stdErrors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for network errors
	var netErr net.Error
	if stdErrors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		// Network errors are generally retryable
		return true
	}

	// Check error code
	errorCode := CategorizeError(err)
	switch errorCode {
	case ErrorCodeTimeout:
		return true
	case ErrorCodeNetwork:
		return true
	case ErrorCodeCircuitBreaker:
		return true
	case ErrorCodeRateLimit:
		return true
	case ErrorCodeInternal:
		return true // Internal errors might be transient
	case ErrorCodeValidation:
		return false // Validation errors won't change on retry
	case ErrorCodeNotFound:
		return false
	case ErrorCodeUnauthorized:
		return false
	case ErrorCodeForbidden:
		return false
	case ErrorCodeBadRequest:
		return false
	case ErrorCodeConfiguration:
		return false
	default:
		return false // Conservative default
	}
}

// ExtractErrorDetails extracts additional context from an error
func ExtractErrorDetails(err error) map[string]interface{} {
	if err == nil {
		return nil
	}

	details := make(map[string]interface{})

	// Check for Icarus error types that might have additional context
	var appErr *icarusErrors.AppError
	if stdErrors.As(err, &appErr) {
		if appErr.Code != "" {
			details["error_code"] = appErr.Code
		}
		if appErr.Message != "" {
			details["error_message"] = appErr.Message
		}
	}

	// Extract timeout information
	var netErr net.Error
	if stdErrors.As(err, &netErr) {
		if netErr.Timeout() {
			details["timeout"] = true
		}
		if netErr.Temporary() {
			details["temporary"] = true
		}
	}

	// Add timestamp
	details["timestamp"] = time.Now().Format(time.RFC3339)

	return details
}
