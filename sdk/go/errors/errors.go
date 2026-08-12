// Package errors provides error types for the VELOCITY-WorkFlow SDK.
//
// All errors include an error code, message, and retryable flag.
// Error codes are consistent across all SDKs.
package errors

import (
	"fmt"
)

// ErrorCode represents a numeric error code.
type ErrorCode int

const (
	// CodeUnknown is an unknown error.
	CodeUnknown ErrorCode = 0
	// CodeNotFound indicates the workflow was not found.
	CodeNotFound ErrorCode = 1
	// CodeAlreadyCompleted indicates the workflow is already completed.
	CodeAlreadyCompleted ErrorCode = 2
	// CodeConnection indicates a connection failure.
	CodeConnection ErrorCode = 3
	// CodeTimeout indicates an operation timed out.
	CodeTimeout ErrorCode = 4
	// CodeRateLimit indicates rate limit exceeded.
	CodeRateLimit ErrorCode = 5
	// CodeAuthentication indicates authentication failure.
	CodeAuthentication ErrorCode = 6
	// CodeInternal indicates an internal server error.
	CodeInternal ErrorCode = 7
)

// VelocityError is the base error type for all VELOCITY-WorkFlow errors.
type VelocityError struct {
	Message   string
	ErrorCode ErrorCode
	Retryable bool
	Details   map[string]interface{}
}

// Error implements the error interface.
func (e *VelocityError) Error() string {
	retry := ""
	if e.Retryable {
		retry = " (retryable)"
	}
	return fmt.Sprintf("VelocityError[%d]: %s%s", e.ErrorCode, e.Message, retry)
}

// NewVelocityError creates a new VelocityError.
func NewVelocityError(message string, code ErrorCode, retryable bool) *VelocityError {
	return &VelocityError{
		Message:   message,
		ErrorCode: code,
		Retryable: retryable,
		Details:   make(map[string]interface{}),
	}
}

// WorkflowNotFoundError indicates the workflow was not found.
type WorkflowNotFoundError struct {
	*VelocityError
	WorkflowKey uint64
}

// NewWorkflowNotFoundError creates a new WorkflowNotFoundError.
func NewWorkflowNotFoundError(workflowKey uint64) *WorkflowNotFoundError {
	return &WorkflowNotFoundError{
		VelocityError: NewVelocityError(
			fmt.Sprintf("Workflow not found: %d", workflowKey),
			CodeNotFound,
			false,
		),
		WorkflowKey: workflowKey,
	}
}

// WorkflowAlreadyCompletedError indicates the workflow is already completed.
type WorkflowAlreadyCompletedError struct {
	*VelocityError
	WorkflowKey uint64
}

// NewWorkflowAlreadyCompletedError creates a new WorkflowAlreadyCompletedError.
func NewWorkflowAlreadyCompletedError(workflowKey uint64) *WorkflowAlreadyCompletedError {
	return &WorkflowAlreadyCompletedError{
		VelocityError: NewVelocityError(
			fmt.Sprintf("Workflow already completed: %d", workflowKey),
			CodeAlreadyCompleted,
			false,
		),
		WorkflowKey: workflowKey,
	}
}

// ConnectionError indicates a connection failure.
type ConnectionError struct {
	*VelocityError
	Target string
}

// NewConnectionError creates a new ConnectionError.
func NewConnectionError(target string) *ConnectionError {
	return &ConnectionError{
		VelocityError: NewVelocityError(
			fmt.Sprintf("Failed to connect to %s", target),
			CodeConnection,
			true,
		),
		Target: target,
	}
}

// TimeoutError indicates an operation timed out.
type TimeoutError struct {
	*VelocityError
	Operation string
	TimeoutMs int
}

// NewTimeoutError creates a new TimeoutError.
func NewTimeoutError(operation string, timeoutMs int) *TimeoutError {
	return &TimeoutError{
		VelocityError: NewVelocityError(
			fmt.Sprintf("Operation '%s' timed out after %dms", operation, timeoutMs),
			CodeTimeout,
			true,
		),
		Operation: operation,
		TimeoutMs: timeoutMs,
	}
}

// RateLimitError indicates rate limit exceeded.
type RateLimitError struct {
	*VelocityError
	RetryAfterMs int
}

// NewRateLimitError creates a new RateLimitError.
func NewRateLimitError(retryAfterMs int) *RateLimitError {
	return &RateLimitError{
		VelocityError: NewVelocityError(
			"Rate limit exceeded",
			CodeRateLimit,
			true,
		),
		RetryAfterMs: retryAfterMs,
	}
}

// AuthenticationError indicates authentication failure.
type AuthenticationError struct {
	*VelocityError
}

// NewAuthenticationError creates a new AuthenticationError.
func NewAuthenticationError() *AuthenticationError {
	return &AuthenticationError{
		VelocityError: NewVelocityError(
			"Authentication failed",
			CodeAuthentication,
			false,
		),
	}
}

// InternalError indicates an internal server error.
type InternalError struct {
	*VelocityError
}

// NewInternalError creates a new InternalError.
func NewInternalError() *InternalError {
	return &InternalError{
		VelocityError: NewVelocityError(
			"Internal server error",
			CodeInternal,
			true,
		),
	}
}
