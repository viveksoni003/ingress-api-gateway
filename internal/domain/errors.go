package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors used across layers. Handlers translate these into HTTP
// status codes; processors use them to decide retry vs. dead-letter.
var (
	ErrNotFound         = errors.New("resource not found")
	ErrDuplicate        = errors.New("duplicate resource")
	ErrInvalidPayload   = errors.New("invalid payload")
	ErrInvalidSignature = errors.New("invalid signature")
	ErrQueueFull        = errors.New("queue is full (backpressure)")
	ErrRateLimited      = errors.New("rate limit exceeded")
	ErrDuplicateScan    = errors.New("duplicate qr scan")
	ErrUnknownJobType   = errors.New("unknown job type")
	ErrNonRetryable     = errors.New("non-retryable error")
)

// ValidationError describes a single failed field-level validation. It is a
// dedicated type so the API layer can render a 400 with the offending field.
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed on %q: %s", e.Field, e.Message)
}

// NewValidationError is a small constructor helper.
func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{Field: field, Message: message}
}
