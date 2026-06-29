// Package domain holds the core business entities, enums and port interfaces
// for the gateway. It depends only on the standard library so that every other
// layer (queue, cache, repository, worker, api) can import it without creating
// import cycles. This is the "hexagonal core" of the clean architecture.
package domain

import (
	"encoding/json"
	"time"
)

// JobType identifies the kind of work a job represents. Each traffic type that
// the gateway accepts maps to exactly one JobType.
type JobType string

const (
	JobTypeRegistration   JobType = "REGISTRATION"
	JobTypePaymentWebhook JobType = "PAYMENT_WEBHOOK"
	JobTypeQRScan         JobType = "QR_SCAN"
	JobTypeNotification   JobType = "NOTIFICATION"
)

// Valid reports whether the JobType is one of the known values.
func (t JobType) Valid() bool {
	switch t {
	case JobTypeRegistration, JobTypePaymentWebhook, JobTypeQRScan, JobTypeNotification:
		return true
	default:
		return false
	}
}

// JobStatus is the lifecycle state of a job as it moves through the system.
type JobStatus string

const (
	JobStatusQueued     JobStatus = "QUEUED"
	JobStatusProcessing JobStatus = "PROCESSING"
	JobStatusSuccess    JobStatus = "SUCCESS"
	JobStatusFailed     JobStatus = "FAILED"
	JobStatusDeadLetter JobStatus = "DEAD_LETTER"
)

// Priority controls which Redis queue a job is pushed to and therefore the
// order in which workers consume it.
type Priority string

const (
	PriorityLow    Priority = "LOW"
	PriorityMedium Priority = "MEDIUM"
	PriorityHigh   Priority = "HIGH"
)

// PriorityForType maps a traffic type to its processing priority. Payment
// webhooks and QR scans are latency sensitive (money + live attendance), so
// they are HIGH; registrations are MEDIUM; notifications are best-effort LOW.
func PriorityForType(t JobType) Priority {
	switch t {
	case JobTypePaymentWebhook, JobTypeQRScan:
		return PriorityHigh
	case JobTypeRegistration:
		return PriorityMedium
	case JobTypeNotification:
		return PriorityLow
	default:
		return PriorityMedium
	}
}

// Job is the generic unit of asynchronous work. The same envelope is used for
// every traffic type; the concrete request lives in Payload as raw JSON so the
// queue and repository never need to know the specific shape.
type Job struct {
	ID             string          `json:"id"`
	JobType        JobType         `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	Status         JobStatus       `json:"status"`
	RetryCount     int             `json:"retry_count"`
	MaxRetries     int             `json:"max_retries"`
	Priority       Priority        `json:"priority"`
	IdempotencyKey string          `json:"idempotency_key"`
	TraceID        string          `json:"trace_id"`
	ErrorMessage   string          `json:"error_message,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	ProcessedAt    *time.Time      `json:"processed_at,omitempty"`
}

// NewJob constructs a QUEUED job with sensible defaults. The caller supplies a
// pre-generated id/idempotency key/trace id so the same values can be logged
// and returned to the client before the job is persisted.
func NewJob(id string, t JobType, payload json.RawMessage, idempotencyKey, traceID string, maxRetries int) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:             id,
		JobType:        t,
		Payload:        payload,
		Status:         JobStatusQueued,
		RetryCount:     0,
		MaxRetries:     maxRetries,
		Priority:       PriorityForType(t),
		IdempotencyKey: idempotencyKey,
		TraceID:        traceID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// CanRetry reports whether the job has retry budget remaining.
func (j *Job) CanRetry() bool {
	return j.RetryCount < j.MaxRetries
}

// JobAttempt records a single processing attempt for auditing/observability.
type JobAttempt struct {
	ID         int64     `json:"id"`
	JobID      string    `json:"job_id"`
	Attempt    int       `json:"attempt"`
	Status     JobStatus `json:"status"`
	Error      string    `json:"error,omitempty"`
	DurationMS int64     `json:"duration_ms"`
	CreatedAt  time.Time `json:"created_at"`
}

// JobStats is an aggregate view used by the admin stats endpoint.
type JobStats struct {
	ByStatus     map[JobStatus]int64 `json:"by_status"`
	ByType       map[JobType]int64   `json:"by_type"`
	Total        int64               `json:"total"`
	DeadLetter   int64               `json:"dead_letter"`
	QueueDepths  map[Priority]int64  `json:"queue_depths"`
	GeneratedAt  time.Time           `json:"generated_at"`
}

// JobFilter is the set of optional filters for listing jobs.
type JobFilter struct {
	Status  JobStatus
	JobType JobType
	Limit   int
	Offset  int
}
