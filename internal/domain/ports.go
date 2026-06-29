package domain

import (
	"context"
	"encoding/json"
	"time"
)

// ---------------------------------------------------------------------------
// Ports (interfaces). Concrete adapters live in internal/queue, internal/cache,
// internal/repository, internal/ratelimiter and internal/worker. Defining them
// here keeps the dependency arrows pointing inward (clean / hexagonal).
// ---------------------------------------------------------------------------

// Queue is the priority job queue abstraction backed by Redis lists.
type Queue interface {
	// Push enqueues a job onto the list matching its Priority.
	Push(ctx context.Context, job *Job) error
	// Pop blocks until a job is available or the configured timeout elapses,
	// consuming in priority order HIGH -> MEDIUM -> LOW. On timeout it returns
	// (nil, nil) so the worker can re-check its context and loop.
	Pop(ctx context.Context) (*Job, error)
	// PushDeadLetter moves a permanently failed job to the dead-letter list.
	PushDeadLetter(ctx context.Context, job *Job) error
	// Depth returns the length of a single priority queue.
	Depth(ctx context.Context, p Priority) (int64, error)
	// TotalDepth returns the combined length of all priority queues (used for
	// backpressure / load shedding decisions).
	TotalDepth(ctx context.Context) (int64, error)
	// DeadLetterDepth returns the dead-letter queue length.
	DeadLetterDepth(ctx context.Context) (int64, error)
	// ListDeadLetter returns up to limit jobs from the dead-letter queue.
	ListDeadLetter(ctx context.Context, limit int64) ([]*Job, error)
	// Ping verifies connectivity (used by /ready).
	Ping(ctx context.Context) error
}

// Cache is the Redis key/value abstraction used for idempotency keys, QR
// de-duplication and live counters.
type Cache interface {
	// SetNX sets key only if it does not exist; returns true when it was set.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (value string, found bool, err error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Incr(ctx context.Context, key string) (int64, error)
	Del(ctx context.Context, key string) error
	Ping(ctx context.Context) error
}

// RateLimiter is a synchronous, in-memory token-bucket limiter. Allow reports
// whether a request identified by key may proceed right now.
type RateLimiter interface {
	Allow(key string) bool
}

// ---------------------------------------------------------------------------
// Repository ports. A single Postgres-backed type in internal/repository
// implements all of these.
// ---------------------------------------------------------------------------

// JobRepository persists job state and attempts.
type JobRepository interface {
	Create(ctx context.Context, job *Job) error
	Update(ctx context.Context, job *Job) error
	GetByID(ctx context.Context, id string) (*Job, error)
	List(ctx context.Context, f JobFilter) ([]*Job, error)
	ListDeadLetter(ctx context.Context, limit, offset int) ([]*Job, error)
	CountByStatus(ctx context.Context) (map[JobStatus]int64, error)
	CountByType(ctx context.Context) (map[JobType]int64, error)
	AddAttempt(ctx context.Context, a *JobAttempt) error
	Ping(ctx context.Context) error
}

// RegistrationRepository persists attendee registrations.
type RegistrationRepository interface {
	CreateRegistration(ctx context.Context, r *Registration) error
}

// PaymentRepository persists verified payment webhooks.
type PaymentRepository interface {
	UpsertPaymentEvent(ctx context.Context, p *PaymentEvent) error
}

// QRScanRepository persists QR scan events.
type QRScanRepository interface {
	CreateQRScanEvent(ctx context.Context, e *QRScanEvent) error
}

// NotificationRepository persists notification delivery results.
type NotificationRepository interface {
	CreateNotificationEvent(ctx context.Context, n *NotificationEvent) error
}

// AuditRepository persists audit-log rows.
type AuditRepository interface {
	CreateAuditLog(ctx context.Context, l *AuditLog) error
}

// Store is the union of all repository ports, implemented by the Postgres
// adapter. Components depend on the narrow interface they actually need.
type Store interface {
	JobRepository
	RegistrationRepository
	PaymentRepository
	QRScanRepository
	NotificationRepository
	AuditRepository
}

// ---------------------------------------------------------------------------
// Enqueue + processing ports.
// ---------------------------------------------------------------------------

// EnqueueRequest is the input to the gateway's Enqueue use-case.
type EnqueueRequest struct {
	JobType        JobType
	Payload        json.RawMessage
	IdempotencyKey string // optional; derived from payload hash when empty
	TraceID        string
}

// EnqueueResult reports the created (or pre-existing) job.
type EnqueueResult struct {
	Job       *Job
	Duplicate bool // true when an idempotent duplicate was detected
}

// Enqueuer accepts, validates, de-duplicates, persists and queues a job. It is
// used both by HTTP handlers and by processors that fan out follow-up work
// (e.g. registration -> notification).
type Enqueuer interface {
	Enqueue(ctx context.Context, req EnqueueRequest) (*EnqueueResult, error)
}

// Processor handles exactly one JobType. Returning a nil error marks the job
// SUCCESS; returning ErrNonRetryable (or a wrapped form) sends it straight to
// the dead-letter queue; any other error triggers retry with backoff.
type Processor interface {
	Type() JobType
	Process(ctx context.Context, job *Job) error
}
