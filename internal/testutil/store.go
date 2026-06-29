// Package testutil provides in-memory fakes used by unit and integration tests
// so the suite can run without a real Postgres instance. It is intentionally
// not guarded by a build tag so it can be imported from any test package.
package testutil

import (
	"context"
	"sort"
	"sync"

	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// InMemoryStore is a thread-safe, in-memory implementation of domain.Store.
type InMemoryStore struct {
	mu            sync.Mutex
	jobs          map[string]*domain.Job
	Attempts      []*domain.JobAttempt
	Registrations []*domain.Registration
	Payments      []*domain.PaymentEvent
	Scans         []*domain.QRScanEvent
	Notifications []*domain.NotificationEvent
	Audits        []*domain.AuditLog
}

var _ domain.Store = (*InMemoryStore)(nil)

// NewInMemoryStore builds an empty store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{jobs: make(map[string]*domain.Job)}
}

func cloneJob(j *domain.Job) *domain.Job {
	cp := *j
	return &cp
}

// Create stores a copy of the job.
func (s *InMemoryStore) Create(_ context.Context, j *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = cloneJob(j)
	return nil
}

// Update replaces an existing job.
func (s *InMemoryStore) Update(_ context.Context, j *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.ID]; !ok {
		return domain.ErrNotFound
	}
	s.jobs[j.ID] = cloneJob(j)
	return nil
}

// GetByID returns a copy of the job or ErrNotFound.
func (s *InMemoryStore) GetByID(_ context.Context, id string) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return cloneJob(j), nil
}

// List returns jobs matching the filter, newest first.
func (s *InMemoryStore) List(_ context.Context, f domain.JobFilter) ([]*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []*domain.Job
	for _, j := range s.jobs {
		if f.Status != "" && j.Status != f.Status {
			continue
		}
		if f.JobType != "" && j.JobType != f.JobType {
			continue
		}
		out = append(out, cloneJob(j))
	}
	sort.Slice(out, func(i, k int) bool { return out[i].CreatedAt.After(out[k].CreatedAt) })

	if f.Offset > 0 && f.Offset < len(out) {
		out = out[f.Offset:]
	} else if f.Offset >= len(out) {
		out = nil
	}
	if f.Limit > 0 && f.Limit < len(out) {
		out = out[:f.Limit]
	}
	return out, nil
}

// ListDeadLetter returns dead-lettered jobs.
func (s *InMemoryStore) ListDeadLetter(ctx context.Context, limit, offset int) ([]*domain.Job, error) {
	return s.List(ctx, domain.JobFilter{Status: domain.JobStatusDeadLetter, Limit: limit, Offset: offset})
}

// CountByStatus groups jobs by status.
func (s *InMemoryStore) CountByStatus(_ context.Context) (map[domain.JobStatus]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[domain.JobStatus]int64{}
	for _, j := range s.jobs {
		out[j.Status]++
	}
	return out, nil
}

// CountByType groups jobs by type.
func (s *InMemoryStore) CountByType(_ context.Context) (map[domain.JobType]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[domain.JobType]int64{}
	for _, j := range s.jobs {
		out[j.JobType]++
	}
	return out, nil
}

// AddAttempt records an attempt.
func (s *InMemoryStore) AddAttempt(_ context.Context, a *domain.JobAttempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Attempts = append(s.Attempts, a)
	return nil
}

// Ping always succeeds.
func (s *InMemoryStore) Ping(_ context.Context) error { return nil }

// CreateRegistration appends a registration.
func (s *InMemoryStore) CreateRegistration(_ context.Context, r *domain.Registration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Registrations = append(s.Registrations, r)
	return nil
}

// UpsertPaymentEvent appends/updates a payment event by gateway order id.
func (s *InMemoryStore) UpsertPaymentEvent(_ context.Context, p *domain.PaymentEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, ex := range s.Payments {
		if ex.GatewayOrderID == p.GatewayOrderID {
			s.Payments[i] = p
			return nil
		}
	}
	s.Payments = append(s.Payments, p)
	return nil
}

// CreateQRScanEvent appends a scan event.
func (s *InMemoryStore) CreateQRScanEvent(_ context.Context, e *domain.QRScanEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Scans = append(s.Scans, e)
	return nil
}

// CreateNotificationEvent appends a notification event.
func (s *InMemoryStore) CreateNotificationEvent(_ context.Context, n *domain.NotificationEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Notifications = append(s.Notifications, n)
	return nil
}

// CreateAuditLog appends an audit log.
func (s *InMemoryStore) CreateAuditLog(_ context.Context, l *domain.AuditLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Audits = append(s.Audits, l)
	return nil
}

// Counts returns convenience counters for assertions.
func (s *InMemoryStore) JobCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.jobs)
}
