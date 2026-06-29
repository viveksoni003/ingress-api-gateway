package service

import (
	"context"
	"fmt"
	"time"

	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"go.uber.org/zap"
)

// AdminService backs the admin API: inspecting jobs, stats, manual retries and
// dead-letter listing.
type AdminService struct {
	queue domain.Queue
	jobs  domain.JobRepository
	log   *zap.Logger
}

// NewAdminService builds the service.
func NewAdminService(q domain.Queue, jobs domain.JobRepository, log *zap.Logger) *AdminService {
	return &AdminService{queue: q, jobs: jobs, log: log}
}

// ListJobs returns jobs matching the filter.
func (s *AdminService) ListJobs(ctx context.Context, f domain.JobFilter) ([]*domain.Job, error) {
	return s.jobs.List(ctx, f)
}

// GetJob returns a single job by id.
func (s *AdminService) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	return s.jobs.GetByID(ctx, id)
}

// ListDeadLetter returns dead-lettered jobs from the database.
func (s *AdminService) ListDeadLetter(ctx context.Context, limit, offset int) ([]*domain.Job, error) {
	return s.jobs.ListDeadLetter(ctx, limit, offset)
}

// Stats aggregates job counts and live queue depths.
func (s *AdminService) Stats(ctx context.Context) (*domain.JobStats, error) {
	byStatus, err := s.jobs.CountByStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}
	byType, err := s.jobs.CountByType(ctx)
	if err != nil {
		return nil, fmt.Errorf("count by type: %w", err)
	}

	var total int64
	for _, n := range byStatus {
		total += n
	}

	depths := map[domain.Priority]int64{}
	for _, p := range []domain.Priority{domain.PriorityHigh, domain.PriorityMedium, domain.PriorityLow} {
		if d, err := s.queue.Depth(ctx, p); err == nil {
			depths[p] = d
		}
	}
	dlqDepth, _ := s.queue.DeadLetterDepth(ctx)

	return &domain.JobStats{
		ByStatus:    byStatus,
		ByType:      byType,
		Total:       total,
		DeadLetter:  dlqDepth,
		QueueDepths: depths,
		GeneratedAt: time.Now().UTC(),
	}, nil
}

// RetryJob re-queues a FAILED or DEAD_LETTER job with a fresh retry budget.
func (s *AdminService) RetryJob(ctx context.Context, id string) (*domain.Job, error) {
	job, err := s.jobs.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if job.Status != domain.JobStatusFailed && job.Status != domain.JobStatusDeadLetter {
		return nil, fmt.Errorf("%w: job %s is in state %s and cannot be retried", domain.ErrInvalidPayload, id, job.Status)
	}

	job.Status = domain.JobStatusQueued
	job.RetryCount = 0
	job.ErrorMessage = ""
	job.ProcessedAt = nil
	job.UpdatedAt = time.Now().UTC()

	if err := s.jobs.Update(ctx, job); err != nil {
		return nil, fmt.Errorf("update job: %w", err)
	}
	if err := s.queue.Push(ctx, job); err != nil {
		return nil, fmt.Errorf("requeue job: %w", err)
	}
	s.log.Info("job manually re-queued", zap.String("job_id", id))
	return job, nil
}
