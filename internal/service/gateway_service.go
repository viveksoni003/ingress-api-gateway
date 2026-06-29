package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/observability"
	"go.uber.org/zap"
)

// GatewayConfig tunes the enqueue path.
type GatewayConfig struct {
	MaxRetries     int
	IdempotencyTTL time.Duration
}

// GatewayService implements domain.Enqueuer: it validates, de-duplicates,
// persists and queues jobs. The same path is used by HTTP handlers and by
// processors fanning out follow-up work.
type GatewayService struct {
	queue   domain.Queue
	jobs    domain.JobRepository
	cache   domain.Cache
	metrics *observability.Metrics
	log     *zap.Logger
	cfg     GatewayConfig
}

var _ domain.Enqueuer = (*GatewayService)(nil)

// NewGatewayService builds the service.
func NewGatewayService(q domain.Queue, jobs domain.JobRepository, cache domain.Cache, m *observability.Metrics, log *zap.Logger, cfg GatewayConfig) *GatewayService {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 24 * time.Hour
	}
	return &GatewayService{queue: q, jobs: jobs, cache: cache, metrics: m, log: log, cfg: cfg}
}

// Enqueue validates and queues a job. If an idempotency key (explicit or
// derived from the payload) was already seen, the pre-existing job is returned
// with Duplicate=true and no new job is created.
func (s *GatewayService) Enqueue(ctx context.Context, req domain.EnqueueRequest) (*domain.EnqueueResult, error) {
	if !req.JobType.Valid() {
		return nil, domain.ErrUnknownJobType
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		return nil, fmt.Errorf("%w: body must be valid JSON", domain.ErrInvalidPayload)
	}

	key := req.IdempotencyKey
	if key == "" {
		key = payloadHash(req.JobType, req.Payload)
	}
	redisKey := idempotencyRedisKey(req.JobType, key)

	traceID := req.TraceID
	if traceID == "" {
		traceID = uuid.NewString()
	}
	jobID := uuid.NewString()

	// Claim the idempotency key atomically. If it already exists this is a
	// duplicate request: return the original job id.
	fresh, err := s.cache.SetNX(ctx, redisKey, jobID, s.cfg.IdempotencyTTL)
	if err != nil {
		return nil, fmt.Errorf("idempotency claim: %w", err)
	}
	if !fresh {
		s.metrics.IdempotencyHits.Inc()
		existingID, _, _ := s.cache.Get(ctx, redisKey)
		if existingID == "" {
			existingID = jobID
		}
		// Best-effort: return the full persisted job; fall back to a stub.
		if existing, err := s.jobs.GetByID(ctx, existingID); err == nil {
			return &domain.EnqueueResult{Job: existing, Duplicate: true}, nil
		}
		return &domain.EnqueueResult{
			Job:       &domain.Job{ID: existingID, JobType: req.JobType, Status: domain.JobStatusQueued, IdempotencyKey: key, TraceID: traceID},
			Duplicate: true,
		}, nil
	}

	job := domain.NewJob(jobID, req.JobType, req.Payload, key, traceID, s.cfg.MaxRetries)

	if err := s.jobs.Create(ctx, job); err != nil {
		_ = s.cache.Del(ctx, redisKey) // release key so a retry can succeed
		return nil, fmt.Errorf("persist job: %w", err)
	}
	if err := s.queue.Push(ctx, job); err != nil {
		_ = s.cache.Del(ctx, redisKey)
		return nil, fmt.Errorf("queue job: %w", err)
	}

	s.log.Info("job enqueued",
		zap.String("job_id", job.ID),
		zap.String("job_type", string(job.JobType)),
		zap.String("priority", string(job.Priority)),
		zap.String("trace_id", job.TraceID))
	return &domain.EnqueueResult{Job: job, Duplicate: false}, nil
}
