// Package worker implements the background processing tier: a configurable
// pool of goroutines that consume jobs from the Redis priority queue, dispatch
// them to per-type processors, and apply retries with exponential backoff,
// dead-lettering and a Redis duplicate-processing guard.
//
// Topology:
//
//	Redis queues ──BRPOP──> dispatcher goroutine ──chan──> N worker goroutines
//
// The single dispatcher centralises the blocking Redis pop and fans work out
// over a buffered channel, which is the idiomatic Go worker-pool pattern and
// makes graceful shutdown a matter of cancelling one context and closing one
// channel.
package worker

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/observability"
	"go.uber.org/zap"
)

const (
	// processedTTL is how long a successful job is remembered so a duplicate
	// delivery is not processed twice.
	processedTTL = 24 * time.Hour
	// lockTTL bounds how long a processing lock is held if a worker dies mid-job.
	lockTTL = 2 * time.Minute
)

// PoolDeps are the dependencies required to build a Pool.
type PoolDeps struct {
	Queue      domain.Queue
	Jobs       domain.JobRepository
	Cache      domain.Cache
	Metrics    *observability.Metrics
	Logger     *zap.Logger
	Processors []domain.Processor
	WorkerCount int
	RetryBase   time.Duration
	RetryMax    time.Duration
}

// Pool is the worker pool.
type Pool struct {
	queue   domain.Queue
	jobsRepo domain.JobRepository
	cache   domain.Cache
	metrics *observability.Metrics
	log     *zap.Logger

	registry    map[domain.JobType]domain.Processor
	workerCount int
	retryBase   time.Duration
	retryMax    time.Duration

	jobs         chan *domain.Job
	workers      sync.WaitGroup // worker goroutines
	dispatcherWG sync.WaitGroup // the single dispatcher goroutine
	retryWG      sync.WaitGroup // delayed-retry goroutines
}

// NewPool wires a pool from its dependencies, indexing processors by type.
func NewPool(d PoolDeps) *Pool {
	if d.WorkerCount <= 0 {
		d.WorkerCount = 4
	}
	reg := make(map[domain.JobType]domain.Processor, len(d.Processors))
	for _, p := range d.Processors {
		reg[p.Type()] = p
	}
	return &Pool{
		queue:       d.Queue,
		jobsRepo:    d.Jobs,
		cache:       d.Cache,
		metrics:     d.Metrics,
		log:         d.Logger,
		registry:    reg,
		workerCount: d.WorkerCount,
		retryBase:   d.RetryBase,
		retryMax:    d.RetryMax,
		jobs:        make(chan *domain.Job, d.WorkerCount*2),
	}
}

// Start launches the dispatcher and worker goroutines. It returns immediately;
// cancel ctx and then call Shutdown to stop.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workerCount; i++ {
		p.workers.Add(1)
		go p.worker(ctx, i)
	}
	p.dispatcherWG.Add(1)
	go p.dispatch(ctx)
	p.log.Info("worker pool started", zap.Int("workers", p.workerCount))
}

// Shutdown waits for the dispatcher and all workers to drain in-flight work,
// bounded by ctx. Cancel the Start context first to begin draining.
func (p *Pool) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		// Order matters: once all workers have returned, no new retry goroutines
		// can be spawned, so retryWG.Wait() never races with a concurrent Add.
		p.dispatcherWG.Wait() // dispatcher stopped & jobs channel closed
		p.workers.Wait()      // all in-flight jobs finished
		p.retryWG.Wait()      // scheduled retries drained (return fast on cancel)
		close(done)
	}()

	select {
	case <-done:
		p.log.Info("worker pool drained cleanly")
		return nil
	case <-ctx.Done():
		p.log.Warn("worker pool shutdown timed out; some jobs may be retried later")
		return ctx.Err()
	}
}

// dispatch is the single goroutine that performs the blocking Redis pop and
// feeds the worker channel. On context cancellation it closes the channel so
// workers exit after draining.
func (p *Pool) dispatch(ctx context.Context) {
	defer p.dispatcherWG.Done()
	defer close(p.jobs)

	for {
		if ctx.Err() != nil {
			return
		}
		job, err := p.queue.Pop(ctx)
		if err != nil {
			p.log.Error("queue pop failed", zap.Error(err))
			select {
			case <-time.After(200 * time.Millisecond): // avoid hot-loop on errors
			case <-ctx.Done():
				return
			}
			continue
		}
		if job == nil {
			continue // poll timeout, loop and re-check ctx
		}
		select {
		case p.jobs <- job:
		case <-ctx.Done():
			// Popped a job but shutting down: requeue so it is not lost.
			_ = p.queue.Push(context.Background(), job)
			return
		}
	}
}

// worker consumes jobs until the channel is closed.
func (p *Pool) worker(ctx context.Context, id int) {
	defer p.workers.Done()
	for job := range p.jobs {
		p.process(ctx, job, id)
	}
}

// process runs a single job through its processor and applies the
// success / retry / dead-letter state machine.
func (p *Pool) process(ctx context.Context, job *domain.Job, workerID int) {
	p.metrics.WorkersActive.Inc()
	defer p.metrics.WorkersActive.Dec()

	start := time.Now()
	log := p.log.With(
		zap.String("job_id", job.ID),
		zap.String("job_type", string(job.JobType)),
		zap.String("trace_id", job.TraceID),
		zap.Int("worker", workerID),
		zap.Int("attempt", job.RetryCount+1),
	)

	proc, ok := p.registry[job.JobType]
	if !ok {
		log.Error("no processor registered for job type")
		p.toDeadLetter(ctx, job, "no processor registered")
		return
	}

	// Duplicate-processing guards.
	if done, _, _ := p.cache.Get(ctx, "job:done:"+job.ID); done != "" {
		log.Info("job already processed; skipping duplicate")
		return
	}
	acquired, _ := p.cache.SetNX(ctx, "job:lock:"+job.ID, "1", lockTTL)
	if !acquired {
		log.Info("job locked by another worker; skipping duplicate")
		return
	}
	defer func() { _ = p.cache.Del(ctx, "job:lock:"+job.ID) }()

	// Mark PROCESSING.
	job.Status = domain.JobStatusProcessing
	job.UpdatedAt = time.Now().UTC()
	if err := p.jobsRepo.Update(ctx, job); err != nil {
		log.Warn("failed to mark job processing", zap.Error(err))
	}

	// Run the processor.
	err := proc.Process(ctx, job)
	dur := time.Since(start)

	attempt := &domain.JobAttempt{
		JobID:      job.ID,
		Attempt:    job.RetryCount + 1,
		DurationMS: dur.Milliseconds(),
		CreatedAt:  time.Now().UTC(),
	}

	if err == nil {
		now := time.Now().UTC()
		job.Status = domain.JobStatusSuccess
		job.ProcessedAt = &now
		job.UpdatedAt = now
		job.ErrorMessage = ""
		_ = p.jobsRepo.Update(ctx, job)

		_ = p.cache.Set(ctx, "job:done:"+job.ID, "1", processedTTL)

		attempt.Status = domain.JobStatusSuccess
		_ = p.jobsRepo.AddAttempt(ctx, attempt)

		p.metrics.JobsProcessed.WithLabelValues(string(job.JobType), string(domain.JobStatusSuccess)).Inc()
		log.Info("job processed", zap.Duration("latency", dur))
		return
	}

	// Failure path.
	p.metrics.JobsFailed.WithLabelValues(string(job.JobType)).Inc()
	attempt.Status = domain.JobStatusFailed
	attempt.Error = err.Error()
	_ = p.jobsRepo.AddAttempt(ctx, attempt)
	log.Warn("job processing failed", zap.Error(err), zap.Duration("latency", dur))

	// Non-retryable or out of retry budget -> dead-letter.
	if errors.Is(err, domain.ErrNonRetryable) || !job.CanRetry() {
		p.toDeadLetter(ctx, job, err.Error())
		return
	}

	// Retry with exponential backoff + jitter.
	job.RetryCount++
	job.Status = domain.JobStatusQueued
	job.ErrorMessage = err.Error()
	job.UpdatedAt = time.Now().UTC()
	_ = p.jobsRepo.Update(ctx, job)

	delay := p.backoff(job.RetryCount)
	p.metrics.JobsRetried.WithLabelValues(string(job.JobType)).Inc()
	log.Info("scheduling retry", zap.Int("retry_count", job.RetryCount), zap.Duration("delay", delay))

	p.retryWG.Add(1)
	go p.requeueAfter(ctx, job, delay)
}

// requeueAfter pushes a job back onto the queue after delay, respecting
// shutdown (on cancellation it requeues immediately so the job is never lost).
func (p *Pool) requeueAfter(ctx context.Context, job *domain.Job, delay time.Duration) {
	defer p.retryWG.Done()

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		if err := p.queue.Push(ctx, job); err != nil {
			_ = p.queue.Push(context.Background(), job)
		}
	case <-ctx.Done():
		_ = p.queue.Push(context.Background(), job) // best-effort durable requeue
	}
}

// toDeadLetter moves a permanently failed job to the dead-letter queue and
// records the terminal status.
func (p *Pool) toDeadLetter(ctx context.Context, job *domain.Job, reason string) {
	now := time.Now().UTC()
	job.Status = domain.JobStatusDeadLetter
	job.ErrorMessage = reason
	job.UpdatedAt = now
	job.ProcessedAt = &now
	_ = p.jobsRepo.Update(ctx, job)

	if err := p.queue.PushDeadLetter(ctx, job); err != nil {
		p.log.Error("failed to push job to dead-letter queue", zap.String("job_id", job.ID), zap.Error(err))
	}
	_ = p.cache.Set(ctx, "job:done:"+job.ID, "1", processedTTL)

	p.metrics.JobsDeadLetter.WithLabelValues(string(job.JobType)).Inc()
	p.metrics.JobsProcessed.WithLabelValues(string(job.JobType), string(domain.JobStatusDeadLetter)).Inc()
	p.log.Warn("job moved to dead-letter queue", zap.String("job_id", job.ID), zap.String("reason", reason))
}

// backoff computes a "full jitter" exponential backoff capped at retryMax:
//
//	base * 2^(attempt-1), capped, then a uniform random in [0, capped].
func (p *Pool) backoff(attempt int) time.Duration {
	if p.retryBase <= 0 {
		p.retryBase = 500 * time.Millisecond
	}
	if p.retryMax <= 0 {
		p.retryMax = 30 * time.Second
	}
	exp := float64(p.retryBase) * math.Pow(2, float64(attempt-1))
	capped := math.Min(exp, float64(p.retryMax))
	return time.Duration(rand.Int63n(int64(capped) + 1))
}
