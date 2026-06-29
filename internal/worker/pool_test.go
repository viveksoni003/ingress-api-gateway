package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viveksoni003/ingress-api-gateway/internal/cache"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/observability"
	"github.com/viveksoni003/ingress-api-gateway/internal/queue"
	"github.com/viveksoni003/ingress-api-gateway/internal/testutil"
	"go.uber.org/zap"
)

// flakyProcessor fails the first failUntil attempts then succeeds.
type flakyProcessor struct {
	failUntil int
	mu        sync.Mutex
	attempts  int
}

func (p *flakyProcessor) Type() domain.JobType { return domain.JobTypeNotification }

func (p *flakyProcessor) Process(_ context.Context, _ *domain.Job) error {
	p.mu.Lock()
	p.attempts++
	n := p.attempts
	p.mu.Unlock()
	if n <= p.failUntil {
		return errors.New("simulated transient failure")
	}
	return nil
}

func (p *flakyProcessor) Attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts
}

func newTestPool(t *testing.T, proc domain.Processor) (*Pool, *testutil.InMemoryStore, domain.Queue) {
	t.Helper()
	rdb := testutil.NewMiniRedis(t)
	q := queue.NewRedisQueue(rdb, 50*time.Millisecond)
	c := cache.NewRedisCache(rdb)
	store := testutil.NewInMemoryStore()
	pool := NewPool(PoolDeps{
		Queue:       q,
		Jobs:        store,
		Cache:       c,
		Metrics:     observability.New(),
		Logger:      zap.NewNop(),
		Processors:  []domain.Processor{proc},
		WorkerCount: 2,
		RetryBase:   5 * time.Millisecond,
		RetryMax:    20 * time.Millisecond,
	})
	return pool, store, q
}

func TestWorkerRetriesThenSucceeds(t *testing.T) {
	proc := &flakyProcessor{failUntil: 2} // fail twice, succeed on 3rd
	pool, store, q := newTestPool(t, proc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := domain.NewJob("retry-job", domain.JobTypeNotification, []byte(`{"channel":"EMAIL","recipient":"a@x.com"}`), "idem", "trace", 5)
	require.NoError(t, store.Create(ctx, job))
	require.NoError(t, q.Push(ctx, job))

	pool.Start(ctx)

	require.Eventually(t, func() bool {
		j, err := store.GetByID(context.Background(), "retry-job")
		return err == nil && j.Status == domain.JobStatusSuccess
	}, 3*time.Second, 10*time.Millisecond, "job should eventually succeed")

	require.GreaterOrEqual(t, proc.Attempts(), 3, "should have retried before succeeding")
}

func TestWorkerDeadLettersAfterMaxRetries(t *testing.T) {
	proc := &flakyProcessor{failUntil: 1000} // always fail
	pool, store, q := newTestPool(t, proc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := domain.NewJob("dead-job", domain.JobTypeNotification, []byte(`{"channel":"EMAIL","recipient":"a@x.com"}`), "idem", "trace", 2)
	require.NoError(t, store.Create(ctx, job))
	require.NoError(t, q.Push(ctx, job))

	pool.Start(ctx)

	require.Eventually(t, func() bool {
		j, err := store.GetByID(context.Background(), "dead-job")
		return err == nil && j.Status == domain.JobStatusDeadLetter
	}, 3*time.Second, 10*time.Millisecond, "job should be dead-lettered")

	depth, err := q.DeadLetterDepth(context.Background())
	require.NoError(t, err)
	require.GreaterOrEqual(t, depth, int64(1), "dead-letter queue should contain the job")
}

// drainProcessor signals when it starts and runs to completion regardless of
// context, so we can verify graceful shutdown waits for in-flight work.
type drainProcessor struct {
	started  chan struct{}
	finished int32
}

func (p *drainProcessor) Type() domain.JobType { return domain.JobTypeNotification }

func (p *drainProcessor) Process(_ context.Context, _ *domain.Job) error {
	select {
	case p.started <- struct{}{}:
	default:
	}
	time.Sleep(150 * time.Millisecond) // simulate in-flight work
	atomic.StoreInt32(&p.finished, 1)
	return nil
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	proc := &drainProcessor{started: make(chan struct{}, 1)}
	pool, store, q := newTestPool(t, proc)
	ctx, cancel := context.WithCancel(context.Background())

	job := domain.NewJob("drain-job", domain.JobTypeNotification, []byte(`{}`), "idem", "trace", 1)
	require.NoError(t, store.Create(ctx, job))
	require.NoError(t, q.Push(ctx, job))

	pool.Start(ctx)

	// Wait until the job is actually being processed, then trigger shutdown.
	select {
	case <-proc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("processor never started")
	}
	cancel() // signal shutdown while a job is in flight

	shutdownCtx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	defer sc()
	require.NoError(t, pool.Shutdown(shutdownCtx), "shutdown should drain cleanly")
	require.Equal(t, int32(1), atomic.LoadInt32(&proc.finished), "in-flight job must finish before shutdown returns")
}

func TestGracefulShutdownWithNoWorkReturnsQuickly(t *testing.T) {
	pool, _, _ := newTestPool(t, &flakyProcessor{})
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	cancel()

	shutdownCtx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	defer sc()
	require.NoError(t, pool.Shutdown(shutdownCtx))
}
