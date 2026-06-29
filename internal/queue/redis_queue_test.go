package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/testutil"
)

func newJob(id string, p domain.Priority) *domain.Job {
	return &domain.Job{ID: id, JobType: domain.JobTypeNotification, Priority: p, Payload: []byte(`{}`)}
}

func TestQueuePushPopPriorityOrder(t *testing.T) {
	q := NewRedisQueue(testutil.NewMiniRedis(t), 100*time.Millisecond)
	ctx := context.Background()

	// Push out of priority order; expect HIGH, then MEDIUM, then LOW.
	require.NoError(t, q.Push(ctx, newJob("low", domain.PriorityLow)))
	require.NoError(t, q.Push(ctx, newJob("medium", domain.PriorityMedium)))
	require.NoError(t, q.Push(ctx, newJob("high", domain.PriorityHigh)))

	for _, want := range []string{"high", "medium", "low"} {
		job, err := q.Pop(ctx)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.Equal(t, want, job.ID)
	}
}

func TestQueuePopTimeoutReturnsNil(t *testing.T) {
	q := NewRedisQueue(testutil.NewMiniRedis(t), 50*time.Millisecond)
	job, err := q.Pop(context.Background())
	require.NoError(t, err)
	require.Nil(t, job, "empty queue should return nil after timeout")
}

func TestQueueFIFOWithinPriority(t *testing.T) {
	q := NewRedisQueue(testutil.NewMiniRedis(t), 100*time.Millisecond)
	ctx := context.Background()

	require.NoError(t, q.Push(ctx, newJob("first", domain.PriorityHigh)))
	require.NoError(t, q.Push(ctx, newJob("second", domain.PriorityHigh)))

	j1, _ := q.Pop(ctx)
	j2, _ := q.Pop(ctx)
	require.Equal(t, "first", j1.ID)
	require.Equal(t, "second", j2.ID)
}

func TestQueueDeadLetter(t *testing.T) {
	q := NewRedisQueue(testutil.NewMiniRedis(t), 100*time.Millisecond)
	ctx := context.Background()

	require.NoError(t, q.PushDeadLetter(ctx, newJob("dead-1", domain.PriorityHigh)))
	require.NoError(t, q.PushDeadLetter(ctx, newJob("dead-2", domain.PriorityHigh)))

	depth, err := q.DeadLetterDepth(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), depth)

	jobs, err := q.ListDeadLetter(ctx, 10)
	require.NoError(t, err)
	require.Len(t, jobs, 2)
}

func TestQueueTotalDepth(t *testing.T) {
	q := NewRedisQueue(testutil.NewMiniRedis(t), 100*time.Millisecond)
	ctx := context.Background()

	require.NoError(t, q.Push(ctx, newJob("a", domain.PriorityHigh)))
	require.NoError(t, q.Push(ctx, newJob("b", domain.PriorityMedium)))
	require.NoError(t, q.Push(ctx, newJob("c", domain.PriorityLow)))

	total, err := q.TotalDepth(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
}
