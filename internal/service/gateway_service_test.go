package service

import (
	"context"
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

func newTestService(t *testing.T) (*GatewayService, *testutil.InMemoryStore, domain.Queue) {
	t.Helper()
	rdb := testutil.NewMiniRedis(t)
	q := queue.NewRedisQueue(rdb, 50*time.Millisecond)
	c := cache.NewRedisCache(rdb)
	store := testutil.NewInMemoryStore()
	svc := NewGatewayService(q, store, c, observability.New(), zap.NewNop(), GatewayConfig{
		MaxRetries:     3,
		IdempotencyTTL: time.Hour,
	})
	return svc, store, q
}

func TestEnqueueCreatesAndQueuesJob(t *testing.T) {
	svc, store, q := newTestService(t)
	ctx := context.Background()

	res, err := svc.Enqueue(ctx, domain.EnqueueRequest{
		JobType:        domain.JobTypeRegistration,
		Payload:        []byte(`{"event_id":"e1","attendee_name":"A","email":"a@x.com"}`),
		IdempotencyKey: "reg-1",
	})
	require.NoError(t, err)
	require.False(t, res.Duplicate)
	require.Equal(t, domain.PriorityMedium, res.Job.Priority)
	require.Equal(t, 1, store.JobCount())

	// Job should be on the queue.
	popped, err := q.Pop(ctx)
	require.NoError(t, err)
	require.Equal(t, res.Job.ID, popped.ID)
}

func TestEnqueueIdempotencyExplicitKey(t *testing.T) {
	svc, store, _ := newTestService(t)
	ctx := context.Background()
	req := domain.EnqueueRequest{
		JobType:        domain.JobTypeQRScan,
		Payload:        []byte(`{"qr_code":"QR1"}`),
		IdempotencyKey: "scan-1",
	}

	first, err := svc.Enqueue(ctx, req)
	require.NoError(t, err)
	require.False(t, first.Duplicate)

	second, err := svc.Enqueue(ctx, req)
	require.NoError(t, err)
	require.True(t, second.Duplicate, "same idempotency key must be a duplicate")
	require.Equal(t, first.Job.ID, second.Job.ID, "duplicate returns the original job id")
	require.Equal(t, 1, store.JobCount(), "no duplicate job persisted")
}

func TestEnqueueIdempotencyDerivedFromPayload(t *testing.T) {
	svc, store, _ := newTestService(t)
	ctx := context.Background()
	payload := []byte(`{"qr_code":"QR-DERIVED","event_id":"E9"}`)

	first, err := svc.Enqueue(ctx, domain.EnqueueRequest{JobType: domain.JobTypeQRScan, Payload: payload})
	require.NoError(t, err)
	require.False(t, first.Duplicate)

	// No explicit key: the payload hash must collapse the duplicate.
	second, err := svc.Enqueue(ctx, domain.EnqueueRequest{JobType: domain.JobTypeQRScan, Payload: payload})
	require.NoError(t, err)
	require.True(t, second.Duplicate)
	require.Equal(t, 1, store.JobCount())
}

func TestEnqueueRejectsInvalidPayload(t *testing.T) {
	svc, _, _ := newTestService(t)
	_, err := svc.Enqueue(context.Background(), domain.EnqueueRequest{
		JobType: domain.JobTypeNotification,
		Payload: []byte(`not-json`),
	})
	require.ErrorIs(t, err, domain.ErrInvalidPayload)
}

func TestEnqueueRejectsUnknownType(t *testing.T) {
	svc, _, _ := newTestService(t)
	_, err := svc.Enqueue(context.Background(), domain.EnqueueRequest{
		JobType: domain.JobType("MYSTERY"),
		Payload: []byte(`{}`),
	})
	require.ErrorIs(t, err, domain.ErrUnknownJobType)
}
