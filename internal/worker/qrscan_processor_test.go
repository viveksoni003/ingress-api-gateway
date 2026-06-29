package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viveksoni003/ingress-api-gateway/internal/cache"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/testutil"
	"go.uber.org/zap"
)

func TestQRScanDuplicatePrevention(t *testing.T) {
	c := cache.NewRedisCache(testutil.NewMiniRedis(t))
	store := testutil.NewInMemoryStore()
	proc := NewQRScanProcessor(store, store, c, time.Minute, zap.NewNop())
	ctx := context.Background()

	payload := []byte(`{"qr_code":"QR-123","event_id":"E1","gate_id":"G1"}`)

	// First scan stores an event and bumps the attendance counter.
	job1 := domain.NewJob("scan-1", domain.JobTypeQRScan, payload, "i1", "t1", 3)
	require.NoError(t, proc.Process(ctx, job1))
	require.Len(t, store.Scans, 1)

	// Duplicate scan within the TTL is a successful no-op: no extra row, no
	// double count.
	job2 := domain.NewJob("scan-2", domain.JobTypeQRScan, payload, "i2", "t2", 3)
	require.NoError(t, proc.Process(ctx, job2))
	require.Len(t, store.Scans, 1, "duplicate scan must not create a second event")

	count, found, err := c.Get(ctx, "attendance:E1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "1", count, "attendance counted exactly once")
}

func TestQRScanRejectsMissingCode(t *testing.T) {
	c := cache.NewRedisCache(testutil.NewMiniRedis(t))
	store := testutil.NewInMemoryStore()
	proc := NewQRScanProcessor(store, store, c, time.Minute, zap.NewNop())

	job := domain.NewJob("scan-x", domain.JobTypeQRScan, []byte(`{"event_id":"E1"}`), "i", "t", 3)
	err := proc.Process(context.Background(), job)
	require.ErrorIs(t, err, domain.ErrNonRetryable)
}
