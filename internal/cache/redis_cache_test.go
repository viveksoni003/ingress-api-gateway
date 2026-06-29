package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viveksoni003/ingress-api-gateway/internal/testutil"
)

func TestRedisCacheSetNXAndGet(t *testing.T) {
	c := NewRedisCache(testutil.NewMiniRedis(t))
	ctx := context.Background()

	ok, err := c.SetNX(ctx, "idemp:k1", "job-1", time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "first SetNX should succeed")

	ok, err = c.SetNX(ctx, "idemp:k1", "job-2", time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "duplicate SetNX should fail")

	val, found, err := c.Get(ctx, "idemp:k1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "job-1", val, "original value must be preserved")

	_, found, err = c.Get(ctx, "missing")
	require.NoError(t, err)
	require.False(t, found)
}

func TestRedisCacheIncr(t *testing.T) {
	c := NewRedisCache(testutil.NewMiniRedis(t))
	ctx := context.Background()

	n, err := c.Incr(ctx, "attendance:E1")
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = c.Incr(ctx, "attendance:E1")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
}
