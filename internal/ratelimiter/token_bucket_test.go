package ratelimiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucketAllowsBurstThenDenies(t *testing.T) {
	// 1 token/sec sustained, burst of 3 -> first 3 succeed, 4th fails.
	b := NewBucket(1, 3)
	require.True(t, b.Allow())
	require.True(t, b.Allow())
	require.True(t, b.Allow())
	require.False(t, b.Allow(), "burst should be exhausted")
}

func TestBucketRefills(t *testing.T) {
	// 100 tokens/sec -> a token regenerates roughly every 10ms.
	b := NewBucket(100, 1)
	require.True(t, b.Allow())
	require.False(t, b.Allow(), "second call immediately should be denied")

	time.Sleep(30 * time.Millisecond) // ~3 tokens refilled
	require.True(t, b.Allow(), "token should have refilled")
}

func TestKeyedLimiterIsolatesKeys(t *testing.T) {
	k := NewKeyedLimiter(1, 1, time.Minute)
	defer k.Close()

	require.True(t, k.Allow("client-a"))
	require.False(t, k.Allow("client-a"), "same client second call denied")
	require.True(t, k.Allow("client-b"), "different client has its own bucket")
	require.Equal(t, 2, k.Size())
}

func TestGlobalLimiterIgnoresKey(t *testing.T) {
	g := NewGlobalLimiter(1, 2)
	require.True(t, g.Allow("anything"))
	require.True(t, g.Allow("else"))
	require.False(t, g.Allow("third"), "shared bucket exhausted regardless of key")
}
