// Package ratelimiter implements an in-memory token-bucket rate limiter used
// for both a global limiter and per-client (route-level) limiters. It is
// intentionally dependency-free to demonstrate the algorithm; in a multi-node
// deployment this would be backed by Redis (see README for the trade-off).
package ratelimiter

import (
	"sync"
	"time"
)

// Bucket is a classic token bucket. Tokens refill continuously at refillPerSec
// up to capacity (the burst). Each allowed request consumes one token.
type Bucket struct {
	mu           sync.Mutex
	capacity     float64
	tokens       float64
	refillPerSec float64
	last         time.Time
}

// NewBucket builds a bucket that sustains rps requests/second with the given
// burst. A starting bucket is full so a fresh client may immediately burst.
func NewBucket(rps float64, burst int) *Bucket {
	if rps <= 0 {
		rps = 1
	}
	if burst <= 0 {
		burst = 1
	}
	return &Bucket{
		capacity:     float64(burst),
		tokens:       float64(burst),
		refillPerSec: rps,
		last:         time.Now(),
	}
}

// Allow reports whether one token is available, consuming it if so.
func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now

	// Refill, clamped to capacity.
	b.tokens = min(b.capacity, b.tokens+elapsed*b.refillPerSec)

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// Tokens returns the current (post-refill) token count. Primarily for tests.
func (b *Bucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens = min(b.capacity, b.tokens+elapsed*b.refillPerSec)
	return b.tokens
}
