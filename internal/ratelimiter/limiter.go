package ratelimiter

import (
	"sync"
	"time"

	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// GlobalLimiter applies a single shared bucket to every request, regardless of
// key. It protects the whole process from aggregate overload.
type GlobalLimiter struct {
	bucket *Bucket
}

var _ domain.RateLimiter = (*GlobalLimiter)(nil)

// NewGlobalLimiter builds a process-wide limiter.
func NewGlobalLimiter(rps float64, burst int) *GlobalLimiter {
	return &GlobalLimiter{bucket: NewBucket(rps, burst)}
}

// Allow ignores key and consults the single shared bucket.
func (g *GlobalLimiter) Allow(_ string) bool { return g.bucket.Allow() }

// KeyedLimiter keeps one bucket per key (e.g. client IP or API key) so a noisy
// client cannot starve others. Idle buckets are evicted by a background
// janitor to bound memory.
type KeyedLimiter struct {
	mu      sync.Mutex
	buckets map[string]*entry
	rps     float64
	burst   int
	idleTTL time.Duration
	stop    chan struct{}
	once    sync.Once
}

type entry struct {
	bucket *Bucket
	seen   time.Time
}

var _ domain.RateLimiter = (*KeyedLimiter)(nil)

// NewKeyedLimiter builds a per-key limiter and starts its janitor goroutine.
func NewKeyedLimiter(rps float64, burst int, idleTTL time.Duration) *KeyedLimiter {
	if idleTTL <= 0 {
		idleTTL = 10 * time.Minute
	}
	k := &KeyedLimiter{
		buckets: make(map[string]*entry),
		rps:     rps,
		burst:   burst,
		idleTTL: idleTTL,
		stop:    make(chan struct{}),
	}
	go k.janitor()
	return k
}

// Allow consults (creating if needed) the bucket for key.
func (k *KeyedLimiter) Allow(key string) bool {
	k.mu.Lock()
	e, ok := k.buckets[key]
	if !ok {
		e = &entry{bucket: NewBucket(k.rps, k.burst)}
		k.buckets[key] = e
	}
	e.seen = time.Now()
	b := e.bucket
	k.mu.Unlock()

	return b.Allow()
}

// janitor periodically evicts buckets not seen within idleTTL.
func (k *KeyedLimiter) janitor() {
	ticker := time.NewTicker(k.idleTTL)
	defer ticker.Stop()
	for {
		select {
		case <-k.stop:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-k.idleTTL)
			k.mu.Lock()
			for key, e := range k.buckets {
				if e.seen.Before(cutoff) {
					delete(k.buckets, key)
				}
			}
			k.mu.Unlock()
		}
	}
}

// Close stops the janitor goroutine. Safe to call multiple times.
func (k *KeyedLimiter) Close() {
	k.once.Do(func() { close(k.stop) })
}

// Size returns the number of tracked keys (for tests / metrics).
func (k *KeyedLimiter) Size() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.buckets)
}
