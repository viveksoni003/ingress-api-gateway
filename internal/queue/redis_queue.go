// Package queue implements domain.Queue on top of Redis lists. Three priority
// lists plus a dead-letter list are used. Producers LPUSH and consumers BRPOP,
// which yields FIFO ordering; BRPOP across the three keys in priority order
// gives HIGH -> MEDIUM -> LOW preference for free.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// Redis list keys.
const (
	KeyHigh       = "jobs:high"
	KeyMedium     = "jobs:medium"
	KeyLow        = "jobs:low"
	KeyDeadLetter = "jobs:dead-letter"
)

// orderedKeys is the BRPOP scan order: higher priority first.
var orderedKeys = []string{KeyHigh, KeyMedium, KeyLow}

// RedisQueue implements domain.Queue.
type RedisQueue struct {
	rdb         *redis.Client
	pollTimeout time.Duration
}

var _ domain.Queue = (*RedisQueue)(nil)

// NewRedisQueue creates a queue. pollTimeout bounds how long Pop blocks before
// returning (nil, nil) so workers can observe context cancellation.
func NewRedisQueue(rdb *redis.Client, pollTimeout time.Duration) *RedisQueue {
	if pollTimeout <= 0 {
		pollTimeout = 5 * time.Second
	}
	return &RedisQueue{rdb: rdb, pollTimeout: pollTimeout}
}

func keyForPriority(p domain.Priority) string {
	switch p {
	case domain.PriorityHigh:
		return KeyHigh
	case domain.PriorityLow:
		return KeyLow
	default:
		return KeyMedium
	}
}

// Push serialises the job and LPUSHes it onto the list for its priority.
func (q *RedisQueue) Push(ctx context.Context, job *domain.Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return q.rdb.LPush(ctx, keyForPriority(job.Priority), b).Err()
}

// Pop blocks up to pollTimeout for the next job in priority order. It returns
// (nil, nil) on timeout so the worker loop can re-check its context.
func (q *RedisQueue) Pop(ctx context.Context) (*domain.Job, error) {
	res, err := q.rdb.BRPop(ctx, q.pollTimeout, orderedKeys...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // timeout, no job available
	}
	if err != nil {
		// A cancelled context surfaces here during shutdown; treat as no job.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, err
	}
	// res = [listKey, value]
	if len(res) != 2 {
		return nil, fmt.Errorf("unexpected BRPOP result length %d", len(res))
	}
	var job domain.Job
	if err := json.Unmarshal([]byte(res[1]), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

// PushDeadLetter serialises and LPUSHes a job onto the dead-letter list.
func (q *RedisQueue) PushDeadLetter(ctx context.Context, job *domain.Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal dead-letter job: %w", err)
	}
	return q.rdb.LPush(ctx, KeyDeadLetter, b).Err()
}

// Depth returns the length of one priority list.
func (q *RedisQueue) Depth(ctx context.Context, p domain.Priority) (int64, error) {
	return q.rdb.LLen(ctx, keyForPriority(p)).Result()
}

// TotalDepth returns the combined length of all priority lists using a
// pipeline (single round-trip).
func (q *RedisQueue) TotalDepth(ctx context.Context) (int64, error) {
	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(orderedKeys))
	for i, k := range orderedKeys {
		cmds[i] = pipe.LLen(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	var total int64
	for _, c := range cmds {
		total += c.Val()
	}
	return total, nil
}

// DeadLetterDepth returns the dead-letter list length.
func (q *RedisQueue) DeadLetterDepth(ctx context.Context) (int64, error) {
	return q.rdb.LLen(ctx, KeyDeadLetter).Result()
}

// ListDeadLetter returns up to limit dead-letter jobs (most recent first).
func (q *RedisQueue) ListDeadLetter(ctx context.Context, limit int64) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	vals, err := q.rdb.LRange(ctx, KeyDeadLetter, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}
	jobs := make([]*domain.Job, 0, len(vals))
	for _, v := range vals {
		var job domain.Job
		if err := json.Unmarshal([]byte(v), &job); err != nil {
			return nil, fmt.Errorf("unmarshal dead-letter job: %w", err)
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

// Ping verifies Redis connectivity.
func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}
