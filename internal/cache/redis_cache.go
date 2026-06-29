// Package cache provides a Redis-backed implementation of domain.Cache used
// for idempotency keys, QR-scan de-duplication and live attendance counters.
package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/viveksoni003/ingress-api-gateway/internal/config"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// NewRedisClient builds a go-redis client with pool settings derived from
// configuration. The same client instance is shared by the cache and the
// queue adapters.
func NewRedisClient(cfg config.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     20,
		MinIdleConns: 4,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}

// RedisCache implements domain.Cache.
type RedisCache struct {
	rdb *redis.Client
}

// Compile-time assertion that RedisCache satisfies the port.
var _ domain.Cache = (*RedisCache)(nil)

// NewRedisCache wraps an existing redis client.
func NewRedisCache(rdb *redis.Client) *RedisCache {
	return &RedisCache{rdb: rdb}
}

// SetNX sets key=value only if key does not already exist. The returned bool is
// true when the key was newly set (i.e. not a duplicate).
func (c *RedisCache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, key, value, ttl).Result()
}

// Get returns the value for key. found is false (with nil error) when the key
// is absent, so callers can distinguish "missing" from "error".
func (c *RedisCache) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// Set unconditionally writes key=value with a TTL.
func (c *RedisCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

// Incr atomically increments an integer counter and returns the new value.
func (c *RedisCache) Incr(ctx context.Context, key string) (int64, error) {
	return c.rdb.Incr(ctx, key).Result()
}

// Del removes a key.
func (c *RedisCache) Del(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, key).Err()
}

// Ping checks connectivity.
func (c *RedisCache) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}
