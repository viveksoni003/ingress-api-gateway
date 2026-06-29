// Package repository implements the domain.Store port on PostgreSQL using the
// pgx driver and a connection pool.
package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/viveksoni003/ingress-api-gateway/internal/config"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// NewPool creates and verifies a pgx connection pool.
func NewPool(ctx context.Context, cfg config.DBConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLife > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLife
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Store is the Postgres-backed implementation of every repository port.
type Store struct {
	pool *pgxpool.Pool
}

// Compile-time assertion that Store satisfies the aggregate port.
var _ domain.Store = (*Store)(nil)

// New wraps a pool in a Store.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Ping verifies database connectivity (used by /ready).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// scanner is implemented by both pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}
