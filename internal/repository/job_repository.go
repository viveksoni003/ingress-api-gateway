package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// Create inserts a new job row.
func (s *Store) Create(ctx context.Context, j *domain.Job) error {
	const q = `
INSERT INTO jobs (id, job_type, payload, status, retry_count, max_retries,
                  priority, idempotency_key, trace_id, error_message,
                  created_at, updated_at, processed_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`
	_, err := s.pool.Exec(ctx, q,
		j.ID, j.JobType, string(j.Payload), j.Status, j.RetryCount, j.MaxRetries,
		j.Priority, j.IdempotencyKey, j.TraceID, j.ErrorMessage,
		j.CreatedAt, j.UpdatedAt, j.ProcessedAt,
	)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// Update persists mutable job fields.
func (s *Store) Update(ctx context.Context, j *domain.Job) error {
	const q = `
UPDATE jobs
   SET status=$2, retry_count=$3, error_message=$4, updated_at=$5, processed_at=$6
 WHERE id=$1`
	ct, err := s.pool.Exec(ctx, q,
		j.ID, j.Status, j.RetryCount, j.ErrorMessage, j.UpdatedAt, j.ProcessedAt,
	)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

const jobColumns = `id, job_type, payload, status, retry_count, max_retries,
	priority, idempotency_key, trace_id, error_message,
	created_at, updated_at, processed_at`

func scanJob(s scanner) (*domain.Job, error) {
	var j domain.Job
	var payload []byte
	if err := s.Scan(
		&j.ID, &j.JobType, &payload, &j.Status, &j.RetryCount, &j.MaxRetries,
		&j.Priority, &j.IdempotencyKey, &j.TraceID, &j.ErrorMessage,
		&j.CreatedAt, &j.UpdatedAt, &j.ProcessedAt,
	); err != nil {
		return nil, err
	}
	j.Payload = payload
	return &j, nil
}

// GetByID returns a single job or domain.ErrNotFound.
func (s *Store) GetByID(ctx context.Context, id string) (*domain.Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs WHERE id=$1`
	job, err := scanJob(s.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

// List returns jobs matching the filter, newest first.
func (s *Store) List(ctx context.Context, f domain.JobFilter) ([]*domain.Job, error) {
	var (
		conds []string
		args  []any
		i     = 1
	)
	if f.Status != "" {
		conds = append(conds, fmt.Sprintf("status=$%d", i))
		args = append(args, f.Status)
		i++
	}
	if f.JobType != "" {
		conds = append(conds, fmt.Sprintf("job_type=$%d", i))
		args = append(args, f.JobType)
		i++
	}

	q := `SELECT ` + jobColumns + ` FROM jobs`
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", i, i+1)
	args = append(args, limit, f.Offset)

	return s.queryJobs(ctx, q, args...)
}

// ListDeadLetter returns jobs whose status is DEAD_LETTER.
func (s *Store) ListDeadLetter(ctx context.Context, limit, offset int) ([]*domain.Job, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	q := `SELECT ` + jobColumns + ` FROM jobs WHERE status=$1 ORDER BY updated_at DESC LIMIT $2 OFFSET $3`
	return s.queryJobs(ctx, q, domain.JobStatusDeadLetter, limit, offset)
}

func (s *Store) queryJobs(ctx context.Context, q string, args ...any) ([]*domain.Job, error) {
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]*domain.Job, 0, 16)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}

// CountByStatus returns job counts grouped by status.
func (s *Store) CountByStatus(ctx context.Context) (map[domain.JobStatus]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}
	defer rows.Close()

	out := make(map[domain.JobStatus]int64)
	for rows.Next() {
		var status domain.JobStatus
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		out[status] = n
	}
	return out, rows.Err()
}

// CountByType returns job counts grouped by job_type.
func (s *Store) CountByType(ctx context.Context) (map[domain.JobType]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT job_type, COUNT(*) FROM jobs GROUP BY job_type`)
	if err != nil {
		return nil, fmt.Errorf("count by type: %w", err)
	}
	defer rows.Close()

	out := make(map[domain.JobType]int64)
	for rows.Next() {
		var t domain.JobType
		var n int64
		if err := rows.Scan(&t, &n); err != nil {
			return nil, err
		}
		out[t] = n
	}
	return out, rows.Err()
}

// AddAttempt records a single processing attempt.
func (s *Store) AddAttempt(ctx context.Context, a *domain.JobAttempt) error {
	const q = `
INSERT INTO job_attempts (job_id, attempt, status, error, duration_ms, created_at)
VALUES ($1,$2,$3,$4,$5,$6)`
	_, err := s.pool.Exec(ctx, q, a.JobID, a.Attempt, a.Status, a.Error, a.DurationMS, a.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert job attempt: %w", err)
	}
	return nil
}
