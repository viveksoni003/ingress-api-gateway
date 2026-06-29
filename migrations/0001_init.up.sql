-- 0001_init.up.sql
-- Core schema for the ingress API gateway. Designed for PostgreSQL 14+.

BEGIN;

-- ---------------------------------------------------------------------------
-- jobs: the generic async work item. Payload is JSONB so any traffic type can
-- be stored without a schema change.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT        PRIMARY KEY,
    job_type        TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL DEFAULT 'QUEUED',
    retry_count     INT         NOT NULL DEFAULT 0,
    max_retries     INT         NOT NULL DEFAULT 5,
    priority        TEXT        NOT NULL DEFAULT 'MEDIUM',
    idempotency_key TEXT        NOT NULL DEFAULT '',
    trace_id        TEXT        NOT NULL DEFAULT '',
    error_message   TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_status          ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_job_type        ON jobs (job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_key ON jobs (idempotency_key);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at      ON jobs (created_at);

-- ---------------------------------------------------------------------------
-- job_attempts: one row per processing attempt (for observability / auditing).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_attempts (
    id          BIGSERIAL   PRIMARY KEY,
    job_id      TEXT        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INT         NOT NULL,
    status      TEXT        NOT NULL,
    error       TEXT        NOT NULL DEFAULT '',
    duration_ms BIGINT      NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id ON job_attempts (job_id);

-- ---------------------------------------------------------------------------
-- registrations
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS registrations (
    id            TEXT        PRIMARY KEY,
    job_id        TEXT        NOT NULL,
    event_id      TEXT        NOT NULL,
    attendee_name TEXT        NOT NULL,
    email         TEXT        NOT NULL,
    phone         TEXT        NOT NULL DEFAULT '',
    ticket_type   TEXT        NOT NULL DEFAULT 'GENERAL',
    status        TEXT        NOT NULL DEFAULT 'CONFIRMED',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_registrations_event_id ON registrations (event_id);
CREATE INDEX IF NOT EXISTS idx_registrations_email    ON registrations (email);

-- ---------------------------------------------------------------------------
-- payment_events: gateway_order_id is unique so a re-delivered webhook upserts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payment_events (
    id               TEXT        PRIMARY KEY,
    job_id           TEXT        NOT NULL,
    gateway_order_id TEXT        NOT NULL,
    payment_id       TEXT        NOT NULL DEFAULT '',
    status           TEXT        NOT NULL,
    amount_cents     BIGINT      NOT NULL DEFAULT 0,
    currency         TEXT        NOT NULL DEFAULT 'INR',
    processed_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_events_gateway_order_id
    ON payment_events (gateway_order_id);

-- ---------------------------------------------------------------------------
-- qr_scan_events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS qr_scan_events (
    id         TEXT        PRIMARY KEY,
    job_id     TEXT        NOT NULL,
    qr_code    TEXT        NOT NULL,
    event_id   TEXT        NOT NULL DEFAULT '',
    gate_id    TEXT        NOT NULL DEFAULT '',
    scanned_by TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qr_scan_events_qr_code  ON qr_scan_events (qr_code);
CREATE INDEX IF NOT EXISTS idx_qr_scan_events_event_id ON qr_scan_events (event_id);

-- ---------------------------------------------------------------------------
-- notification_events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS notification_events (
    id         TEXT        PRIMARY KEY,
    job_id     TEXT        NOT NULL,
    channel    TEXT        NOT NULL,
    recipient  TEXT        NOT NULL,
    template   TEXT        NOT NULL DEFAULT '',
    status     TEXT        NOT NULL,
    attempts   INT         NOT NULL DEFAULT 0,
    sent_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_events_recipient ON notification_events (recipient);

-- ---------------------------------------------------------------------------
-- audit_logs
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_logs (
    id          BIGSERIAL   PRIMARY KEY,
    entity_type TEXT        NOT NULL,
    entity_id   TEXT        NOT NULL,
    action      TEXT        NOT NULL,
    detail      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    trace_id    TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON audit_logs (entity_type, entity_id);

COMMIT;
