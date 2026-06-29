-- 0001_init.down.sql
BEGIN;

DROP TABLE IF EXISTS audit_logs;
DROP TABLE IF EXISTS notification_events;
DROP TABLE IF EXISTS qr_scan_events;
DROP TABLE IF EXISTS payment_events;
DROP TABLE IF EXISTS registrations;
DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS jobs;

COMMIT;
