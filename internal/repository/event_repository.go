package repository

import (
	"context"
	"fmt"

	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// CreateRegistration inserts an attendee registration.
func (s *Store) CreateRegistration(ctx context.Context, r *domain.Registration) error {
	const q = `
INSERT INTO registrations (id, job_id, event_id, attendee_name, email, phone, ticket_type, status, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`
	_, err := s.pool.Exec(ctx, q,
		r.ID, r.JobID, r.EventID, r.AttendeeName, r.Email, r.Phone, r.TicketType, r.Status, r.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert registration: %w", err)
	}
	return nil
}

// UpsertPaymentEvent inserts or updates a payment event keyed by
// gateway_order_id, so a re-delivered webhook updates the existing row.
func (s *Store) UpsertPaymentEvent(ctx context.Context, p *domain.PaymentEvent) error {
	const q = `
INSERT INTO payment_events (id, job_id, gateway_order_id, payment_id, status, amount_cents, currency, processed_at, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (gateway_order_id) DO UPDATE
   SET payment_id   = EXCLUDED.payment_id,
       status       = EXCLUDED.status,
       amount_cents = EXCLUDED.amount_cents,
       currency     = EXCLUDED.currency,
       processed_at = EXCLUDED.processed_at`
	_, err := s.pool.Exec(ctx, q,
		p.ID, p.JobID, p.GatewayOrderID, p.PaymentID, p.Status, p.AmountCents, p.Currency, p.ProcessedAt, p.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert payment event: %w", err)
	}
	return nil
}

// CreateQRScanEvent inserts a QR scan event.
func (s *Store) CreateQRScanEvent(ctx context.Context, e *domain.QRScanEvent) error {
	const q = `
INSERT INTO qr_scan_events (id, job_id, qr_code, event_id, gate_id, scanned_by, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)`
	_, err := s.pool.Exec(ctx, q,
		e.ID, e.JobID, e.QRCode, e.EventID, e.GateID, e.ScannedBy, e.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert qr scan event: %w", err)
	}
	return nil
}

// CreateNotificationEvent inserts a notification delivery record.
func (s *Store) CreateNotificationEvent(ctx context.Context, n *domain.NotificationEvent) error {
	const q = `
INSERT INTO notification_events (id, job_id, channel, recipient, template, status, attempts, sent_at, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`
	_, err := s.pool.Exec(ctx, q,
		n.ID, n.JobID, n.Channel, n.Recipient, n.Template, n.Status, n.Attempts, n.SentAt, n.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert notification event: %w", err)
	}
	return nil
}

// CreateAuditLog inserts an audit-log entry.
func (s *Store) CreateAuditLog(ctx context.Context, l *domain.AuditLog) error {
	const q = `
INSERT INTO audit_logs (entity_type, entity_id, action, detail, trace_id, created_at)
VALUES ($1,$2,$3,$4,$5,$6)`
	detail := l.Detail
	if len(detail) == 0 {
		detail = []byte("{}")
	}
	_, err := s.pool.Exec(ctx, q,
		l.EntityType, l.EntityID, l.Action, string(detail), l.TraceID, l.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert audit log: %w", err)
	}
	return nil
}
