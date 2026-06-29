package domain

import (
	"encoding/json"
	"time"
)

// ---------------------------------------------------------------------------
// Request payloads (the parsed body of each traffic type). These are stored
// inside Job.Payload as raw JSON and decoded by the matching processor.
// ---------------------------------------------------------------------------

// RegistrationPayload is the body of POST /gateway/registrations.
type RegistrationPayload struct {
	EventID      string `json:"event_id"`
	AttendeeName string `json:"attendee_name"`
	Email        string `json:"email"`
	Phone        string `json:"phone"`
	TicketType   string `json:"ticket_type"`
}

// PaymentWebhookPayload is the body of POST /gateway/payment-webhooks.
type PaymentWebhookPayload struct {
	GatewayOrderID string `json:"gateway_order_id"`
	PaymentID      string `json:"payment_id"`
	PaymentStatus  string `json:"payment_status"` // CAPTURED, FAILED, REFUNDED, ...
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency"`
}

// QRScanPayload is the body of POST /gateway/qr-scans.
type QRScanPayload struct {
	QRCode    string `json:"qr_code"`
	EventID   string `json:"event_id"`
	GateID    string `json:"gate_id"`
	ScannedBy string `json:"scanned_by"`
}

// NotificationChannel enumerates supported delivery channels.
type NotificationChannel string

const (
	ChannelEmail NotificationChannel = "EMAIL"
	ChannelSMS   NotificationChannel = "SMS"
	ChannelPush  NotificationChannel = "PUSH"
)

// Valid reports whether the channel is supported.
func (c NotificationChannel) Valid() bool {
	switch c {
	case ChannelEmail, ChannelSMS, ChannelPush:
		return true
	default:
		return false
	}
}

// NotificationPayload is the body of POST /gateway/notifications.
type NotificationPayload struct {
	Channel   NotificationChannel `json:"channel"`
	Recipient string              `json:"recipient"`
	Template  string              `json:"template"`
	Subject   string              `json:"subject"`
	Body      string              `json:"body"`
	EventID   string              `json:"event_id"`
}

// ---------------------------------------------------------------------------
// Persisted entities (one table each).
// ---------------------------------------------------------------------------

// Registration is a stored attendee registration.
type Registration struct {
	ID           string    `json:"id"`
	JobID        string    `json:"job_id"`
	EventID      string    `json:"event_id"`
	AttendeeName string    `json:"attendee_name"`
	Email        string    `json:"email"`
	Phone        string    `json:"phone"`
	TicketType   string    `json:"ticket_type"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
}

// PaymentEvent is a stored, verified payment webhook.
type PaymentEvent struct {
	ID             string     `json:"id"`
	JobID          string     `json:"job_id"`
	GatewayOrderID string     `json:"gateway_order_id"`
	PaymentID      string     `json:"payment_id"`
	Status         string     `json:"status"`
	AmountCents    int64      `json:"amount_cents"`
	Currency       string     `json:"currency"`
	ProcessedAt    *time.Time `json:"processed_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// QRScanEvent is a stored, de-duplicated QR scan.
type QRScanEvent struct {
	ID        string    `json:"id"`
	JobID     string    `json:"job_id"`
	QRCode    string    `json:"qr_code"`
	EventID   string    `json:"event_id"`
	GateID    string    `json:"gate_id"`
	ScannedBy string    `json:"scanned_by"`
	CreatedAt time.Time `json:"created_at"`
}

// NotificationEvent is a stored notification delivery attempt/result.
type NotificationEvent struct {
	ID        string     `json:"id"`
	JobID     string     `json:"job_id"`
	Channel   string     `json:"channel"`
	Recipient string     `json:"recipient"`
	Template  string     `json:"template"`
	Status    string     `json:"status"` // SENT, FAILED
	Attempts  int        `json:"attempts"`
	SentAt    *time.Time `json:"sent_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

// AuditLog records a notable side effect for compliance / debugging.
type AuditLog struct {
	ID         int64           `json:"id"`
	EntityType string          `json:"entity_type"`
	EntityID   string          `json:"entity_id"`
	Action     string          `json:"action"`
	Detail     json.RawMessage `json:"detail"`
	TraceID    string          `json:"trace_id"`
	CreatedAt  time.Time       `json:"created_at"`
}
