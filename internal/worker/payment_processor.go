package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"go.uber.org/zap"
)

// validPaymentStatuses is the set of statuses a webhook may carry.
var validPaymentStatuses = map[string]bool{
	"AUTHORIZED": true,
	"CAPTURED":   true,
	"FAILED":     true,
	"REFUNDED":   true,
	"PENDING":    true,
}

// PaymentProcessor validates a (HMAC-verified at ingress) payment webhook and
// upserts the payment event. Upsert keyed on gateway_order_id makes
// re-delivery idempotent at the storage layer too.
type PaymentProcessor struct {
	payments domain.PaymentRepository
	audit    domain.AuditRepository
	log      *zap.Logger
}

var _ domain.Processor = (*PaymentProcessor)(nil)

// NewPaymentProcessor builds the processor.
func NewPaymentProcessor(payments domain.PaymentRepository, audit domain.AuditRepository, log *zap.Logger) *PaymentProcessor {
	return &PaymentProcessor{payments: payments, audit: audit, log: log}
}

// Type implements domain.Processor.
func (p *PaymentProcessor) Type() domain.JobType { return domain.JobTypePaymentWebhook }

// Process validates and stores the payment event.
func (p *PaymentProcessor) Process(ctx context.Context, job *domain.Job) error {
	var in domain.PaymentWebhookPayload
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("%w: decode payment payload: %v", domain.ErrNonRetryable, err)
	}
	if in.GatewayOrderID == "" {
		return fmt.Errorf("%w: gateway_order_id is required", domain.ErrNonRetryable)
	}
	if !validPaymentStatuses[in.PaymentStatus] {
		return fmt.Errorf("%w: unknown payment_status %q", domain.ErrNonRetryable, in.PaymentStatus)
	}

	now := time.Now().UTC()
	event := &domain.PaymentEvent{
		ID:             uuid.NewString(),
		JobID:          job.ID,
		GatewayOrderID: in.GatewayOrderID,
		PaymentID:      in.PaymentID,
		Status:         in.PaymentStatus,
		AmountCents:    in.AmountCents,
		Currency:       defaultStr(in.Currency, "INR"),
		ProcessedAt:    &now,
		CreatedAt:      now,
	}
	// Simulate updating the payment record. DB failure is transient -> retry.
	if err := p.payments.UpsertPaymentEvent(ctx, event); err != nil {
		return fmt.Errorf("upsert payment event: %w", err)
	}

	if err := p.audit.CreateAuditLog(ctx, &domain.AuditLog{
		EntityType: "payment", EntityID: in.GatewayOrderID, Action: "WEBHOOK_" + in.PaymentStatus,
		Detail: job.Payload, TraceID: job.TraceID, CreatedAt: now,
	}); err != nil {
		p.log.Warn("write payment audit log failed", zap.Error(err))
	}

	p.log.Info("payment webhook processed",
		zap.String("gateway_order_id", in.GatewayOrderID),
		zap.String("payment_status", in.PaymentStatus))
	return nil
}
