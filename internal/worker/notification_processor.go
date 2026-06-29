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

// Sender abstracts the actual delivery mechanism (email/SMS/push) so it can be
// swapped for SES/SNS/FCM in production and faked in tests. The default
// implementation simulates a successful send.
type Sender interface {
	Send(ctx context.Context, channel domain.NotificationChannel, recipient, subject, body string) error
}

// SimulatedSender is a no-op sender that always succeeds. It stands in for a
// real provider integration.
type SimulatedSender struct{}

// Send simulates delivery latency-free and always succeeds.
func (SimulatedSender) Send(_ context.Context, _ domain.NotificationChannel, _, _, _ string) error {
	return nil
}

// NotificationProcessor delivers notifications and records the delivery result.
// Transient send failures bubble up as retryable errors so the worker pool
// retries with backoff and eventually dead-letters.
type NotificationProcessor struct {
	notes  domain.NotificationRepository
	audit  domain.AuditRepository
	sender Sender
	log    *zap.Logger
}

var _ domain.Processor = (*NotificationProcessor)(nil)

// NewNotificationProcessor builds the processor. A nil sender defaults to the
// simulated sender.
func NewNotificationProcessor(notes domain.NotificationRepository, audit domain.AuditRepository, sender Sender, log *zap.Logger) *NotificationProcessor {
	if sender == nil {
		sender = SimulatedSender{}
	}
	return &NotificationProcessor{notes: notes, audit: audit, sender: sender, log: log}
}

// Type implements domain.Processor.
func (p *NotificationProcessor) Type() domain.JobType { return domain.JobTypeNotification }

// Process validates, sends and records a notification.
func (p *NotificationProcessor) Process(ctx context.Context, job *domain.Job) error {
	var in domain.NotificationPayload
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("%w: decode notification payload: %v", domain.ErrNonRetryable, err)
	}
	if !in.Channel.Valid() {
		return fmt.Errorf("%w: invalid channel %q", domain.ErrNonRetryable, in.Channel)
	}
	if in.Recipient == "" {
		return fmt.Errorf("%w: recipient is required", domain.ErrNonRetryable)
	}

	now := time.Now().UTC()
	status := "SENT"
	sendErr := p.sender.Send(ctx, in.Channel, in.Recipient, in.Subject, in.Body)
	if sendErr != nil {
		status = "FAILED"
	}

	// Always record the delivery attempt/result.
	if err := p.notes.CreateNotificationEvent(ctx, &domain.NotificationEvent{
		ID:        uuid.NewString(),
		JobID:     job.ID,
		Channel:   string(in.Channel),
		Recipient: in.Recipient,
		Template:  in.Template,
		Status:    status,
		Attempts:  job.RetryCount + 1,
		SentAt:    sentAtPtr(now, sendErr == nil),
		CreatedAt: now,
	}); err != nil {
		return fmt.Errorf("store notification event: %w", err)
	}

	if sendErr != nil {
		// Transient provider failure -> retryable so the pool backs off.
		return fmt.Errorf("send %s notification: %w", in.Channel, sendErr)
	}

	if err := p.audit.CreateAuditLog(ctx, &domain.AuditLog{
		EntityType: "notification", EntityID: in.Recipient, Action: "SENT_" + string(in.Channel),
		Detail: job.Payload, TraceID: job.TraceID, CreatedAt: now,
	}); err != nil {
		p.log.Warn("write notification audit log failed", zap.Error(err))
	}
	return nil
}

func sentAtPtr(t time.Time, sent bool) *time.Time {
	if sent {
		return &t
	}
	return nil
}
