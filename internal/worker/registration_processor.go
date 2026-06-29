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

// RegistrationProcessor stores attendee registrations and fans out a welcome
// notification job on success.
type RegistrationProcessor struct {
	regs     domain.RegistrationRepository
	audit    domain.AuditRepository
	enqueuer domain.Enqueuer
	log      *zap.Logger
}

var _ domain.Processor = (*RegistrationProcessor)(nil)

// NewRegistrationProcessor builds the processor.
func NewRegistrationProcessor(regs domain.RegistrationRepository, audit domain.AuditRepository, enq domain.Enqueuer, log *zap.Logger) *RegistrationProcessor {
	return &RegistrationProcessor{regs: regs, audit: audit, enqueuer: enq, log: log}
}

// Type implements domain.Processor.
func (p *RegistrationProcessor) Type() domain.JobType { return domain.JobTypeRegistration }

// Process validates and stores a registration, then enqueues a notification.
func (p *RegistrationProcessor) Process(ctx context.Context, job *domain.Job) error {
	var in domain.RegistrationPayload
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("%w: decode registration payload: %v", domain.ErrNonRetryable, err)
	}
	if in.EventID == "" || in.AttendeeName == "" || in.Email == "" {
		return fmt.Errorf("%w: event_id, attendee_name and email are required", domain.ErrNonRetryable)
	}

	now := time.Now().UTC()
	reg := &domain.Registration{
		ID:           uuid.NewString(),
		JobID:        job.ID,
		EventID:      in.EventID,
		AttendeeName: in.AttendeeName,
		Email:        in.Email,
		Phone:        in.Phone,
		TicketType:   defaultStr(in.TicketType, "GENERAL"),
		Status:       "CONFIRMED",
		CreatedAt:    now,
	}
	// A DB failure here is transient -> returned as a retryable error.
	if err := p.regs.CreateRegistration(ctx, reg); err != nil {
		return fmt.Errorf("store registration: %w", err)
	}

	// Fan out a welcome notification. Failure to enqueue must NOT fail the
	// registration (which already succeeded), so we only log it.
	p.enqueueWelcome(ctx, job, reg)

	p.writeAudit(ctx, "registration", reg.ID, "CREATED", job.Payload, job.TraceID, now)
	return nil
}

func (p *RegistrationProcessor) enqueueWelcome(ctx context.Context, job *domain.Job, reg *domain.Registration) {
	np := domain.NotificationPayload{
		Channel:   domain.ChannelEmail,
		Recipient: reg.Email,
		Template:  "registration_welcome",
		Subject:   "Your registration is confirmed",
		Body:      fmt.Sprintf("Hi %s, you are confirmed for event %s.", reg.AttendeeName, reg.EventID),
		EventID:   reg.EventID,
	}
	b, err := json.Marshal(np)
	if err != nil {
		p.log.Warn("marshal welcome notification failed", zap.Error(err))
		return
	}
	if _, err := p.enqueuer.Enqueue(ctx, domain.EnqueueRequest{
		JobType:        domain.JobTypeNotification,
		Payload:        b,
		IdempotencyKey: "notif:registration:" + reg.ID, // deterministic -> no dup
		TraceID:        job.TraceID,
	}); err != nil {
		p.log.Warn("enqueue welcome notification failed", zap.String("registration_id", reg.ID), zap.Error(err))
	}
}

func (p *RegistrationProcessor) writeAudit(ctx context.Context, entity, id, action string, detail json.RawMessage, traceID string, ts time.Time) {
	if err := p.audit.CreateAuditLog(ctx, &domain.AuditLog{
		EntityType: entity, EntityID: id, Action: action, Detail: detail, TraceID: traceID, CreatedAt: ts,
	}); err != nil {
		p.log.Warn("write audit log failed", zap.String("entity", entity), zap.Error(err))
	}
}

func defaultStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
