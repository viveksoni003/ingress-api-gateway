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

// QRScanProcessor stores QR scan events, rejecting duplicate scans inside a
// short TTL window and maintaining a live attendance counter in Redis.
type QRScanProcessor struct {
	scans    domain.QRScanRepository
	audit    domain.AuditRepository
	cache    domain.Cache
	dedupTTL time.Duration
	log      *zap.Logger
}

var _ domain.Processor = (*QRScanProcessor)(nil)

// NewQRScanProcessor builds the processor.
func NewQRScanProcessor(scans domain.QRScanRepository, audit domain.AuditRepository, cache domain.Cache, dedupTTL time.Duration, log *zap.Logger) *QRScanProcessor {
	if dedupTTL <= 0 {
		dedupTTL = 10 * time.Second
	}
	return &QRScanProcessor{scans: scans, audit: audit, cache: cache, dedupTTL: dedupTTL, log: log}
}

// Type implements domain.Processor.
func (p *QRScanProcessor) Type() domain.JobType { return domain.JobTypeQRScan }

// Process de-duplicates and stores a scan event.
func (p *QRScanProcessor) Process(ctx context.Context, job *domain.Job) error {
	var in domain.QRScanPayload
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("%w: decode qr payload: %v", domain.ErrNonRetryable, err)
	}
	if in.QRCode == "" {
		return fmt.Errorf("%w: qr_code is required", domain.ErrNonRetryable)
	}

	// Duplicate-scan suppression: SetNX returns false if the code was scanned
	// within dedupTTL. A duplicate is a successful no-op (not an error), so the
	// job is marked SUCCESS without writing a second row or double counting.
	dedupKey := fmt.Sprintf("qr:seen:%s:%s", in.EventID, in.QRCode)
	fresh, err := p.cache.SetNX(ctx, dedupKey, "1", p.dedupTTL)
	if err != nil {
		return fmt.Errorf("qr dedup check: %w", err) // transient -> retry
	}
	if !fresh {
		p.log.Info("duplicate qr scan suppressed", zap.String("qr_code", in.QRCode), zap.String("event_id", in.EventID))
		return nil
	}

	now := time.Now().UTC()
	event := &domain.QRScanEvent{
		ID:        uuid.NewString(),
		JobID:     job.ID,
		QRCode:    in.QRCode,
		EventID:   in.EventID,
		GateID:    in.GateID,
		ScannedBy: in.ScannedBy,
		CreatedAt: now,
	}
	if err := p.scans.CreateQRScanEvent(ctx, event); err != nil {
		return fmt.Errorf("store qr scan: %w", err)
	}

	// Maintain a live attendance counter per event.
	if in.EventID != "" {
		if n, err := p.cache.Incr(ctx, "attendance:"+in.EventID); err != nil {
			p.log.Warn("increment attendance counter failed", zap.Error(err))
		} else {
			p.log.Info("attendance updated", zap.String("event_id", in.EventID), zap.Int64("count", n))
		}
	}

	if err := p.audit.CreateAuditLog(ctx, &domain.AuditLog{
		EntityType: "qr_scan", EntityID: event.ID, Action: "SCANNED",
		Detail: job.Payload, TraceID: job.TraceID, CreatedAt: now,
	}); err != nil {
		p.log.Warn("write qr audit log failed", zap.Error(err))
	}
	return nil
}
