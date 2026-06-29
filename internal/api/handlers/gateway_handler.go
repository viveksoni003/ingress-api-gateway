// Package handlers contains the HTTP handlers for the public gateway API, the
// admin API and the health/observability endpoints.
package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/logger"
	"github.com/viveksoni003/ingress-api-gateway/internal/security"
	"go.uber.org/zap"
)

// GatewayHandler accepts public traffic, validates it, verifies signatures
// where required, and hands it to the enqueuer. It returns 202 Accepted once
// the job is queued (or 200 for an idempotent duplicate).
type GatewayHandler struct {
	enqueuer   domain.Enqueuer
	hmacSecret string
	log        *zap.Logger
}

// NewGatewayHandler builds the handler.
func NewGatewayHandler(enqueuer domain.Enqueuer, hmacSecret string, log *zap.Logger) *GatewayHandler {
	return &GatewayHandler{enqueuer: enqueuer, hmacSecret: hmacSecret, log: log}
}

// AcceptResponse is returned when a job is accepted onto the queue.
type AcceptResponse struct {
	JobID     string `json:"job_id"`
	Status    string `json:"status"`
	JobType   string `json:"job_type"`
	Duplicate bool   `json:"duplicate"`
	TraceID   string `json:"trace_id,omitempty"`
}

// Registrations handles POST /api/v1/gateway/registrations.
func (h *GatewayHandler) Registrations(w http.ResponseWriter, r *http.Request) {
	h.accept(w, r, domain.JobTypeRegistration, false)
}

// PaymentWebhooks handles POST /api/v1/gateway/payment-webhooks (HMAC verified).
func (h *GatewayHandler) PaymentWebhooks(w http.ResponseWriter, r *http.Request) {
	h.accept(w, r, domain.JobTypePaymentWebhook, true)
}

// QRScans handles POST /api/v1/gateway/qr-scans.
func (h *GatewayHandler) QRScans(w http.ResponseWriter, r *http.Request) {
	h.accept(w, r, domain.JobTypeQRScan, false)
}

// Notifications handles POST /api/v1/gateway/notifications.
func (h *GatewayHandler) Notifications(w http.ResponseWriter, r *http.Request) {
	h.accept(w, r, domain.JobTypeNotification, false)
}

// accept is the shared pipeline for every traffic type.
func (h *GatewayHandler) accept(w http.ResponseWriter, r *http.Request, jobType domain.JobType, verifyHMAC bool) {
	body, ok := h.readBody(w, r)
	if !ok {
		return
	}

	// Payment webhooks must carry a valid HMAC-SHA256 signature over the raw
	// body. Verifying at ingress rejects forgeries before they consume queue
	// or worker capacity.
	if verifyHMAC {
		sig := r.Header.Get("X-Signature")
		if sig == "" {
			sig = r.Header.Get("X-Razorpay-Signature")
		}
		if !security.VerifyHMACSHA256(h.hmacSecret, body, sig) {
			httpx.WriteError(w, r, http.StatusUnauthorized, "invalid_signature", "webhook signature verification failed")
			return
		}
	}

	if err := validatePayload(jobType, body); err != nil {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_payload", err.Error())
		return
	}

	res, err := h.enqueuer.Enqueue(r.Context(), domain.EnqueueRequest{
		JobType:        jobType,
		Payload:        body,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		TraceID:        logger.TraceID(r.Context()),
	})
	if err != nil {
		h.mapError(w, r, err)
		return
	}

	status := http.StatusAccepted
	if res.Duplicate {
		status = http.StatusOK // already accepted earlier; idempotent replay
	}
	httpx.WriteJSON(w, status, AcceptResponse{
		JobID:     res.Job.ID,
		Status:    string(res.Job.Status),
		JobType:   string(jobType),
		Duplicate: res.Duplicate,
		TraceID:   logger.TraceID(r.Context()),
	})
}

// readBody reads and bounds the request body, translating MaxBytesReader
// overflows into 413.
func (h *GatewayHandler) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			httpx.WriteError(w, r, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds limit")
			return nil, false
		}
		httpx.WriteError(w, r, http.StatusBadRequest, "bad_request", "could not read request body")
		return nil, false
	}
	if len(body) == 0 {
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_payload", "empty request body")
		return nil, false
	}
	return body, true
}

// mapError translates domain errors to HTTP status codes.
func (h *GatewayHandler) mapError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrUnknownJobType):
		httpx.WriteError(w, r, http.StatusBadRequest, "unknown_job_type", err.Error())
	case errors.Is(err, domain.ErrInvalidPayload):
		httpx.WriteError(w, r, http.StatusBadRequest, "invalid_payload", err.Error())
	case errors.Is(err, domain.ErrQueueFull):
		w.Header().Set("Retry-After", "2")
		httpx.WriteError(w, r, http.StatusServiceUnavailable, "service_unavailable", "queue saturated, retry later")
	default:
		h.log.Error("enqueue failed", zap.String("trace_id", logger.TraceID(r.Context())), zap.Error(err))
		httpx.WriteError(w, r, http.StatusInternalServerError, "internal_error", "could not accept request")
	}
}

// validatePayload performs synchronous, per-type input validation so clients
// get a clear 400 instead of a silently dead-lettered job.
func validatePayload(jobType domain.JobType, body []byte) error {
	switch jobType {
	case domain.JobTypeRegistration:
		var p domain.RegistrationPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return errInvalidJSON()
		}
		return firstNonNil(
			required("event_id", p.EventID),
			required("attendee_name", p.AttendeeName),
			required("email", p.Email),
		)
	case domain.JobTypePaymentWebhook:
		var p domain.PaymentWebhookPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return errInvalidJSON()
		}
		return firstNonNil(
			required("gateway_order_id", p.GatewayOrderID),
			required("payment_status", p.PaymentStatus),
		)
	case domain.JobTypeQRScan:
		var p domain.QRScanPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return errInvalidJSON()
		}
		return required("qr_code", p.QRCode)
	case domain.JobTypeNotification:
		var p domain.NotificationPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return errInvalidJSON()
		}
		if !p.Channel.Valid() {
			return domain.NewValidationError("channel", "must be one of EMAIL, SMS, PUSH")
		}
		return required("recipient", p.Recipient)
	default:
		return domain.ErrUnknownJobType
	}
}

func required(field, value string) error {
	if value == "" {
		return domain.NewValidationError(field, "is required")
	}
	return nil
}

func errInvalidJSON() error { return fmt.Errorf("%w: body is not valid JSON", domain.ErrInvalidPayload) }

func firstNonNil(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
