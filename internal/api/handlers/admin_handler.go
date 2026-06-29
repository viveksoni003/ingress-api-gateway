package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/service"
	"go.uber.org/zap"
)

// AdminHandler serves the JWT-protected admin API.
type AdminHandler struct {
	svc *service.AdminService
	log *zap.Logger
}

// NewAdminHandler builds the handler.
func NewAdminHandler(svc *service.AdminService, log *zap.Logger) *AdminHandler {
	return &AdminHandler{svc: svc, log: log}
}

type listResponse struct {
	Count int           `json:"count"`
	Jobs  []*domain.Job `json:"jobs"`
}

// ListJobs handles GET /api/v1/admin/jobs?status=&job_type=&limit=&offset=.
func (h *AdminHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := domain.JobFilter{
		Status:  domain.JobStatus(q.Get("status")),
		JobType: domain.JobType(q.Get("job_type")),
		Limit:   atoiDefault(q.Get("limit"), 50),
		Offset:  atoiDefault(q.Get("offset"), 0),
	}
	jobs, err := h.svc.ListJobs(r.Context(), f)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Count: len(jobs), Jobs: jobs})
}

// GetJob handles GET /api/v1/admin/jobs/{id}.
func (h *AdminHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := h.svc.GetJob(r.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		httpx.WriteError(w, r, http.StatusNotFound, "not_found", "job not found")
		return
	}
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, job)
}

// Stats handles GET /api/v1/admin/jobs/stats.
func (h *AdminHandler) Stats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.svc.Stats(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, stats)
}

// RetryJob handles POST /api/v1/admin/jobs/{id}/retry.
func (h *AdminHandler) RetryJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := h.svc.RetryJob(r.Context(), id)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		httpx.WriteError(w, r, http.StatusNotFound, "not_found", "job not found")
	case errors.Is(err, domain.ErrInvalidPayload):
		httpx.WriteError(w, r, http.StatusConflict, "conflict", err.Error())
	case err != nil:
		h.fail(w, r, err)
	default:
		httpx.WriteJSON(w, http.StatusOK, job)
	}
}

// DeadLetterJobs handles GET /api/v1/admin/dead-letter-jobs?limit=&offset=.
func (h *AdminHandler) DeadLetterJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	jobs, err := h.svc.ListDeadLetter(r.Context(), atoiDefault(q.Get("limit"), 50), atoiDefault(q.Get("offset"), 0))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, listResponse{Count: len(jobs), Jobs: jobs})
}

func (h *AdminHandler) fail(w http.ResponseWriter, r *http.Request, err error) {
	h.log.Error("admin request failed", zap.Error(err))
	httpx.WriteError(w, r, http.StatusInternalServerError, "internal_error", "request failed")
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}
