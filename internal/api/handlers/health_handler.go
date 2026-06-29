package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
)

// Pinger is anything that can verify its own connectivity (Redis, Postgres).
type Pinger interface {
	Ping(ctx context.Context) error
}

// HealthHandler serves liveness/readiness probes.
//
//	/health -> overall process health (liveness-style)
//	/live   -> liveness: process is up and serving
//	/ready  -> readiness: dependencies (Redis, Postgres) are reachable
type HealthHandler struct {
	redis   Pinger
	db      Pinger
	started time.Time
	version string
}

// NewHealthHandler builds the handler.
func NewHealthHandler(redis, db Pinger, version string) *HealthHandler {
	return &HealthHandler{redis: redis, db: db, started: time.Now(), version: version}
}

// Health reports basic process health and uptime.
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"version":        h.version,
		"uptime_seconds": int64(time.Since(h.started).Seconds()),
	})
}

// Live is the liveness probe: if the process can answer, it is alive.
func (h *HealthHandler) Live(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// Ready is the readiness probe: it returns 503 until all dependencies respond.
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{}
	ready := true

	if err := h.redis.Ping(ctx); err != nil {
		checks["redis"] = "down"
		ready = false
	} else {
		checks["redis"] = "up"
	}
	if err := h.db.Ping(ctx); err != nil {
		checks["postgres"] = "down"
		ready = false
	} else {
		checks["postgres"] = "up"
	}

	status := http.StatusOK
	state := "ready"
	if !ready {
		status = http.StatusServiceUnavailable
		state = "not_ready"
	}
	httpx.WriteJSON(w, status, map[string]any{"status": state, "checks": checks})
}
