package middleware

import (
	"net/http"

	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/observability"
	"go.uber.org/zap"
)

// Backpressure implements load shedding: before accepting a job it checks the
// total queue depth and, if it is at or above the threshold, returns 503 so the
// client can back off. This protects workers and downstream stores from being
// overwhelmed during traffic spikes.
func Backpressure(q domain.Queue, threshold int64, m *observability.Metrics, log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if threshold > 0 {
				depth, err := q.TotalDepth(r.Context())
				if err != nil {
					// Fail open: a metrics/Redis hiccup should not reject traffic,
					// but log it for visibility.
					log.Warn("backpressure depth check failed; allowing request", zap.Error(err))
				} else if depth >= threshold {
					m.LoadShed.Inc()
					log.Warn("shedding load due to queue backpressure",
						zap.Int64("queue_depth", depth),
						zap.Int64("threshold", threshold))
					w.Header().Set("Retry-After", "2")
					httpx.WriteError(w, r, http.StatusServiceUnavailable, "service_unavailable", "queue saturated, please retry shortly")
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
