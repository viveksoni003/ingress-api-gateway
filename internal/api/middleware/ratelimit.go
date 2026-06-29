package middleware

import (
	"net"
	"net/http"

	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/observability"
)

// KeyFunc derives the rate-limit key for a request.
type KeyFunc func(*http.Request) string

// RateLimit rejects requests with HTTP 429 when the provided limiter denies
// the key. scope ("global" | "route") labels the metric.
func RateLimit(limiter domain.RateLimiter, scope string, key KeyFunc, m *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.Allow(key(r)) {
				m.RateLimited.WithLabelValues(scope).Inc()
				w.Header().Set("Retry-After", "1")
				httpx.WriteError(w, r, http.StatusTooManyRequests, "rate_limited", "too many requests, slow down")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ClientIPKey keys the limiter by client IP (RealIP middleware should run first
// so X-Forwarded-For is honoured behind a load balancer).
func ClientIPKey(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// GlobalKey is a constant key so all traffic shares one global bucket.
func GlobalKey(*http.Request) string { return "global" }
