// Package api assembles the chi router: global middleware, the public gateway
// routes (rate limited + backpressured), the JWT-protected admin routes and the
// health/observability endpoints.
package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/viveksoni003/ingress-api-gateway/internal/api/handlers"
	mw "github.com/viveksoni003/ingress-api-gateway/internal/api/middleware"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/observability"
	"github.com/viveksoni003/ingress-api-gateway/internal/security"
	"go.uber.org/zap"
)

// RouterDeps are the dependencies needed to build the HTTP handler.
type RouterDeps struct {
	Logger        *zap.Logger
	Metrics       *observability.Metrics
	JWT           *security.JWTManager
	Gateway       *handlers.GatewayHandler
	Admin         *handlers.AdminHandler
	Health        *handlers.HealthHandler
	GlobalLimiter domain.RateLimiter
	ClientLimiter domain.RateLimiter
	Queue         domain.Queue
	MaxBodyBytes  int64
	QueueMaxDepth int64
	OpenAPIPath   string
}

// NewRouter wires the full middleware stack and route table.
func NewRouter(d RouterDeps) http.Handler {
	r := chi.NewRouter()

	// Global middleware (outermost first). RealIP must precede rate limiting so
	// per-client buckets key on the true client address behind a load balancer.
	r.Use(chimw.RealIP)
	r.Use(mw.RequestID)
	r.Use(mw.Recovery(d.Logger))
	r.Use(mw.Logging(d.Logger))
	r.Use(mw.Metrics(d.Metrics))

	// Health + metrics: unauthenticated and not rate limited so probes/scrapers
	// always work even under load shedding.
	r.Get("/health", d.Health.Health)
	r.Get("/live", d.Health.Live)
	r.Get("/ready", d.Health.Ready)
	r.Handle("/metrics", d.Metrics.Handler())

	// OpenAPI spec + Swagger UI.
	if d.OpenAPIPath != "" {
		r.Get("/openapi.yaml", func(w http.ResponseWriter, req *http.Request) {
			http.ServeFile(w, req, d.OpenAPIPath)
		})
		r.Get("/docs", swaggerUI)
	}

	r.Route("/api/v1", func(r chi.Router) {
		// Process-wide limiter protecting the whole API surface.
		r.Use(mw.RateLimit(d.GlobalLimiter, "global", mw.GlobalKey, d.Metrics))

		// Public gateway routes: body limit, per-client rate limit, backpressure.
		r.Route("/gateway", func(r chi.Router) {
			r.Use(mw.BodyLimit(d.MaxBodyBytes))
			r.Use(mw.RateLimit(d.ClientLimiter, "route", mw.ClientIPKey, d.Metrics))
			r.Use(mw.Backpressure(d.Queue, d.QueueMaxDepth, d.Metrics, d.Logger))

			r.Post("/registrations", d.Gateway.Registrations)
			r.Post("/payment-webhooks", d.Gateway.PaymentWebhooks)
			r.Post("/qr-scans", d.Gateway.QRScans)
			r.Post("/notifications", d.Gateway.Notifications)
		})

		// Admin routes: JWT (role=admin) required. Static segments are registered
		// before the {id} param so /jobs/stats is not shadowed.
		r.Route("/admin", func(r chi.Router) {
			r.Use(mw.JWTAuth(d.JWT, "admin", d.Logger))

			r.Get("/jobs", d.Admin.ListJobs)
			r.Get("/jobs/stats", d.Admin.Stats)
			r.Get("/jobs/{id}", d.Admin.GetJob)
			r.Post("/jobs/{id}/retry", d.Admin.RetryJob)
			r.Get("/dead-letter-jobs", d.Admin.DeadLetterJobs)
		})
	})

	return r
}

// swaggerUI serves a tiny Swagger UI page (loaded from a CDN) pointed at the
// gateway's /openapi.yaml.
func swaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerHTML))
}

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>Ingress API Gateway — API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" });
    };
  </script>
</body>
</html>`
