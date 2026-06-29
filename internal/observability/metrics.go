// Package observability exposes Prometheus metrics for the gateway. A private
// registry is used (instead of the global default) so metrics are isolated and
// tests can construct independent instances without "duplicate collector"
// panics.
package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every collector the application reports.
type Metrics struct {
	reg *prometheus.Registry

	// HTTP layer.
	HTTPRequests *prometheus.CounterVec   // by method, route, status
	HTTPDuration *prometheus.HistogramVec // by method, route

	// Queue + workers.
	QueueDepth      *prometheus.GaugeVec   // by priority
	DeadLetterDepth prometheus.Gauge       // dead-letter depth
	JobsProcessed   *prometheus.CounterVec // by job_type, status
	JobsFailed     *prometheus.CounterVec // by job_type
	JobsRetried    *prometheus.CounterVec // by job_type
	JobsDeadLetter *prometheus.CounterVec // by job_type
	WorkersActive  prometheus.Gauge

	// Gateway protection.
	RateLimited     *prometheus.CounterVec // by scope (global|route)
	LoadShed        prometheus.Counter
	IdempotencyHits prometheus.Counter
}

// New builds and registers all collectors on a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	// Standard Go runtime + process collectors are handy in dashboards.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	f := promauto.With(reg)

	return &Metrics{
		reg: reg,
		HTTPRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_http_requests_total",
			Help: "Total HTTP requests processed by the gateway.",
		}, []string{"method", "route", "status"}),
		HTTPDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gateway_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"method", "route"}),
		QueueDepth: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_queue_depth",
			Help: "Current depth of each priority queue.",
		}, []string{"priority"}),
		DeadLetterDepth: f.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_dead_letter_depth",
			Help: "Current depth of the dead-letter queue.",
		}),
		JobsProcessed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_jobs_processed_total",
			Help: "Total jobs processed, labelled by type and terminal status.",
		}, []string{"job_type", "status"}),
		JobsFailed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_jobs_failed_total",
			Help: "Total job processing attempts that returned an error.",
		}, []string{"job_type"}),
		JobsRetried: f.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_jobs_retried_total",
			Help: "Total jobs re-queued for retry.",
		}, []string{"job_type"}),
		JobsDeadLetter: f.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_jobs_dead_letter_total",
			Help: "Total jobs moved to the dead-letter queue.",
		}, []string{"job_type"}),
		WorkersActive: f.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_workers_active",
			Help: "Number of worker goroutines currently processing a job.",
		}),
		RateLimited: f.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_rate_limited_total",
			Help: "Total requests rejected by a rate limiter.",
		}, []string{"scope"}),
		LoadShed: f.NewCounter(prometheus.CounterOpts{
			Name: "gateway_load_shed_total",
			Help: "Total requests shed due to queue backpressure (HTTP 503).",
		}),
		IdempotencyHits: f.NewCounter(prometheus.CounterOpts{
			Name: "gateway_idempotency_hits_total",
			Help: "Total requests served from an existing idempotency key.",
		}),
	}
}

// Handler returns the HTTP handler that exposes the metrics registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Registry exposes the underlying registry (useful in tests).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }
