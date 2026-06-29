// Package middleware contains the gateway's HTTP middleware: correlation ids,
// structured logging, panic recovery, Prometheus metrics, JWT auth, token-
// bucket rate limiting, body-size limits and queue backpressure / load
// shedding.
package middleware

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/viveksoni003/ingress-api-gateway/internal/logger"
)

// Correlation header names.
const (
	HeaderRequestID = "X-Request-ID"
	HeaderTraceID   = "X-Trace-ID"
)

// RequestID ensures every request has a request_id and trace_id, reusing
// inbound values when present (so traces propagate across services) and
// generating them otherwise. Both are echoed back as response headers and
// stored on the context for logging.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(HeaderRequestID)
		if requestID == "" {
			requestID = uuid.NewString()
		}
		traceID := r.Header.Get(HeaderTraceID)
		if traceID == "" {
			traceID = uuid.NewString()
		}

		w.Header().Set(HeaderRequestID, requestID)
		w.Header().Set(HeaderTraceID, traceID)

		ctx := logger.WithIDs(r.Context(), requestID, traceID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
