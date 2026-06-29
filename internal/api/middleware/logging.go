package middleware

import (
	"net/http"
	"time"

	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/viveksoni003/ingress-api-gateway/internal/logger"
	"go.uber.org/zap"
)

// Logging emits one structured access-log line per request, enriched with the
// request_id and trace_id so it can be correlated with worker logs.
func Logging(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			log.Info("http_request",
				zap.String("request_id", logger.RequestID(r.Context())),
				zap.String("trace_id", logger.TraceID(r.Context())),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", ww.Status()),
				zap.Int("bytes", ww.BytesWritten()),
				zap.Duration("latency", time.Since(start)),
				zap.String("remote_ip", r.RemoteAddr),
			)
		})
	}
}
