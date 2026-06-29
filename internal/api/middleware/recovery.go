package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
	"github.com/viveksoni003/ingress-api-gateway/internal/logger"
	"go.uber.org/zap"
)

// Recovery converts panics into a logged 500 response so a single bad request
// can never take down the process.
func Recovery(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic recovered",
						zap.Any("panic", rec),
						zap.String("request_id", logger.RequestID(r.Context())),
						zap.String("path", r.URL.Path),
						zap.ByteString("stack", debug.Stack()),
					)
					httpx.WriteError(w, r, http.StatusInternalServerError, "internal_error", "unexpected server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
