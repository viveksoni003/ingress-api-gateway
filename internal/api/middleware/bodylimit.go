package middleware

import (
	"net/http"

	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
)

// BodyLimit caps the request body size. It rejects an oversized Content-Length
// up front with 413 and also wraps the body in a MaxBytesReader so chunked
// requests are truncated safely.
func BodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if maxBytes > 0 {
				if r.ContentLength > maxBytes {
					httpx.WriteError(w, r, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds limit")
					return
				}
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}
			next.ServeHTTP(w, r)
		})
	}
}
