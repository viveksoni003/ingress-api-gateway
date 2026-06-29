// Package httpx contains small HTTP helpers (JSON encoding and a standard error
// envelope) shared by handlers and middleware. Keeping it separate avoids an
// import cycle between the api and middleware packages.
package httpx

import (
	"encoding/json"
	"net/http"

	"github.com/viveksoni003/ingress-api-gateway/internal/logger"
)

// ErrorBody is the standard error envelope returned to clients.
type ErrorBody struct {
	Error     string `json:"error"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
}

// WriteJSON serialises v as JSON with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// WriteError writes a standard JSON error including correlation ids pulled from
// the request context.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	WriteJSON(w, status, ErrorBody{
		Error:     code,
		Detail:    detail,
		RequestID: logger.RequestID(r.Context()),
		TraceID:   logger.TraceID(r.Context()),
	})
}
