// Package service holds the application use-cases: the gateway enqueue path
// (validation + idempotency + persistence + queueing) and the admin job
// operations. It depends only on domain ports, never on concrete adapters.
package service

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
)

// payloadHash derives a stable idempotency key from the job type and the raw
// request body, used when the client does not supply an Idempotency-Key.
func payloadHash(jobType domain.JobType, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(jobType))
	h.Write([]byte{':'})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// idempotencyRedisKey namespaces idempotency keys in Redis by job type.
func idempotencyRedisKey(jobType domain.JobType, key string) string {
	return "idemp:" + string(jobType) + ":" + key
}
