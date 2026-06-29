// Package security holds authentication and integrity primitives: JWT for the
// admin API and HMAC-SHA256 signature verification for payment webhooks.
package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// ComputeHMACSHA256 returns the lowercase hex HMAC-SHA256 of payload under
// secret. This is the canonical signature scheme used by Razorpay/Stripe-style
// webhooks.
func ComputeHMACSHA256(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyHMACSHA256 reports whether signature matches the HMAC of payload. The
// comparison is constant-time to avoid timing attacks. A leading "sha256="
// prefix (used by some providers) is tolerated.
func VerifyHMACSHA256(secret string, payload []byte, signature string) bool {
	if signature == "" {
		return false
	}
	provided := strings.TrimPrefix(signature, "sha256=")
	expected := ComputeHMACSHA256(secret, payload)
	// hmac.Equal is constant-time; lengths must match first.
	return hmac.Equal([]byte(expected), []byte(provided))
}
