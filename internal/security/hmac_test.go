package security

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyHMACSHA256(t *testing.T) {
	secret := "whsec_test"
	body := []byte(`{"gateway_order_id":"order_123","payment_status":"CAPTURED"}`)
	sig := ComputeHMACSHA256(secret, body)

	t.Run("valid signature", func(t *testing.T) {
		require.True(t, VerifyHMACSHA256(secret, body, sig))
	})
	t.Run("tolerates sha256= prefix", func(t *testing.T) {
		require.True(t, VerifyHMACSHA256(secret, body, "sha256="+sig))
	})
	t.Run("rejects wrong signature", func(t *testing.T) {
		require.False(t, VerifyHMACSHA256(secret, body, "deadbeef"))
	})
	t.Run("rejects wrong secret", func(t *testing.T) {
		require.False(t, VerifyHMACSHA256("other_secret", body, sig))
	})
	t.Run("rejects tampered body", func(t *testing.T) {
		require.False(t, VerifyHMACSHA256(secret, []byte(`{"tampered":true}`), sig))
	})
	t.Run("rejects empty signature", func(t *testing.T) {
		require.False(t, VerifyHMACSHA256(secret, body, ""))
	})
}
