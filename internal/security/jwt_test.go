package security

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJWTGenerateAndVerify(t *testing.T) {
	m := NewJWTManager("super-secret", "ingress-api-gateway")

	token, err := m.Generate("admin-1", "admin", time.Hour)
	require.NoError(t, err)

	claims, err := m.Verify(token)
	require.NoError(t, err)
	require.Equal(t, "admin", claims.Role)
	require.Equal(t, "admin-1", claims.Subject)
}

func TestJWTRejectsWrongSecret(t *testing.T) {
	signer := NewJWTManager("secret-a", "ingress-api-gateway")
	verifier := NewJWTManager("secret-b", "ingress-api-gateway")

	token, err := signer.Generate("admin-1", "admin", time.Hour)
	require.NoError(t, err)

	_, err = verifier.Verify(token)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestJWTRejectsExpired(t *testing.T) {
	m := NewJWTManager("secret", "ingress-api-gateway")
	token, err := m.Generate("admin-1", "admin", -time.Minute) // already expired
	require.NoError(t, err)

	_, err = m.Verify(token)
	require.Error(t, err)
}
