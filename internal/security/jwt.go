package security

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ErrInvalidToken is returned when a token fails verification.
var ErrInvalidToken = errors.New("invalid token")

// Claims are the JWT claims carried by admin tokens.
type Claims struct {
	Role string `json:"role"`
	jwt.RegisteredClaims
}

// JWTManager signs and verifies HS256 tokens for the admin API.
type JWTManager struct {
	secret []byte
	issuer string
}

// NewJWTManager builds a manager from a shared secret and issuer.
func NewJWTManager(secret, issuer string) *JWTManager {
	return &JWTManager{secret: []byte(secret), issuer: issuer}
}

// Generate issues a signed token for subject with the given role and TTL.
func (m *JWTManager) Generate(subject, role string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			Issuer:    m.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secret)
}

// Verify parses and validates a token, returning its claims. It enforces the
// HMAC signing method (preventing the classic "alg: none" / RS-vs-HS confusion
// attack) and the configured issuer.
func (m *JWTManager) Verify(tokenString string) (*Claims, error) {
	keyFunc := func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: unexpected signing method %q", ErrInvalidToken, t.Header["alg"])
		}
		return m.secret, nil
	}

	opts := []jwt.ParserOption{jwt.WithValidMethods([]string{"HS256"})}
	if m.issuer != "" {
		opts = append(opts, jwt.WithIssuer(m.issuer))
	}

	parsed, err := jwt.ParseWithClaims(tokenString, &Claims{}, keyFunc, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return nil, ErrInvalidToken
	}
	return claims, nil
}
