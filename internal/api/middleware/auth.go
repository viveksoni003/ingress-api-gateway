package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/viveksoni003/ingress-api-gateway/internal/api/httpx"
	"github.com/viveksoni003/ingress-api-gateway/internal/security"
	"go.uber.org/zap"
)

type ctxKey int

const claimsKey ctxKey = iota

// JWTAuth verifies a Bearer token using the JWT manager and, when requiredRole
// is non-empty, enforces that the token carries that role. It guards the admin
// API.
func JWTAuth(jm *security.JWTManager, requiredRole string, log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			const prefix = "Bearer "
			authz := r.Header.Get("Authorization")
			if !strings.HasPrefix(authz, prefix) {
				httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or malformed Authorization header")
				return
			}

			claims, err := jm.Verify(strings.TrimSpace(strings.TrimPrefix(authz, prefix)))
			if err != nil {
				log.Debug("jwt verification failed", zap.Error(err))
				httpx.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "invalid or expired token")
				return
			}
			if requiredRole != "" && claims.Role != requiredRole {
				httpx.WriteError(w, r, http.StatusForbidden, "forbidden", "insufficient role")
				return
			}

			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ClaimsFrom returns the verified JWT claims stored on the context, if any.
func ClaimsFrom(ctx context.Context) (*security.Claims, bool) {
	c, ok := ctx.Value(claimsKey).(*security.Claims)
	return c, ok
}
