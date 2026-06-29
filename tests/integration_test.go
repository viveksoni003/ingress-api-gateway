// Package tests contains end-to-end integration tests that exercise the full
// HTTP -> queue -> worker -> store pipeline. They use an in-process Redis
// (miniredis) and an in-memory store so they run anywhere without Docker.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viveksoni003/ingress-api-gateway/internal/api"
	"github.com/viveksoni003/ingress-api-gateway/internal/api/handlers"
	"github.com/viveksoni003/ingress-api-gateway/internal/cache"
	"github.com/viveksoni003/ingress-api-gateway/internal/domain"
	"github.com/viveksoni003/ingress-api-gateway/internal/observability"
	"github.com/viveksoni003/ingress-api-gateway/internal/queue"
	"github.com/viveksoni003/ingress-api-gateway/internal/ratelimiter"
	"github.com/viveksoni003/ingress-api-gateway/internal/security"
	"github.com/viveksoni003/ingress-api-gateway/internal/service"
	"github.com/viveksoni003/ingress-api-gateway/internal/testutil"
	"github.com/viveksoni003/ingress-api-gateway/internal/worker"
	"go.uber.org/zap"
)

const (
	testJWTSecret  = "integration-secret"
	testJWTIssuer  = "ingress-api-gateway"
	testHMACSecret = "whsec_integration"
)

type harness struct {
	server *httptest.Server
	store  *testutil.InMemoryStore
	jwt    *security.JWTManager
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	rdb := testutil.NewMiniRedis(t)
	q := queue.NewRedisQueue(rdb, 50*time.Millisecond)
	c := cache.NewRedisCache(rdb)
	store := testutil.NewInMemoryStore()
	metrics := observability.New()
	log := zap.NewNop()
	jwtMgr := security.NewJWTManager(testJWTSecret, testJWTIssuer)

	gatewaySvc := service.NewGatewayService(q, store, c, metrics, log, service.GatewayConfig{MaxRetries: 3, IdempotencyTTL: time.Hour})
	adminSvc := service.NewAdminService(q, store, log)

	processors := []domain.Processor{
		worker.NewRegistrationProcessor(store, store, gatewaySvc, log),
		worker.NewPaymentProcessor(store, store, log),
		worker.NewQRScanProcessor(store, store, c, time.Second, log),
		worker.NewNotificationProcessor(store, store, nil, log),
	}
	pool := worker.NewPool(worker.PoolDeps{
		Queue: q, Jobs: store, Cache: c, Metrics: metrics, Logger: log,
		Processors: processors, WorkerCount: 4, RetryBase: 5 * time.Millisecond, RetryMax: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	clientLimiter := ratelimiter.NewKeyedLimiter(100000, 100000, time.Minute)
	router := api.NewRouter(api.RouterDeps{
		Logger:        log,
		Metrics:       metrics,
		JWT:           jwtMgr,
		Gateway:       handlers.NewGatewayHandler(gatewaySvc, testHMACSecret, log),
		Admin:         handlers.NewAdminHandler(adminSvc, log),
		Health:        handlers.NewHealthHandler(q, store, "test"),
		GlobalLimiter: ratelimiter.NewGlobalLimiter(100000, 100000),
		ClientLimiter: clientLimiter,
		Queue:         q,
		MaxBodyBytes:  1 << 20,
		QueueMaxDepth: 1000000,
	})
	srv := httptest.NewServer(router)

	t.Cleanup(func() {
		srv.Close()
		cancel()
		clientLimiter.Close()
		sc, c2 := context.WithTimeout(context.Background(), 2*time.Second)
		defer c2()
		_ = pool.Shutdown(sc)
	})

	return &harness{server: srv, store: store, jwt: jwtMgr}
}

func (h *harness) do(t *testing.T, method, path string, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, h.server.URL+path, rdr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.server.Client().Do(req)
	require.NoError(t, err)
	return resp
}

func decodeAccept(t *testing.T, resp *http.Response) handlers.AcceptResponse {
	t.Helper()
	defer resp.Body.Close()
	var out handlers.AcceptResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func (h *harness) adminToken(t *testing.T) string {
	t.Helper()
	tok, err := h.jwt.Generate("admin-user", "admin", time.Hour)
	require.NoError(t, err)
	return "Bearer " + tok
}

func TestRegistrationEndToEnd(t *testing.T) {
	h := newHarness(t)

	body := []byte(`{"event_id":"evt-1","attendee_name":"Vivek","email":"vivek@example.com","ticket_type":"VIP"}`)
	resp := h.do(t, http.MethodPost, "/api/v1/gateway/registrations", body, nil)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	accepted := decodeAccept(t, resp)
	require.NotEmpty(t, accepted.JobID)
	require.False(t, accepted.Duplicate)

	// The job should be processed: a registration row stored and a welcome
	// notification fanned out and delivered.
	require.Eventually(t, func() bool {
		j, err := h.store.GetByID(context.Background(), accepted.JobID)
		return err == nil && j.Status == domain.JobStatusSuccess
	}, 3*time.Second, 20*time.Millisecond)

	require.Len(t, h.store.Registrations, 1)
	require.Eventually(t, func() bool {
		return len(h.store.Notifications) >= 1
	}, 3*time.Second, 20*time.Millisecond, "registration should fan out a welcome notification")
}

func TestIdempotentRegistration(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"event_id":"evt-2","attendee_name":"Dup","email":"dup@example.com"}`)
	headers := map[string]string{"Idempotency-Key": "idem-key-abc"}

	first := h.do(t, http.MethodPost, "/api/v1/gateway/registrations", body, headers)
	require.Equal(t, http.StatusAccepted, first.StatusCode)
	a1 := decodeAccept(t, first)

	second := h.do(t, http.MethodPost, "/api/v1/gateway/registrations", body, headers)
	require.Equal(t, http.StatusOK, second.StatusCode, "duplicate returns 200, not 202")
	a2 := decodeAccept(t, second)

	require.True(t, a2.Duplicate)
	require.Equal(t, a1.JobID, a2.JobID)
}

func TestPaymentWebhookSignature(t *testing.T) {
	h := newHarness(t)
	body := []byte(`{"gateway_order_id":"order_777","payment_id":"pay_1","payment_status":"CAPTURED","amount_cents":50000,"currency":"INR"}`)

	// Missing/invalid signature -> 401.
	bad := h.do(t, http.MethodPost, "/api/v1/gateway/payment-webhooks", body, nil)
	require.Equal(t, http.StatusUnauthorized, bad.StatusCode)
	bad.Body.Close()

	// Valid signature -> 202.
	sig := security.ComputeHMACSHA256(testHMACSecret, body)
	good := h.do(t, http.MethodPost, "/api/v1/gateway/payment-webhooks", body, map[string]string{"X-Signature": sig})
	require.Equal(t, http.StatusAccepted, good.StatusCode)
	accepted := decodeAccept(t, good)

	require.Eventually(t, func() bool {
		j, err := h.store.GetByID(context.Background(), accepted.JobID)
		return err == nil && j.Status == domain.JobStatusSuccess
	}, 3*time.Second, 20*time.Millisecond)
	require.Len(t, h.store.Payments, 1)
}

func TestInvalidPayloadRejected(t *testing.T) {
	h := newHarness(t)
	resp := h.do(t, http.MethodPost, "/api/v1/gateway/registrations", []byte(`{"attendee_name":"NoEmail"}`), nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()
}

func TestAdminRequiresJWT(t *testing.T) {
	h := newHarness(t)

	unauth := h.do(t, http.MethodGet, "/api/v1/admin/jobs", nil, nil)
	require.Equal(t, http.StatusUnauthorized, unauth.StatusCode)
	unauth.Body.Close()

	authed := h.do(t, http.MethodGet, "/api/v1/admin/jobs", nil, map[string]string{"Authorization": h.adminToken(t)})
	require.Equal(t, http.StatusOK, authed.StatusCode)
	authed.Body.Close()
}

func TestHealthEndpoints(t *testing.T) {
	h := newHarness(t)
	for path, want := range map[string]int{"/health": 200, "/live": 200, "/ready": 200} {
		resp := h.do(t, http.MethodGet, path, nil, nil)
		require.Equal(t, want, resp.StatusCode, "path %s", path)
		resp.Body.Close()
	}
}
