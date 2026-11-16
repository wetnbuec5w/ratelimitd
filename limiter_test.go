package ratelimitd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMemoryStoreBurstThenRefill(t *testing.T) {
	store := NewMemoryStore(time.Minute)
	limit := Limit{Burst: 2, Rate: 0.5}
	now := time.Unix(0, 0)

	for i := range 2 {
		dec, err := store.Take(context.Background(), "acme", limit, now)
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
		if !dec.Allowed {
			t.Fatalf("take %d denied inside burst", i)
		}
	}

	dec, err := store.Take(context.Background(), "acme", limit, now)
	if err != nil {
		t.Fatalf("take over burst: %v", err)
	}
	// A 0.5 token/s refill needs 2s to hand back one token.
	if dec.Allowed || dec.RetryAfter != 2*time.Second {
		t.Fatalf("got allowed=%v retry=%v, want denied with 2s", dec.Allowed, dec.RetryAfter)
	}

	dec, err = store.Take(context.Background(), "acme", limit, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("take after refill: %v", err)
	}
	if !dec.Allowed {
		t.Fatal("token not returned after refill window")
	}
}

func TestMemoryStoreSweepsIdleBuckets(t *testing.T) {
	store := NewMemoryStore(time.Minute)
	limit := Limit{Burst: 5, Rate: 1}
	now := time.Unix(0, 0)

	if _, err := store.Take(context.Background(), "acme", limit, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Take(context.Background(), "globex", limit, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := store.Len(); got != 1 {
		t.Fatalf("len = %d, want 1 after sweep", got)
	}
}

func TestMiddlewareRejectsWithRetryAfter(t *testing.T) {
	var limited int
	h := Middleware(Config{
		Tenant:    TenantFromHeader("X-Tenant"),
		Limit:     FixedLimit(1, 1),
		OnLimited: func(*http.Request, Decision) { limited++ },
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Tenant", "acme")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req.Clone(context.Background()))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	if limited != 1 {
		t.Fatalf("OnLimited calls = %d, want 1", limited)
	}
}

func TestMiddlewareSeparatesTenants(t *testing.T) {
	h := Middleware(Config{
		Tenant: TenantFromHeader("X-Tenant"),
		Limit:  FixedLimit(1, 1),
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, tenant := range []string{"acme", "globex"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Tenant", tenant)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("tenant %s = %d, want 200", tenant, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req) // no tenant header
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing tenant = %d, want 503", rec.Code)
	}
}
