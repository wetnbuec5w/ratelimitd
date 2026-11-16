package route

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wetnbuec5w/ratelimitd"
)

func serve(h http.Handler, path, tenant string) int {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if tenant != "" {
		req.Header.Set("X-Tenant", tenant)
	}
	h.ServeHTTP(rec, req)
	return rec.Code
}

func newRouter(skip ...string) http.Handler {
	rt := New(
		ratelimitd.TenantFromHeader("X-Tenant"),
		ratelimitd.NewMemoryStore(time.Minute),
		map[string]ratelimitd.Limit{"/v1/search": {Burst: 1, Rate: 1}},
		ratelimitd.FixedLimit(5, 1),
		skip...,
	)
	return rt.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func TestOverrideHasOwnBucket(t *testing.T) {
	h := newRouter()

	// Exhaust the search bucket for one tenant.
	if got := serve(h, "/v1/search?q=x", "acme"); got != http.StatusOK {
		t.Fatalf("first search = %d, want 200", got)
	}
	if got := serve(h, "/v1/search?q=y", "acme"); got != http.StatusTooManyRequests {
		t.Fatalf("second search = %d, want 429", got)
	}
	// The tenant plan is untouched.
	if got := serve(h, "/v1/orders", "acme"); got != http.StatusOK {
		t.Fatalf("orders after search burst = %d, want 200", got)
	}
	// Another tenant keeps its own search bucket.
	if got := serve(h, "/v1/search?q=z", "globex"); got != http.StatusOK {
		t.Fatalf("globex search = %d, want 200", got)
	}
}

func TestLongestPrefixWins(t *testing.T) {
	rt := New(
		ratelimitd.TenantFromHeader("X-Tenant"),
		ratelimitd.NewMemoryStore(time.Minute),
		map[string]ratelimitd.Limit{
			"/v1":        {Burst: 100, Rate: 10},
			"/v1/search": {Burst: 1, Rate: 1},
		},
		nil,
	)
	if got := rt.match("/v1/search/typo"); got != "/v1/search" {
		t.Fatalf("match = %q, want /v1/search", got)
	}
	if got := rt.match("/v1/orders"); got != "/v1" {
		t.Fatalf("match = %q, want /v1", got)
	}
	if got := rt.match("/v10"); got != "" {
		t.Fatalf("match /v10 = %q, want fallback", got)
	}
}

func TestSkipBypassesLimiter(t *testing.T) {
	h := newRouter("/healthz")
	for i := range 10 {
		if got := serve(h, "/healthz", ""); got != http.StatusOK {
			t.Fatalf("healthz call %d = %d, want 200", i, got)
		}
	}
}

func TestMissingTenantIsRejected(t *testing.T) {
	h := newRouter()
	if got := serve(h, "/v1/orders", ""); got != http.StatusServiceUnavailable {
		t.Fatalf("no tenant = %d, want 503", got)
	}
}
