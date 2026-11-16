package ratelimitd

import (
	"net/http"
	"strconv"
	"time"
)

// TenantFunc returns the tenant a request is billed to. Returning "" rejects
// the request instead of rate limiting it, so a missing header cannot drain
// another tenant's budget.
type TenantFunc func(*http.Request) string

// LimitFunc returns the policy for a tenant. Size plans here; keep it cheap
// because it runs on every request.
type LimitFunc func(tenant string) Limit

// Config configures Middleware.
type Config struct {
	Tenant TenantFunc
	Limit  LimitFunc
	Store  Store
	// OnLimited is called after a rejection, for logs and counters.
	OnLimited func(*http.Request, Decision)
	// Now is injectable for tests and defaults to time.Now.
	Now func() time.Time
}

func (c Config) withDefaults() Config {
	if c.Store == nil {
		c.Store = NewMemoryStore(10 * time.Minute)
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Middleware enforces the tenant limit. Rejections are 429 with Retry-After
// and a Limit header; every response carries the remaining budget.
//
// Store errors fail closed (503) because unbounded traffic is worse than a
// short queue while the store recovers.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	cfg = cfg.withDefaults()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenant := cfg.Tenant(r)
			limit := cfg.Limit(tenant)
			dec, err := cfg.Store.Take(r.Context(), tenant, limit, cfg.Now())
			if err != nil {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("RateLimit-Limit", strconv.Itoa(limit.Burst))
			w.Header().Set("RateLimit-Remaining", strconv.Itoa(dec.Remaining))
			if dec.Allowed {
				next.ServeHTTP(w, r)
				return
			}
			secs := int(dec.RetryAfter.Round(time.Second) / time.Second)
			if secs < 1 {
				secs = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			if cfg.OnLimited != nil {
				cfg.OnLimited(r, dec)
			}
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		})
	}
}

// TenantFromHeader reads the tenant from a header such as X-Tenant.
func TenantFromHeader(name string) TenantFunc {
	return func(r *http.Request) string { return r.Header.Get(name) }
}

// FixedLimit returns a LimitFunc that applies one policy to every tenant.
func FixedLimit(burst int, rate float64) LimitFunc {
	limit := Limit{Burst: burst, Rate: rate}
	return func(string) Limit { return limit }
}
