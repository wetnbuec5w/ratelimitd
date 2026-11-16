// Package route layers per-route rate limits over ratelimitd, so expensive
// endpoints get their own bucket instead of competing with the tenant plan.
//
// The longest matching prefix wins. Overrides are keyed as tenant plus scope,
// which keeps one hot endpoint from draining the budget of the rest of the API
// for the same tenant.
package route

import (
	"net/http"
	"sort"
	"strings"

	"github.com/wetnbuec5w/ratelimitd"
)

const scopeSep = "\x1f"

type rule struct {
	prefix string
	limit  ratelimitd.Limit
}

// Router builds a middleware that picks a limit per path prefix. Fallback
// applies to everything without an override and to tenants priced by plan.
type Router struct {
	tenant   ratelimitd.TenantFunc
	store    ratelimitd.Store
	rules    []rule
	fallback ratelimitd.LimitFunc
	skip     map[string]struct{}
}

// New validates and orders rules longest prefix first.
func New(tenant ratelimitd.TenantFunc, store ratelimitd.Store, rules map[string]ratelimitd.Limit, fallback ratelimitd.LimitFunc, skip ...string) *Router {
	rt := &Router{tenant: tenant, store: store, fallback: fallback, skip: make(map[string]struct{}, len(skip))}
	for _, p := range skip {
		rt.skip[p] = struct{}{}
	}
	for p, l := range rules {
		rt.rules = append(rt.rules, rule{prefix: p, limit: l})
	}
	sort.Slice(rt.rules, func(i, j int) bool { return len(rt.rules[i].prefix) > len(rt.rules[j].prefix) })
	return rt
}

// match returns the prefix owning path, or "" when the fallback applies.
// Prefixes match on segment boundaries, so "/v1/se" does not catch
// "/v1/search".
func (rt *Router) match(path string) string {
	for _, r := range rt.rules {
		if path == r.prefix {
			return r.prefix
		}
		if strings.HasPrefix(path, r.prefix) && strings.HasPrefix(path[len(r.prefix):], "/") {
			return r.prefix
		}
	}
	return ""
}

func (rt *Router) config(limit ratelimitd.LimitFunc, scope string) ratelimitd.Config {
	key := rt.tenant
	if scope != "" {
		key = func(r *http.Request) string {
			tenant := rt.tenant(r)
			if tenant == "" {
				return ""
			}
			return tenant + scopeSep + scope
		}
	}
	return ratelimitd.Config{Tenant: key, Limit: limit, Store: rt.store}
}

func fixed(l ratelimitd.Limit) ratelimitd.LimitFunc {
	return func(string) ratelimitd.Limit { return l }
}

// Middleware prebuilds one child handler per rule, so dispatch costs a prefix
// scan and no per-request allocation beyond the store Take.
func (rt *Router) Middleware() func(http.Handler) http.Handler {
	if rt.fallback == nil {
		rt.fallback = fixed(ratelimitd.Limit{Burst: 60, Rate: 1})
	}
	return func(next http.Handler) http.Handler {
		handlers := make(map[string]http.Handler, len(rt.rules))
		for _, r := range rt.rules {
			handlers[r.prefix] = ratelimitd.Middleware(rt.config(fixed(r.limit), r.prefix))(next)
		}
		fallback := ratelimitd.Middleware(rt.config(rt.fallback, ""))(next)

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := rt.skip[r.URL.Path]; ok {
				next.ServeHTTP(w, r)
				return
			}
			if h, ok := handlers[rt.match(r.URL.Path)]; ok {
				h.ServeHTTP(w, r)
				return
			}
			fallback.ServeHTTP(w, r)
		})
	}
}
