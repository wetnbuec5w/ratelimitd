# ratelimitd

Per-tenant token bucket rate limiting for `net/http` services.

## Why

Shared-IP limiting punishes whoever sits behind a NAT and still lets one
tenant flood the queue. `ratelimitd` keys buckets on a tenant you choose (an
API key, an account id from your auth layer) and only that tenant pays for the
burst. Buckets refill continuously, so a tenant recovering from a spike is not
locked out for a whole window.

## Install

```sh
go get github.com/wetnbuec5w/ratelimitd
```

## Usage

```go
store := ratelimitd.NewMemoryStore(15 * time.Minute)

mux := http.NewServeMux()
mux.HandleFunc("/v1/orders", createOrder)

handler := ratelimitd.Middleware(ratelimitd.Config{
	Tenant:    ratelimitd.TenantFromHeader("X-Tenant"),
	Limit:     ratelimitd.FixedLimit(60, 1), // 60 burst, 1 token/s
	Store:     store,
	OnLimited: func(r *http.Request, d ratelimitd.Decision) { slog.Warn("limited", "tenant", r.Header.Get("X-Tenant")) },
})(mux)

http.ListenAndServe(":8080", handler)
```

`Limit` can be a per-plan lookup:

```go
limits := map[string]ratelimitd.Limit{"free": {Burst: 10, Rate: 0.2}}
cfg.Limit = func(tenant string) ratelimitd.Limit {
	if l, ok := limits[tenant]; ok {
		return l
	}
	return ratelimitd.Limit{Burst: 100, Rate: 5}
}
```

## Config

| Field       | Meaning                                                          |
|-------------|------------------------------------------------------------------|
| `Tenant`    | Extracts the tenant. Empty result rejects the request.           |
| `Limit`     | Policy per tenant (burst, tokens/second). Runs on every request. |
| `Store`     | `MemoryStore`, `PostgresStore`, or your own `ratelimitd.Store`.  |
| `OnLimited` | Called after a rejection, for logs and counters.                 |
| `Now`       | Clock override; defaults to `time.Now`.                          |

## Responses

| Status | When                                                        |
|--------|-------------------------------------------------------------|
| 200    | Token taken. `RateLimit-Remaining` reflects the bucket.      |
| 429    | Bucket empty. `Retry-After` is seconds until one token.       |
| 503    | Store error or missing tenant. Fail closed, `Retry-After: 1`. |

Every response carries `RateLimit-Limit` and `RateLimit-Remaining`.
`RetryAfter` is computed from the refill rate, not a fixed window, so clients
that honour it retry exactly when a token exists.

## Stores

`MemoryStore` is process local: each replica has its own budget, total
ingress is `replicas * Burst`. Idle buckets are swept on write
(`NewMemoryStore(maxIdle)`), so tenant churn does not grow the map.

`PostgresStore` keeps one budget for the whole fleet:

```sql
CREATE TABLE IF NOT EXISTS rate_limit_buckets (
  tenant     text PRIMARY KEY,
  tokens     double precision NOT NULL,
  updated_at timestamptz NOT NULL
);
```

`NewPostgresStore(ctx, db)` applies that DDL. Each `Take` is one transaction:
upsert-and-refill, `SELECT ... FOR UPDATE`, then decrement. Expect one row
lock per tenant per request; route heavy data planes to a dedicated database
instead of your primary writer.

## Operational notes

- Buckets are swept lazily. With millions of short-lived tenants, size
  `maxIdle` to your longest realistic retry window.
- `Limit.Rate == 0` gives a burst that never refills until the bucket is swept
  and rebuilt full. Fine for hard per-session caps, wrong for API plans.
- Store latency sits in the request path. Wrap the store with a short
  timeout; at p99 above your own SLO, drop to fail-open by returning an
  allowed decision instead of an error.
- The limiter does not retry. A `429` is final for that request.

## Layout

```
limiter.go        Limit, Decision, Store, MemoryStore
middleware.go     Config, Middleware, TenantFromHeader, FixedLimit
store_postgres.go PostgresStore and its schema
limiter_test.go   burst, refill, sweep, and middleware behaviour
```
