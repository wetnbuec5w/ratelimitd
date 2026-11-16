// Package ratelimitd provides per-tenant token bucket rate limiting
// middleware for net/http services.
//
// Buckets are keyed by tenant and kept in a Store. MemoryStore is process
// local and drops idle buckets; PostgresStore shares one budget across
// replicas. A Take is atomic: a caller either gets a token or a RetryAfter
// delay that is honest about when the bucket refills.
package ratelimitd

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrEmptyTenant is returned when the tenant key is empty.
var ErrEmptyTenant = errors.New("ratelimitd: empty tenant key")

// Limit is a refill policy: a bucket holds Burst tokens and gains Rate
// tokens per second. Rate 0 makes a bucket refill only through Burst resets.
type Limit struct {
	Burst int
	Rate  float64
}

// Decision is the outcome of a single Take.
type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

// Store keeps per-tenant bucket state and must be safe for concurrent use.
type Store interface {
	// Take consumes one token from key's bucket after refilling it to now.
	Take(ctx context.Context, key string, limit Limit, now time.Time) (Decision, error)
}

type bucket struct {
	tokens float64
	last   time.Time
}

// MemoryStore is a process-local Store. Buckets idle longer than maxIdle are
// swept on write so tenant churn does not leak memory.
type MemoryStore struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	maxIdle time.Duration
}

// NewMemoryStore returns a store that forgets buckets idle longer than
// maxIdle. maxIdle <= 0 disables sweeping.
func NewMemoryStore(maxIdle time.Duration) *MemoryStore {
	return &MemoryStore{buckets: make(map[string]*bucket), maxIdle: maxIdle}
}

// Take implements Store.
func (s *MemoryStore) Take(_ context.Context, key string, limit Limit, now time.Time) (Decision, error) {
	if key == "" {
		return Decision{}, ErrEmptyTenant
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)
	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(limit.Burst), last: now}
		s.buckets[key] = b
	}
	refill(b, limit, now)
	if b.tokens < 1 {
		return Decision{RetryAfter: retryAfter(b.tokens, limit)}, nil
	}
	b.tokens--
	return Decision{Allowed: true, Remaining: int(b.tokens)}, nil
}

// Len reports the number of tracked buckets. It is meant for tests and
// readiness checks.
func (s *MemoryStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buckets)
}

func (s *MemoryStore) sweepLocked(now time.Time) {
	if s.maxIdle <= 0 {
		return
	}
	for k, b := range s.buckets {
		if now.Sub(b.last) > s.maxIdle {
			delete(s.buckets, k)
		}
	}
}

func refill(b *bucket, limit Limit, now time.Time) {
	elapsed := now.Sub(b.last).Seconds()
	if elapsed <= 0 {
		return
	}
	b.last = now
	b.tokens += elapsed * limit.Rate
	if burst := float64(limit.Burst); b.tokens > burst {
		b.tokens = burst
	}
}

// retryAfter is the wait until tokens reaches 1 under limit.Rate.
func retryAfter(tokens float64, limit Limit) time.Duration {
	if limit.Rate <= 0 {
		return 0
	}
	return time.Duration((1 - tokens) / limit.Rate * float64(time.Second))
}
