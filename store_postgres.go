package ratelimitd

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Schema is the table PostgresStore expects:
//
//	CREATE TABLE rate_limit_buckets (
//	  tenant     text PRIMARY KEY,
//	  tokens     double precision NOT NULL,
//	  updated_at timestamptz NOT NULL
//	);
const Schema = `CREATE TABLE IF NOT EXISTS rate_limit_buckets (
	tenant     text PRIMARY KEY,
	tokens     double precision NOT NULL,
	updated_at timestamptz NOT NULL
)`

// PostgresStore shares buckets between replicas. Refill happens inside the
// upsert so a slow replica cannot spend the same tokens twice, and the row
// lock from FOR UPDATE serialises concurrent writers per tenant.
type PostgresStore struct {
	DB *sql.DB
}

// NewPostgresStore creates the bucket table if needed and returns the store.
func NewPostgresStore(ctx context.Context, db *sql.DB) (*PostgresStore, error) {
	if _, err := db.ExecContext(ctx, Schema); err != nil {
		return nil, fmt.Errorf("create rate_limit_buckets: %w", err)
	}
	return &PostgresStore{DB: db}, nil
}

// Take implements Store.
func (s *PostgresStore) Take(ctx context.Context, key string, limit Limit, now time.Time) (Decision, error) {
	if key == "" {
		return Decision{}, ErrEmptyTenant
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Decision{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// New tenants start full; existing rows refill by elapsed seconds and
	// stay capped at Burst.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO rate_limit_buckets (tenant, tokens, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant) DO UPDATE
		SET tokens = LEAST($2::double precision, rate_limit_buckets.tokens
		        + EXTRACT(EPOCH FROM $3::timestamptz - rate_limit_buckets.updated_at) * $4::double precision),
		    updated_at = $3::timestamptz`,
		key, float64(limit.Burst), now, limit.Rate); err != nil {
		return Decision{}, err
	}

	var tokens float64
	if err := tx.QueryRowContext(ctx,
		`SELECT tokens FROM rate_limit_buckets WHERE tenant = $1 FOR UPDATE`,
		key).Scan(&tokens); err != nil {
		return Decision{}, err
	}

	dec := Decision{}
	if tokens >= 1 {
		tokens--
		dec.Allowed = true
		dec.Remaining = int(tokens)
		if _, err := tx.ExecContext(ctx,
			`UPDATE rate_limit_buckets SET tokens = $2 WHERE tenant = $1`,
			key, tokens); err != nil {
			return Decision{}, err
		}
	} else {
		dec.RetryAfter = retryAfter(tokens, limit)
	}

	if err := tx.Commit(); err != nil {
		return Decision{}, err
	}
	return dec, nil
}
