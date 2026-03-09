// Package ratelimit implements a Redis-backed token bucket rate limiter.
// Each task kind gets its own bucket identified by a Redis key. The bucket
// refills at a steady rate and allows bursts up to the configured maximum.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter is a per-key token bucket rate limiter backed by Redis.
type Limiter struct {
	rdb redis.UniversalClient
}

// New creates a limiter from an existing Redis connection.
func New(rdb redis.UniversalClient) *Limiter {
	return &Limiter{rdb: rdb}
}

// Result contains the outcome of a rate limit check.
type Result struct {
	Allowed   bool
	Remaining int64
	RetryIn   time.Duration
}

// tokenBucketScript atomically checks and decrements a token bucket.
// KEYS[1] is the bucket key. ARGV[1] is max tokens, ARGV[2] is the refill
// interval in milliseconds, ARGV[3] is the current time in milliseconds.
//
// The bucket is stored as a hash with two fields: tokens (current count)
// and last_refill (timestamp of last refill). On each call, elapsed time
// is used to add tokens up to the max, then one token is consumed if available.
// Returns {1, remaining} if allowed or {0, retry_in_ms} if denied.
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local max = tonumber(ARGV[1])
local intervalMs = tonumber(ARGV[2])
local nowMs = tonumber(ARGV[3])

local tokens = tonumber(redis.call("HGET", key, "tokens"))
local lastRefill = tonumber(redis.call("HGET", key, "last_refill"))

if tokens == nil then
    tokens = max
    lastRefill = nowMs
end

local elapsed = nowMs - lastRefill
local refillRate = max / intervalMs
local added = elapsed * refillRate

tokens = math.min(max, tokens + added)
lastRefill = nowMs

if tokens >= 1 then
    tokens = tokens - 1
    redis.call("HSET", key, "tokens", tokens, "last_refill", lastRefill)
    redis.call("PEXPIRE", key, intervalMs * 2)
    return {1, math.floor(tokens)}
end

local deficit = 1 - tokens
local retryMs = math.ceil(deficit / refillRate)
redis.call("HSET", key, "tokens", tokens, "last_refill", lastRefill)
redis.call("PEXPIRE", key, intervalMs * 2)
return {0, retryMs}
`)

func bucketKey(kind string) string { return "winter:ratelimit:" + kind }

// Allow checks whether a single token is available for the given task kind
// with the specified rate configuration (max tokens per interval).
func (l *Limiter) Allow(ctx context.Context, kind string, max int, per time.Duration) (*Result, error) {
	nowMs := time.Now().UnixMilli()
	intervalMs := per.Milliseconds()

	vals, err := tokenBucketScript.Run(ctx, l.rdb, []string{bucketKey(kind)}, max, intervalMs, nowMs).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("winter: rate limit: %w", err)
	}

	if vals[0] == 1 {
		return &Result{Allowed: true, Remaining: vals[1]}, nil
	}

	return &Result{
		Allowed: false,
		RetryIn: time.Duration(vals[1]) * time.Millisecond,
	}, nil
}
