package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) *Limiter {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb)
}

// TestAllowUnderLimit verifies tokens are granted when the bucket has capacity.
func TestAllowUnderLimit(t *testing.T) {
	lim := setup(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		res, err := lim.Allow(ctx, "task.a", 5, time.Second)
		require.NoError(t, err)
		assert.True(t, res.Allowed)
		assert.Equal(t, int64(4-i), res.Remaining)
	}
}

// TestAllowExceedsLimit verifies the limiter denies requests past the max.
func TestAllowExceedsLimit(t *testing.T) {
	lim := setup(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res, err := lim.Allow(ctx, "task.b", 3, time.Second)
		require.NoError(t, err)
		assert.True(t, res.Allowed)
	}

	res, err := lim.Allow(ctx, "task.b", 3, time.Second)
	require.NoError(t, err)
	assert.False(t, res.Allowed)
	assert.Greater(t, res.RetryIn, time.Duration(0))
}

// TestAllowSeparateKinds verifies different kinds have independent buckets.
func TestAllowSeparateKinds(t *testing.T) {
	lim := setup(t)
	ctx := context.Background()

	res, err := lim.Allow(ctx, "kind.x", 1, time.Second)
	require.NoError(t, err)
	assert.True(t, res.Allowed)

	denied, err := lim.Allow(ctx, "kind.x", 1, time.Second)
	require.NoError(t, err)
	assert.False(t, denied.Allowed)

	other, err := lim.Allow(ctx, "kind.y", 1, time.Second)
	require.NoError(t, err)
	assert.True(t, other.Allowed)
}
