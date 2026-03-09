package winter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type strictTask struct {
	Seq int `json:"seq"`
}

func (strictTask) Kind() string { return "strict.test" }

func setupStrictServer(t *testing.T, strict bool) (*Server, *Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	client := NewClientFromRedis(rdb)
	server := NewServerFromRedis(rdb, ServerConfig{
		Concurrency:    1,
		StrictPriority: strict,
		Queues:         Queues("critical", 6, "default", 3, "low", 1),
		PollInterval:   10 * time.Millisecond,
	})

	return server, client
}

// TestStrictPriorityDrainsHighFirst verifies that strict mode processes all
// critical jobs before any default or low jobs.
func TestStrictPriorityDrainsHighFirst(t *testing.T) {
	server, client := setupStrictServer(t, true)

	var mu sync.Mutex
	var order []string

	HandleFunc(server, func(_ context.Context, job *Job[strictTask]) error {
		mu.Lock()
		order = append(order, job.Queue)
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	for i := range 3 {
		_, err := Enqueue(client, ctx, strictTask{Seq: i}, Queue("critical"))
		require.NoError(t, err)
	}
	for i := range 3 {
		_, err := Enqueue(client, ctx, strictTask{Seq: i}, Queue("low"))
		require.NoError(t, err)
	}

	go func() { _ = server.Start() }()
	t.Cleanup(func() { cancel(); server.Stop() })

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 6
	}, 5*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// All critical jobs should come before all low jobs.
	for i := range 3 {
		assert.Equal(t, "critical", order[i], "position %d should be critical", i)
	}
	for i := 3; i < 6; i++ {
		assert.Equal(t, "low", order[i], "position %d should be low", i)
	}
}

// TestWeightedModeInterleavesQueues verifies that weighted mode gives both
// queues attention even when the higher-weight queue has work available.
func TestWeightedModeInterleavesQueues(t *testing.T) {
	server, client := setupStrictServer(t, false)

	var mu sync.Mutex
	var order []string

	HandleFunc(server, func(_ context.Context, job *Job[strictTask]) error {
		mu.Lock()
		order = append(order, job.Queue)
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	for i := range 10 {
		_, err := Enqueue(client, ctx, strictTask{Seq: i}, Queue("critical"))
		require.NoError(t, err)
	}
	for i := range 10 {
		_, err := Enqueue(client, ctx, strictTask{Seq: i}, Queue("low"))
		require.NoError(t, err)
	}

	go func() { _ = server.Start() }()
	t.Cleanup(func() { cancel(); server.Stop() })

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 20
	}, 5*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// In weighted mode, low jobs should appear before all critical jobs
	// are exhausted. Find the first low job and verify some critical jobs
	// remain after it.
	firstLow := -1
	lastCritical := -1
	for i, q := range order {
		if q == "low" && firstLow == -1 {
			firstLow = i
		}
		if q == "critical" {
			lastCritical = i
		}
	}

	assert.Greater(t, firstLow, -1, "low queue should have been processed")
	assert.Greater(t, lastCritical, firstLow, "weighted mode should interleave queues")
}
