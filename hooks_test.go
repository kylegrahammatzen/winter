package winter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hookTask struct {
	Value string `json:"value"`
}

func (hookTask) Kind() string { return "hook.test" }

func setupHookServer(t *testing.T) (*Server, *Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	client := NewClientFromRedis(rdb)
	server := NewServerFromRedis(rdb, ServerConfig{
		Concurrency:  1,
		PollInterval: 10 * time.Millisecond,
	})

	return server, client, mr
}

// TestOnStartHook verifies the OnStart hook fires before the handler runs.
func TestOnStartHook(t *testing.T) {
	server, client, _ := setupHookServer(t)

	var started atomic.Int32
	server.OnStart(func(_ context.Context, ev JobEvent) {
		started.Add(1)
		assert.Equal(t, "hook.test", ev.Kind)
		assert.Equal(t, "default", ev.Queue)
	})

	HandleFunc(server, func(_ context.Context, _ *Job[hookTask]) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := Enqueue(client, ctx, hookTask{Value: "a"})
	require.NoError(t, err)

	go server.Start()
	t.Cleanup(func() { cancel(); server.Stop() })

	require.Eventually(t, func() bool { return started.Load() == 1 }, 2*time.Second, 20*time.Millisecond)
}

// TestOnCompleteHook verifies the OnComplete hook fires after successful processing.
func TestOnCompleteHook(t *testing.T) {
	server, client, _ := setupHookServer(t)

	var completed atomic.Int32
	server.OnComplete(func(_ context.Context, ev JobEvent) {
		completed.Add(1)
		assert.Nil(t, ev.Err)
	})

	HandleFunc(server, func(_ context.Context, _ *Job[hookTask]) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := Enqueue(client, ctx, hookTask{Value: "b"})
	require.NoError(t, err)

	go server.Start()
	t.Cleanup(func() { cancel(); server.Stop() })

	require.Eventually(t, func() bool { return completed.Load() == 1 }, 2*time.Second, 20*time.Millisecond)
}

// TestOnErrorHook verifies the OnError hook fires when a job fails but will be retried.
func TestOnErrorHook(t *testing.T) {
	server, client, _ := setupHookServer(t)

	var errored atomic.Int32
	var mu sync.Mutex
	var capturedErr error

	server.OnError(func(_ context.Context, ev JobEvent) {
		mu.Lock()
		capturedErr = ev.Err
		mu.Unlock()
		errored.Add(1)
	})

	HandleFunc(server, func(_ context.Context, _ *Job[hookTask]) error {
		return fmt.Errorf("temporary failure")
	})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := Enqueue(client, ctx, hookTask{Value: "c"})
	require.NoError(t, err)

	go server.Start()
	t.Cleanup(func() { cancel(); server.Stop() })

	require.Eventually(t, func() bool { return errored.Load() >= 1 }, 2*time.Second, 20*time.Millisecond)

	mu.Lock()
	assert.Contains(t, capturedErr.Error(), "temporary failure")
	mu.Unlock()
}

// TestOnDeadHook verifies the OnDead hook fires when a job exhausts retries.
func TestOnDeadHook(t *testing.T) {
	server, client, _ := setupHookServer(t)

	var dead atomic.Int32
	server.OnDead(func(_ context.Context, ev JobEvent) {
		dead.Add(1)
		assert.NotNil(t, ev.Err)
	})

	HandleFunc(server, func(_ context.Context, _ *Job[hookTask]) error {
		return ErrSkipRetry
	})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := Enqueue(client, ctx, hookTask{Value: "d"})
	require.NoError(t, err)

	go server.Start()
	t.Cleanup(func() { cancel(); server.Stop() })

	require.Eventually(t, func() bool { return dead.Load() == 1 }, 2*time.Second, 20*time.Millisecond)
}

// TestMultipleHooksSameEvent verifies multiple hooks on the same event all fire.
func TestMultipleHooksSameEvent(t *testing.T) {
	server, client, _ := setupHookServer(t)

	var count atomic.Int32
	server.OnComplete(func(_ context.Context, _ JobEvent) { count.Add(1) })
	server.OnComplete(func(_ context.Context, _ JobEvent) { count.Add(1) })

	HandleFunc(server, func(_ context.Context, _ *Job[hookTask]) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := Enqueue(client, ctx, hookTask{Value: "e"})
	require.NoError(t, err)

	go server.Start()
	t.Cleanup(func() { cancel(); server.Stop() })

	require.Eventually(t, func() bool { return count.Load() == 2 }, 2*time.Second, 20*time.Millisecond)
}
