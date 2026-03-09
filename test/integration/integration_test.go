//go:build integration

// Package integration runs end-to-end tests against a real Redis instance
// started via testcontainers. These tests validate that the full stack works
// correctly with real Redis, not miniredis.
package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/kylegrahammatzen/winter/internal/workflow"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func setupRedis(t *testing.T) (redis.UniversalClient, *queue.Queue) {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { container.Terminate(context.Background()) })

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)

	rdb := redis.NewClient(&redis.Options{Addr: endpoint})
	t.Cleanup(func() { rdb.Close() })

	require.NoError(t, rdb.Ping(ctx).Err())

	return rdb, queue.New(rdb)
}

// Enqueues 100 jobs and processes them with 3 concurrent workers, verifying
// all are completed with no duplicates and no lost jobs.
func TestFullEndToEnd(t *testing.T) {
	rdb, q := setupRedis(t)
	_ = rdb
	ctx := context.Background()

	for i := range 100 {
		job := &queue.JobRecord{
			ID:         fmt.Sprintf("job-%d", i),
			Kind:       "test.job",
			Queue:      "default",
			Priority:   i % 10,
			State:      "pending",
			Payload:    []byte(fmt.Sprintf(`{"i":%d}`, i)),
			MaxRetries: 3,
			CreatedAt:  time.Now().UnixMilli(),
		}
		err := q.Enqueue(ctx, job, "", 0)
		require.NoError(t, err)
	}

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(100), stats["ready"])

	var processed atomic.Int64
	seen := sync.Map{}

	var wg sync.WaitGroup
	for w := range 3 {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				rec, err := q.Dequeue(ctx, "default", workerID)
				if err != nil {
					return
				}
				if rec == nil {
					return
				}

				if _, loaded := seen.LoadOrStore(rec.ID, true); loaded {
					t.Errorf("duplicate dequeue: %s", rec.ID)
					return
				}

				err = q.Ack(ctx, "default", rec.ID, workerID)
				if err != nil {
					t.Errorf("ack error: %v", err)
					return
				}
				processed.Add(1)
			}
		}(fmt.Sprintf("worker-%d", w))
	}

	wg.Wait()
	assert.Equal(t, int64(100), processed.Load())

	stats, err = q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats["ready"])
	assert.Equal(t, int64(0), stats["active"])
	assert.Equal(t, int64(100), stats["completed"])
}

// Dequeues jobs without acking them, waits for lease expiry, and verifies
// the recovery mechanism moves them back to ready.
func TestLeaseRecovery(t *testing.T) {
	_, q := setupRedis(t)
	ctx := context.Background()

	for i := range 5 {
		job := &queue.JobRecord{
			ID:         fmt.Sprintf("lease-%d", i),
			Kind:       "test.job",
			Queue:      "default",
			Priority:   5,
			State:      "pending",
			Payload:    []byte(`{}`),
			MaxRetries: 3,
			CreatedAt:  time.Now().UnixMilli(),
		}
		require.NoError(t, q.Enqueue(ctx, job, "", 0))
	}

	// Dequeue all 5 without acking.
	for range 5 {
		rec, err := q.Dequeue(ctx, "default", "stale-worker")
		require.NoError(t, err)
		require.NotNil(t, rec)
	}

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats["ready"])
	assert.Equal(t, int64(5), stats["active"])

	// Simulate lease expiry by using a timestamp 31s in the future.
	futureMs := time.Now().Add(31 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	assert.Len(t, recovered, 5)

	stats, err = q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(5), stats["ready"])
	assert.Equal(t, int64(0), stats["active"])
}

// Exhausts retries to send a job to dead, then retries it from the DLQ and
// verifies it becomes dequeueable again.
func TestDeadLetterQueueRetry(t *testing.T) {
	_, q := setupRedis(t)
	ctx := context.Background()

	job := &queue.JobRecord{
		ID:         "dlq-job",
		Kind:       "test.job",
		Queue:      "default",
		Priority:   5,
		State:      "pending",
		Payload:    []byte(`{}`),
		MaxRetries: 1,
		CreatedAt:  time.Now().UnixMilli(),
	}
	require.NoError(t, q.Enqueue(ctx, job, "", 0))

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	result, err := q.Nack(ctx, "default", rec.ID, "worker-1", "fatal error", 0, true)
	require.NoError(t, err)
	assert.Equal(t, "dead", result)

	count, err := q.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	err = q.RetryDead(ctx, "default", "dlq-job")
	require.NoError(t, err)

	rec, err = q.Dequeue(ctx, "default", "worker-2")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "dlq-job", rec.ID)
	assert.Equal(t, 0, rec.Attempt)
}

// Runs a chain of 3 tasks end-to-end, verifying sequential execution and
// final workflow completion state.
func TestWorkflowChain(t *testing.T) {
	rdb, q := setupRedis(t)
	ctx := context.Background()

	mgr := workflow.NewManager(q, rdb)

	wfID, err := mgr.CreateChain(ctx, []workflow.TaskSpec{
		{Kind: "step.one", Payload: []byte(`{}`), Queue: "default"},
		{Kind: "step.two", Payload: []byte(`{}`), Queue: "default"},
		{Kind: "step.three", Payload: []byte(`{}`), Queue: "default"},
	}, "default")
	require.NoError(t, err)

	expectedKinds := []string{"step.one", "step.two", "step.three"}

	for _, expectedKind := range expectedKinds {
		rec, err := q.Dequeue(ctx, "default", "worker-1")
		require.NoError(t, err)
		require.NotNil(t, rec, "expected job with kind %s", expectedKind)
		assert.Equal(t, expectedKind, rec.Kind)
		assert.Equal(t, wfID, rec.WorkflowID)

		err = q.Ack(ctx, "default", rec.ID, "worker-1")
		require.NoError(t, err)
		err = mgr.OnJobCompleted(ctx, wfID, rec.ID)
		require.NoError(t, err)
	}

	// No more jobs.
	empty, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, empty)
}

// Runs a chord with 3 headers and a callback, verifying the callback only
// fires after all headers complete.
func TestWorkflowChord(t *testing.T) {
	rdb, q := setupRedis(t)
	ctx := context.Background()

	mgr := workflow.NewManager(q, rdb)

	wfID, err := mgr.CreateChord(ctx, []workflow.TaskSpec{
		{Kind: "build.linux", Payload: []byte(`{}`), Queue: "default"},
		{Kind: "build.darwin", Payload: []byte(`{}`), Queue: "default"},
		{Kind: "build.windows", Payload: []byte(`{}`), Queue: "default"},
	}, workflow.TaskSpec{Kind: "deploy", Payload: []byte(`{}`), Queue: "default"}, "default")
	require.NoError(t, err)

	// Complete all 3 headers.
	for range 3 {
		rec, err := q.Dequeue(ctx, "default", "worker-1")
		require.NoError(t, err)
		require.NotNil(t, rec)

		err = q.Ack(ctx, "default", rec.ID, "worker-1")
		require.NoError(t, err)
		err = mgr.OnJobCompleted(ctx, wfID, rec.ID)
		require.NoError(t, err)
	}

	// Callback should now be enqueued.
	callback, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, callback)
	assert.Equal(t, "deploy", callback.Kind)
}

// Batch enqueues 1000 jobs and verifies all are present in the queue.
func TestBatchEnqueue(t *testing.T) {
	_, q := setupRedis(t)
	ctx := context.Background()

	jobs := make([]*queue.JobRecord, 1000)
	for i := range jobs {
		jobs[i] = &queue.JobRecord{
			ID:         fmt.Sprintf("batch-%d", i),
			Kind:       "batch.task",
			Queue:      "default",
			Priority:   5,
			State:      "pending",
			Payload:    []byte(`{}`),
			MaxRetries: 3,
			CreatedAt:  time.Now().UnixMilli(),
		}
	}

	err := q.EnqueueMany(ctx, jobs)
	require.NoError(t, err)

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1000), stats["ready"])
}

// Simulates a worker dying mid-processing: worker-1 dequeues 5 jobs and
// never acks them, then recovery moves them back to ready and worker-2
// processes all of them successfully.
func TestWorkerDeathRecovery(t *testing.T) {
	_, q := setupRedis(t)
	ctx := context.Background()

	for i := range 5 {
		job := &queue.JobRecord{
			ID:         fmt.Sprintf("death-%d", i),
			Kind:       "test.job",
			Queue:      "default",
			Priority:   5,
			State:      "pending",
			Payload:    []byte(fmt.Sprintf(`{"i":%d}`, i)),
			MaxRetries: 3,
			CreatedAt:  time.Now().UnixMilli(),
		}
		require.NoError(t, q.Enqueue(ctx, job, "", 0))
	}

	// Worker-1 dequeues all 5 but "dies" without acking.
	for range 5 {
		rec, err := q.Dequeue(ctx, "default", "doomed-worker")
		require.NoError(t, err)
		require.NotNil(t, rec)
	}

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(5), stats["active"])
	assert.Equal(t, int64(0), stats["ready"])

	// Simulate lease expiry.
	futureMs := time.Now().Add(31 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	assert.Len(t, recovered, 5)

	// Worker-2 picks up all recovered jobs and processes them.
	var processed int
	for {
		rec, err := q.Dequeue(ctx, "default", "healthy-worker")
		require.NoError(t, err)
		if rec == nil {
			break
		}
		err = q.Ack(ctx, "default", rec.ID, "healthy-worker")
		require.NoError(t, err)
		processed++
	}

	assert.Equal(t, 5, processed)

	stats, err = q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats["ready"])
	assert.Equal(t, int64(0), stats["active"])
	assert.Equal(t, int64(5), stats["completed"])
}

// Verifies unique job deduplication against real Redis.
func TestUniqueJobDedup(t *testing.T) {
	_, q := setupRedis(t)
	ctx := context.Background()

	job1 := &queue.JobRecord{
		ID:         "uniq-1",
		Kind:       "test.job",
		Queue:      "default",
		Priority:   5,
		State:      "pending",
		Payload:    []byte(`{"key":"value"}`),
		MaxRetries: 3,
		CreatedAt:  time.Now().UnixMilli(),
	}
	err := q.Enqueue(ctx, job1, "test:uniq", 60*time.Second)
	require.NoError(t, err)

	job2 := &queue.JobRecord{
		ID:         "uniq-2",
		Kind:       "test.job",
		Queue:      "default",
		Priority:   5,
		State:      "pending",
		Payload:    []byte(`{"key":"value"}`),
		MaxRetries: 3,
		CreatedAt:  time.Now().UnixMilli(),
	}
	err = q.Enqueue(ctx, job2, "test:uniq", 60*time.Second)
	require.ErrorIs(t, err, queue.ErrDuplicate)
}
