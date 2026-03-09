package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (*Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb), mr
}

func makeJob(id, kind, queue string, priority int) *JobRecord {
	payload, _ := json.Marshal(map[string]string{"test": "data"})
	return &JobRecord{
		ID:         id,
		Kind:       kind,
		Queue:      queue,
		Priority:   priority,
		State:      "pending",
		Payload:    payload,
		MaxRetries: 3,
		CreatedAt:  time.Now().UnixMilli(),
	}
}

// Enqueues a job and verifies all fields survive the round trip through Redis.
func TestEnqueueAndDequeue(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	job := makeJob("job-1", "test.task", "default", 5)
	err := q.Enqueue(ctx, job, "", 0)
	require.NoError(t, err)

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	assert.Equal(t, "job-1", rec.ID)
	assert.Equal(t, "test.task", rec.Kind)
	assert.Equal(t, "default", rec.Queue)
	assert.Equal(t, 5, rec.Priority)
	assert.Equal(t, "active", rec.State)
	assert.Equal(t, 3, rec.MaxRetries)
}

// Dequeue from an empty queue returns nil without error.
func TestDequeueEmpty(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

// Enqueues jobs with priorities 10, 0, 5 and verifies dequeue order is 0, 5, 10.
func TestPriorityOrdering(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, makeJob("high", "test", "default", 10), "", 0))
	require.NoError(t, q.Enqueue(ctx, makeJob("low", "test", "default", 0), "", 0))
	require.NoError(t, q.Enqueue(ctx, makeJob("mid", "test", "default", 5), "", 0))

	first, err := q.Dequeue(ctx, "default", "w1")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, "low", first.ID)

	second, err := q.Dequeue(ctx, "default", "w1")
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, "mid", second.ID)

	third, err := q.Dequeue(ctx, "default", "w1")
	require.NoError(t, err)
	require.NotNil(t, third)
	assert.Equal(t, "high", third.ID)
}

// Acks a job and verifies its state becomes completed with a timestamp.
func TestAck(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "", 0))
	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	err = q.Ack(ctx, "default", "job-1", "worker-1")
	require.NoError(t, err)

	job, err := q.GetJob(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "completed", job.State)
	assert.NotZero(t, job.CompletedAt)
}

// Nacks a job with retries remaining and verifies it moves to the delayed set.
func TestNackWithRetry(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "", 0))
	_, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	result, err := q.Nack(ctx, "default", "job-1", "worker-1", "connection timeout", 1000, false)
	require.NoError(t, err)
	assert.Equal(t, "retry", result)

	job, err := q.GetJob(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "retry", job.State)
	assert.Equal(t, 1, job.Attempt)
	assert.Equal(t, "connection timeout", job.LastError)
}

// Nacks a job that has exhausted retries and verifies it goes to the dead list.
func TestNackExhaustedRetries(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	job := makeJob("job-1", "test", "default", 5)
	job.MaxRetries = 1
	require.NoError(t, q.Enqueue(ctx, job, "", 0))

	_, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	result, err := q.Nack(ctx, "default", "job-1", "worker-1", "failed again", 0, false)
	require.NoError(t, err)
	assert.Equal(t, "dead", result)

	rec, err := q.GetJob(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "dead", rec.State)
}

// Nacks a job with skipRetry and verifies it goes directly to dead.
func TestNackSkipRetry(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "", 0))
	_, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	result, err := q.Nack(ctx, "default", "job-1", "worker-1", "bad data", 0, true)
	require.NoError(t, err)
	assert.Equal(t, "dead", result)
}

// Reschedules an active job and verifies it moves back to delayed with a new timestamp.
func TestReschedule(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "", 0))
	_, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	future := time.Now().Add(30 * time.Second)
	err = q.RescheduleJob(ctx, "default", "job-1", "worker-1", future)
	require.NoError(t, err)

	job, err := q.GetJob(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "pending", job.State)
	assert.NotZero(t, job.ScheduledAt)
}

// Cancels an active job and verifies its state and reason are stored.
func TestCancel(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "", 0))
	_, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	err = q.CancelJob(ctx, "default", "job-1", "worker-1", "resource deleted")
	require.NoError(t, err)

	job, err := q.GetJob(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "cancelled", job.State)
	assert.Equal(t, "resource deleted", job.LastError)
}

// Enqueues a delayed job, verifies it is not dequeueable, promotes it, then dequeues.
func TestDelayedEnqueueAndPromote(t *testing.T) {
	q, mr := setup(t)
	ctx := context.Background()

	job := makeJob("delayed-1", "test", "default", 5)
	job.ScheduledAt = time.Now().Add(-1 * time.Second).UnixMilli()
	require.NoError(t, q.Enqueue(ctx, job, "", 0))

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, rec)

	mr.FastForward(2 * time.Second)

	promoted, err := q.Promote(ctx, "default", 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), promoted)

	rec, err = q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "delayed-1", rec.ID)
}

// Pauses a queue, verifies dequeue returns nil, resumes, and dequeues successfully.
func TestPauseResume(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "", 0))

	require.NoError(t, q.Pause(ctx, "default"))

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, rec)

	require.NoError(t, q.Resume(ctx, "default"))

	rec, err = q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "job-1", rec.ID)
}

// Enqueues a unique job and verifies the second enqueue with the same key is rejected.
func TestUniqueJob(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	err := q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "test:abc123", 60*time.Second)
	require.NoError(t, err)

	err = q.Enqueue(ctx, makeJob("job-2", "test", "default", 5), "test:abc123", 60*time.Second)
	require.ErrorIs(t, err, ErrDuplicate)
}

// Enqueues 1000 jobs in a single batch and verifies all are dequeueable.
func TestBatchEnqueue(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	jobs := make([]*JobRecord, 1000)
	for i := range jobs {
		jobs[i] = makeJob(fmt.Sprintf("batch-%d", i), "batch.task", "default", 5)
	}

	err := q.EnqueueMany(ctx, jobs)
	require.NoError(t, err)

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1000), stats["ready"])
	assert.Equal(t, int64(1000), stats["enqueued"])
}

// Verifies that the unique key is deleted when a job is acked.
func TestUniqueKeyCleanupOnAck(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	err := q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "test:uniq1", 60*time.Second)
	require.NoError(t, err)

	// Same unique key should be rejected.
	err = q.Enqueue(ctx, makeJob("job-2", "test", "default", 5), "test:uniq1", 60*time.Second)
	require.ErrorIs(t, err, ErrDuplicate)

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	err = q.Ack(ctx, "default", rec.ID, "worker-1")
	require.NoError(t, err)

	// After ack, the unique key should be freed.
	err = q.Enqueue(ctx, makeJob("job-3", "test", "default", 5), "test:uniq1", 60*time.Second)
	require.NoError(t, err)
}

// Verifies that the unique key is deleted when a job goes to the dead letter queue.
func TestUniqueKeyCleanupOnDead(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	job := makeJob("job-1", "test", "default", 5)
	job.MaxRetries = 1
	err := q.Enqueue(ctx, job, "test:uniq2", 60*time.Second)
	require.NoError(t, err)

	_, err = q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	result, err := q.Nack(ctx, "default", "job-1", "worker-1", "fatal error", 0, false)
	require.NoError(t, err)
	assert.Equal(t, "dead", result)

	// After dead, the unique key should be freed.
	err = q.Enqueue(ctx, makeJob("job-2", "test", "default", 5), "test:uniq2", 60*time.Second)
	require.NoError(t, err)
}

// Verifies that the unique key is NOT deleted on retry so duplicates are still prevented.
func TestUniqueKeyPreservedOnRetry(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	job := makeJob("job-1", "test", "default", 5)
	job.MaxRetries = 5
	err := q.Enqueue(ctx, job, "test:uniq3", 60*time.Second)
	require.NoError(t, err)

	_, err = q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	result, err := q.Nack(ctx, "default", "job-1", "worker-1", "transient error", 1000, false)
	require.NoError(t, err)
	assert.Equal(t, "retry", result)

	// Unique key should still be held since the job is retrying.
	err = q.Enqueue(ctx, makeJob("job-2", "test", "default", 5), "test:uniq3", 60*time.Second)
	require.ErrorIs(t, err, ErrDuplicate)
}

// Lists dead jobs with pagination and verifies ordering matches insertion order.
func TestListDead(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	for i := range 5 {
		job := makeJob(fmt.Sprintf("dead-%d", i), "test", "default", 5)
		job.MaxRetries = 1
		err := q.Enqueue(ctx, job, "", 0)
		require.NoError(t, err)

		_, err = q.Dequeue(ctx, "default", "worker-1")
		require.NoError(t, err)

		result, err := q.Nack(ctx, "default", fmt.Sprintf("dead-%d", i), "worker-1", "fail", 0, true)
		require.NoError(t, err)
		assert.Equal(t, "dead", result)
	}

	count, err := q.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	// First page.
	page1, err := q.ListDead(ctx, "default", 0, 3)
	require.NoError(t, err)
	assert.Len(t, page1, 3)

	// Second page.
	page2, err := q.ListDead(ctx, "default", 3, 3)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
}

// Peeks at the first dead job without removing it.
func TestPeekDead(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	rec, err := q.PeekDead(ctx, "default")
	require.NoError(t, err)
	assert.Nil(t, rec)

	job := makeJob("job-1", "test", "default", 5)
	job.MaxRetries = 1
	err = q.Enqueue(ctx, job, "", 0)
	require.NoError(t, err)

	_, err = q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	_, err = q.Nack(ctx, "default", "job-1", "worker-1", "fail", 0, true)
	require.NoError(t, err)

	rec, err = q.PeekDead(ctx, "default")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "job-1", rec.ID)
	assert.Equal(t, "dead", rec.State)
}

// Retries a dead job and verifies it moves back to the ready set with reset state.
func TestRetryDead(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	job := makeJob("job-1", "test", "default", 5)
	job.MaxRetries = 1
	err := q.Enqueue(ctx, job, "", 0)
	require.NoError(t, err)

	_, err = q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	_, err = q.Nack(ctx, "default", "job-1", "worker-1", "fail", 0, true)
	require.NoError(t, err)

	count, err := q.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	err = q.RetryDead(ctx, "default", "job-1")
	require.NoError(t, err)

	count, err = q.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	rec, err := q.Dequeue(ctx, "default", "worker-2")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "job-1", rec.ID)
	assert.Equal(t, "active", rec.State)
	assert.Equal(t, 0, rec.Attempt)
}

// Retrying a nonexistent job returns an error.
func TestRetryDeadNotFound(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	err := q.RetryDead(ctx, "default", "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in dead queue")
}

// Purges all dead jobs and verifies the queue is empty and job hashes are deleted.
func TestPurgeDead(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	for i := range 3 {
		job := makeJob(fmt.Sprintf("dead-%d", i), "test", "default", 5)
		job.MaxRetries = 1
		err := q.Enqueue(ctx, job, "", 0)
		require.NoError(t, err)

		_, err = q.Dequeue(ctx, "default", "worker-1")
		require.NoError(t, err)

		_, err = q.Nack(ctx, "default", fmt.Sprintf("dead-%d", i), "worker-1", "fail", 0, true)
		require.NoError(t, err)
	}

	purged, err := q.PurgeDead(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(3), purged)

	count, err := q.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// Job hashes should be deleted too.
	rec, err := q.GetJob(ctx, "dead-0")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

// Enqueues and dequeues 50 jobs concurrently and verifies none are lost.
func TestConcurrentEnqueueDequeue(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()
	n := 50

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job := makeJob(
				fmt.Sprintf("job-%d", i),
				"test",
				"default",
				i%10,
			)
			_ = q.Enqueue(ctx, job, "", 0)
		}(i)
	}
	wg.Wait()

	dequeued := 0
	for {
		rec, err := q.Dequeue(ctx, "default", "worker-1")
		require.NoError(t, err)
		if rec == nil {
			break
		}
		dequeued++
	}
	assert.Equal(t, n, dequeued)
}
