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

func TestDequeueEmpty(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

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

func TestUniqueJob(t *testing.T) {
	q, _ := setup(t)
	ctx := context.Background()

	err := q.Enqueue(ctx, makeJob("job-1", "test", "default", 5), "test:abc123", 60*time.Second)
	require.NoError(t, err)

	err = q.Enqueue(ctx, makeJob("job-2", "test", "default", 5), "test:abc123", 60*time.Second)
	require.ErrorIs(t, err, ErrDuplicate)
}

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
