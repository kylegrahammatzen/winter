package winter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupInspector(t *testing.T) (*Inspector, *queue.Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	insp := NewInspectorFromRedis(rdb)
	q := queue.New(rdb)
	return insp, q, mr
}

func enqueueTestJob(t *testing.T, q *queue.Queue, queueName, kind string, payload any) string {
	t.Helper()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	id := "job-" + kind + "-" + time.Now().Format("150405.000")
	rec := &queue.JobRecord{
		ID:         id,
		Kind:       kind,
		Queue:      queueName,
		Priority:   5,
		State:      "pending",
		Payload:    data,
		MaxRetries: 1,
		CreatedAt:  time.Now().UnixMilli(),
	}
	require.NoError(t, q.Enqueue(context.Background(), rec, "", 0))
	return id
}

func dequeueAndNack(t *testing.T, q *queue.Queue, queueName, workerID string) string {
	t.Helper()
	ctx := context.Background()
	rec, err := q.Dequeue(ctx, queueName, workerID)
	require.NoError(t, err)
	require.NotNil(t, rec)
	_, err = q.Nack(ctx, queueName, rec.ID, workerID, "test error", 0, true)
	require.NoError(t, err)
	return rec.ID
}

// TestInspectorQueue verifies that Queue returns accurate depth counts.
func TestInspectorQueue(t *testing.T) {
	insp, q, _ := setupInspector(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "default", "task.a", map[string]string{"k": "v"})
	enqueueTestJob(t, q, "default", "task.b", map[string]string{"k": "v"})

	info, err := insp.Queue(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, "default", info.Name)
	assert.Equal(t, int64(2), info.Ready)
	assert.Equal(t, int64(0), info.Active)
	assert.Equal(t, int64(0), info.Dead)
	assert.Equal(t, int64(2), info.Enqueued)
}

// TestInspectorDead verifies listing dead letter jobs with pagination.
func TestInspectorDead(t *testing.T) {
	insp, q, _ := setupInspector(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "default", "task.fail", map[string]string{"seq": "1"})
	enqueueTestJob(t, q, "default", "task.fail", map[string]string{"seq": "2"})

	dequeueAndNack(t, q, "default", "w1")
	dequeueAndNack(t, q, "default", "w1")

	jobs, err := insp.Dead(ctx, "default", 0, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 2)

	page, err := insp.Dead(ctx, "default", 0, 1)
	require.NoError(t, err)
	assert.Len(t, page, 1)
}

// TestInspectorPeekDead returns the first dead job without removing it.
func TestInspectorPeekDead(t *testing.T) {
	insp, q, _ := setupInspector(t)
	ctx := context.Background()

	empty, err := insp.PeekDead(ctx, "default")
	require.NoError(t, err)
	assert.Nil(t, empty)

	enqueueTestJob(t, q, "default", "task.peek", map[string]string{"v": "1"})
	dequeueAndNack(t, q, "default", "w1")

	job, err := insp.PeekDead(ctx, "default")
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, "task.peek", job.Kind)
}

// TestInspectorDeadCount returns the correct dead queue length.
func TestInspectorDeadCount(t *testing.T) {
	insp, q, _ := setupInspector(t)
	ctx := context.Background()

	count, err := insp.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	enqueueTestJob(t, q, "default", "task.count", nil)
	dequeueAndNack(t, q, "default", "w1")

	count, err = insp.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// TestInspectorRetry moves a dead job back to the ready set.
func TestInspectorRetry(t *testing.T) {
	insp, q, _ := setupInspector(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "default", "task.retry", nil)
	jobID := dequeueAndNack(t, q, "default", "w1")

	count, err := insp.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	require.NoError(t, insp.Retry(ctx, "default", jobID))

	count, err = insp.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	info, err := insp.Queue(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1), info.Ready)
}

// TestInspectorPurgeDead removes all dead jobs and returns the count.
func TestInspectorPurgeDead(t *testing.T) {
	insp, q, _ := setupInspector(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		enqueueTestJob(t, q, "default", "task.purge", nil)
		dequeueAndNack(t, q, "default", "w1")
	}

	purged, err := insp.PurgeDead(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(3), purged)

	count, err := insp.DeadCount(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

// TestInspectorPauseResume verifies that pausing prevents dequeue and
// resuming allows it again.
func TestInspectorPauseResume(t *testing.T) {
	insp, q, _ := setupInspector(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "default", "task.pause", nil)

	require.NoError(t, insp.Pause(ctx, "default"))

	rec, err := q.Dequeue(ctx, "default", "w1")
	require.NoError(t, err)
	assert.Nil(t, rec, "dequeue should return nil while paused")

	require.NoError(t, insp.Resume(ctx, "default"))

	rec, err = q.Dequeue(ctx, "default", "w1")
	require.NoError(t, err)
	assert.NotNil(t, rec, "dequeue should succeed after resume")
}

// TestInspectorClose verifies the inspector can be closed without error.
func TestInspectorClose(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	insp := NewInspectorFromRedis(rdb)
	require.NoError(t, insp.Close())
}
