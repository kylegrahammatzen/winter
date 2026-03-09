package worker

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func setupQueue(t *testing.T) *queue.Queue {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return queue.New(rdb)
}

func enqueueTestJob(t *testing.T, q *queue.Queue, id string) {
	t.Helper()
	ctx := context.Background()
	err := q.Enqueue(ctx, &queue.JobRecord{
		ID:         id,
		Kind:       "test.job",
		Queue:      "default",
		Priority:   5,
		State:      "pending",
		Payload:    []byte(`{"x":1}`),
		MaxRetries: 3,
		CreatedAt:  time.Now().UnixMilli(),
	}, "", 0)
	require.NoError(t, err)
}

// Dequeues 3 jobs, acks one, and verifies the 2 with expired leases are
// recovered back to the ready set.
func TestRecoverExpiredLeases(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")
	enqueueTestJob(t, q, "job-2")
	enqueueTestJob(t, q, "job-3")

	rec1, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec1)

	rec2, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec2)

	rec3, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec3)

	err = q.Ack(ctx, "default", rec3.ID, "worker-1")
	require.NoError(t, err)

	// Lease duration is 30s, so 31s in the future guarantees expiry.
	futureMs := time.Now().Add(31 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Len(t, recovered, 2)

	rec, err := q.Dequeue(ctx, "default", "worker-2")
	require.NoError(t, err)
	require.NotNil(t, rec)

	rec, err = q.Dequeue(ctx, "default", "worker-2")
	require.NoError(t, err)
	require.NotNil(t, rec)

	rec, err = q.Dequeue(ctx, "default", "worker-2")
	require.NoError(t, err)
	require.Nil(t, rec)
}

// Extends a lease to 60s and verifies the job is not recovered at 35s but is at 65s.
func TestLeaseExtensionPreventsRecovery(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	err = q.ExtendLease(ctx, "default", rec.ID, 60*time.Second)
	require.NoError(t, err)

	at35s := time.Now().Add(35 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, at35s)
	require.NoError(t, err)
	require.Empty(t, recovered)

	at65s := time.Now().Add(65 * time.Second).UnixMilli()
	recovered, err = q.RecoverExpiredLeasesAt(ctx, "default", 100, at65s)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	require.Equal(t, rec.ID, recovered[0])
}

// Runs recovery twice at the same future time and verifies the job is only
// recovered once thanks to the SREM guard in the Lua script.
func TestRecoveryIdempotent(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	futureMs := time.Now().Add(31 * time.Second).UnixMilli()

	recovered1, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Len(t, recovered1, 1)

	recovered2, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Empty(t, recovered2)
}

// Verifies that a recovered job's state is reset to pending in the hash.
func TestRecoveredJobStateIsPending(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.Equal(t, "active", rec.State)

	futureMs := time.Now().Add(31 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Len(t, recovered, 1)

	job, err := q.GetJob(ctx, "job-1")
	require.NoError(t, err)
	require.Equal(t, "pending", job.State)
}

// Verifies that acking a job removes its lease entry so it cannot be recovered.
func TestAckCleansLease(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	err = q.Ack(ctx, "default", rec.ID, "worker-1")
	require.NoError(t, err)

	futureMs := time.Now().Add(60 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Empty(t, recovered)
}

// Verifies that nacking a job removes its lease entry so it cannot be recovered.
func TestNackCleansLease(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	_, err = q.Nack(ctx, "default", rec.ID, "worker-1", "test error", 1000, false)
	require.NoError(t, err)

	futureMs := time.Now().Add(60 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Empty(t, recovered)
}

// Verifies that rescheduling a job removes its lease entry.
func TestRescheduleCleansLease(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	err = q.RescheduleJob(ctx, "default", rec.ID, "worker-1", time.Now().Add(5*time.Minute))
	require.NoError(t, err)

	futureMs := time.Now().Add(60 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Empty(t, recovered)
}

// Verifies that cancelling a job removes its lease entry.
func TestCancelCleansLease(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	err = q.CancelJob(ctx, "default", rec.ID, "worker-1", "test cancel")
	require.NoError(t, err)

	futureMs := time.Now().Add(60 * time.Second).UnixMilli()
	recovered, err := q.RecoverExpiredLeasesAt(ctx, "default", 100, futureMs)
	require.NoError(t, err)
	require.Empty(t, recovered)
}

// Verifies that consecutive heartbeats succeed without error.
func TestHeartbeat(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	err := q.Heartbeat(ctx, "worker-1")
	require.NoError(t, err)

	err = q.Heartbeat(ctx, "worker-1")
	require.NoError(t, err)
}

// Verifies that QueueStats accurately reflects ready, active, and completed counts.
func TestQueueStats(t *testing.T) {
	q := setupQueue(t)
	ctx := context.Background()

	enqueueTestJob(t, q, "job-1")
	enqueueTestJob(t, q, "job-2")
	enqueueTestJob(t, q, "job-3")

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, int64(3), stats["ready"])
	require.Equal(t, int64(0), stats["active"])
	require.Equal(t, int64(3), stats["enqueued"])

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	stats, err = q.QueueStats(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, int64(2), stats["ready"])
	require.Equal(t, int64(1), stats["active"])

	err = q.Ack(ctx, "default", rec.ID, "worker-1")
	require.NoError(t, err)

	stats, err = q.QueueStats(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, int64(2), stats["ready"])
	require.Equal(t, int64(0), stats["active"])
	require.Equal(t, int64(1), stats["completed"])
}
