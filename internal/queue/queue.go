// Package queue implements the core Redis operations for Winter's job queue.
// All multi-step operations are atomic Lua scripts executed in a single
// round trip.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrDuplicate is returned when a unique job constraint prevents enqueue.
var ErrDuplicate = errors.New("winter: duplicate job")

// Queue wraps a Redis client and exposes atomic job operations backed by Lua scripts.
type Queue struct {
	rdb redis.UniversalClient
}

// New creates a Queue from an existing Redis connection.
func New(rdb redis.UniversalClient) *Queue {
	return &Queue{rdb: rdb}
}

func readyKey(queue string) string   { return "winter:" + queue + ":ready" }
func delayedKey(queue string) string { return "winter:" + queue + ":delayed" }
func activeKey(queue string) string  { return "winter:" + queue + ":active" }
func deadKey(queue string) string    { return "winter:" + queue + ":dead" }
func pausedKey(queue string) string  { return "winter:" + queue + ":paused" }
func statsKey(queue string) string   { return "winter:stats:" + queue }
func jobKey(id string) string        { return "winter:job:" + id }
func leaseKey(queue string) string   { return "winter:" + queue + ":lease" }
func workerJobsKey(id string) string { return "winter:worker:" + id + ":jobs" }
func uniqueKey(key string) string    { return "winter:unique:" + key }
func workersKey() string             { return "winter:workers" }

// Enqueue atomically inserts a job into Redis, placing it in the ready or
// delayed set depending on whether a scheduled time is set.
func (q *Queue) Enqueue(ctx context.Context, job *JobRecord, uniqKey string, uniquePeriod time.Duration) error {
	keys := []string{
		jobKey(job.ID),
		readyKey(job.Queue),
		delayedKey(job.Queue),
		statsKey(job.Queue),
		"",
	}

	uniqueTTL := 0
	if uniqKey != "" && uniquePeriod > 0 {
		keys[4] = uniqueKey(uniqKey)
		uniqueTTL = int(uniquePeriod.Seconds())
	}

	result, err := enqueueScript.Run(ctx, q.rdb, keys,
		job.ID,
		job.Kind,
		job.Queue,
		job.Priority,
		job.State,
		job.Payload,
		job.MaxRetries,
		job.CreatedAt,
		job.ScheduledAt,
		uniqueTTL,
		job.WorkflowID,
	).Result()

	if err != nil {
		if strings.Contains(err.Error(), "DUPLICATE") {
			return ErrDuplicate
		}
		return fmt.Errorf("winter: enqueue: %w", err)
	}

	_ = result
	return nil
}

// DefaultLeaseDuration is how long a worker has to complete or extend a job
// before the lease expires and the job becomes eligible for recovery.
const DefaultLeaseDuration = 30 * time.Second

// EnqueueMany inserts multiple jobs using Redis pipelining for throughput. Jobs
// are chunked into batches of 100 to keep per-pipeline blocking time reasonable.
func (q *Queue) EnqueueMany(ctx context.Context, jobs []*JobRecord) error {
	const chunkSize = 100

	for i := 0; i < len(jobs); i += chunkSize {
		end := i + chunkSize
		if end > len(jobs) {
			end = len(jobs)
		}
		if err := q.enqueueBatch(ctx, jobs[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) enqueueBatch(ctx context.Context, jobs []*JobRecord) error {
	// Pre-load the script SHA so pipelined EVALSHA calls succeed.
	if err := enqueueScript.Load(ctx, q.rdb).Err(); err != nil {
		return fmt.Errorf("winter: script load: %w", err)
	}

	pipe := q.rdb.Pipeline()

	for _, job := range jobs {
		keys := []string{
			jobKey(job.ID),
			readyKey(job.Queue),
			delayedKey(job.Queue),
			statsKey(job.Queue),
			"",
		}

		enqueueScript.Run(ctx, pipe, keys,
			job.ID,
			job.Kind,
			job.Queue,
			job.Priority,
			job.State,
			job.Payload,
			job.MaxRetries,
			job.CreatedAt,
			job.ScheduledAt,
			0,
			job.WorkflowID,
		)
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		for _, cmd := range cmds {
			if cmd.Err() != nil && strings.Contains(cmd.Err().Error(), "DUPLICATE") {
				continue
			}
			if cmd.Err() != nil {
				return fmt.Errorf("winter: batch enqueue: %w", cmd.Err())
			}
		}
	}
	return nil
}

// Dequeue atomically pops the highest-priority job from the ready set, marks it
// active, assigns it to the worker, and sets a lease expiry in the lease ZSET.
func (q *Queue) Dequeue(ctx context.Context, queueName string, workerID string) (*JobRecord, error) {
	now := time.Now().UnixMilli()

	keys := []string{
		readyKey(queueName),
		activeKey(queueName),
		pausedKey(queueName),
		workerJobsKey(workerID),
		leaseKey(queueName),
	}

	result, err := dequeueScript.Run(ctx, q.rdb, keys,
		workerID,
		now,
		DefaultLeaseDuration.Milliseconds(),
	).Result()

	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("winter: dequeue: %w", err)
	}

	vals, ok := result.([]any)
	if !ok || len(vals) == 0 {
		return nil, nil
	}

	return parseJobRecord(vals)
}

// Ack marks a job as completed and removes it from the active set and lease ZSET.
func (q *Queue) Ack(ctx context.Context, queueName string, jobID string, workerID string) error {
	now := time.Now().UnixMilli()

	keys := []string{
		jobKey(jobID),
		activeKey(queueName),
		statsKey(queueName),
		workerJobsKey(workerID),
		leaseKey(queueName),
	}

	_, err := ackScript.Run(ctx, q.rdb, keys, jobID, now).Result()
	if err != nil {
		return fmt.Errorf("winter: ack: %w", err)
	}
	return nil
}

// Nack records a job failure. If retries remain the job moves to the delayed set
// with a backoff delay; otherwise it goes to the dead letter queue.
func (q *Queue) Nack(ctx context.Context, queueName string, jobID string, workerID string, errMsg string, backoffMs int64, skipRetry bool) (string, error) {
	now := time.Now().UnixMilli()

	skip := 0
	if skipRetry {
		skip = 1
	}

	keys := []string{
		jobKey(jobID),
		activeKey(queueName),
		delayedKey(queueName),
		deadKey(queueName),
		statsKey(queueName),
		workerJobsKey(workerID),
		leaseKey(queueName),
	}

	result, err := nackScript.Run(ctx, q.rdb, keys,
		jobID,
		errMsg,
		backoffMs,
		now,
		skip,
	).Result()

	if err != nil {
		return "", fmt.Errorf("winter: nack: %w", err)
	}
	s, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("winter: nack: unexpected result type %T", result)
	}
	return s, nil
}

// RescheduleJob moves an active job back into the delayed set with a new scheduled time.
func (q *Queue) RescheduleJob(ctx context.Context, queueName string, jobID string, workerID string, newTime time.Time) error {
	keys := []string{
		jobKey(jobID),
		activeKey(queueName),
		delayedKey(queueName),
		workerJobsKey(workerID),
		leaseKey(queueName),
	}

	_, err := rescheduleScript.Run(ctx, q.rdb, keys,
		jobID,
		newTime.UnixMilli(),
	).Result()

	if err != nil {
		return fmt.Errorf("winter: reschedule: %w", err)
	}
	return nil
}

// CancelJob permanently removes a job from active processing without retrying.
func (q *Queue) CancelJob(ctx context.Context, queueName string, jobID string, workerID string, reason string) error {
	now := time.Now().UnixMilli()

	keys := []string{
		jobKey(jobID),
		activeKey(queueName),
		workerJobsKey(workerID),
		leaseKey(queueName),
	}

	_, err := cancelScript.Run(ctx, q.rdb, keys, jobID, now, reason).Result()
	if err != nil {
		return fmt.Errorf("winter: cancel: %w", err)
	}
	return nil
}

// Promote moves up to limit delayed jobs whose scheduled time has passed into the ready set.
func (q *Queue) Promote(ctx context.Context, queueName string, limit int) (int64, error) {
	now := time.Now().UnixMilli()

	keys := []string{
		delayedKey(queueName),
		readyKey(queueName),
	}

	result, err := promoteScript.Run(ctx, q.rdb, keys, now, limit).Result()
	if err != nil {
		return 0, fmt.Errorf("winter: promote: %w", err)
	}
	n, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("winter: promote: unexpected result type %T", result)
	}
	return n, nil
}

// ExtendLease pushes the lease expiry forward for a job that is still being processed.
func (q *Queue) ExtendLease(ctx context.Context, queueName string, jobID string, extension time.Duration) error {
	newExpiry := time.Now().Add(extension).UnixMilli()

	_, err := extendLeaseScript.Run(ctx, q.rdb, []string{leaseKey(queueName)}, jobID, newExpiry).Result()
	if err != nil {
		return fmt.Errorf("winter: extend lease: %w", err)
	}
	return nil
}

// RecoverExpiredLeases finds jobs whose lease has expired and moves them back to ready.
func (q *Queue) RecoverExpiredLeases(ctx context.Context, queueName string, limit int) ([]string, error) {
	return q.RecoverExpiredLeasesAt(ctx, queueName, limit, time.Now().UnixMilli())
}

// RecoverExpiredLeasesAt is like RecoverExpiredLeases but accepts an explicit
// timestamp, which is useful for testing without real clock advancement.
func (q *Queue) RecoverExpiredLeasesAt(ctx context.Context, queueName string, limit int, nowMs int64) ([]string, error) {
	keys := []string{
		leaseKey(queueName),
		activeKey(queueName),
		readyKey(queueName),
	}

	result, err := recoverLeasesScript.Run(ctx, q.rdb, keys, nowMs, limit).Result()
	if err != nil {
		return nil, fmt.Errorf("winter: recover leases: %w", err)
	}

	vals, ok := result.([]any)
	if !ok {
		return nil, nil
	}

	ids := make([]string, 0, len(vals))
	for _, v := range vals {
		if id, ok := v.(string); ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// Heartbeat updates the worker's last-seen timestamp in the workers hash.
func (q *Queue) Heartbeat(ctx context.Context, workerID string) error {
	now := time.Now().UnixMilli()
	return q.rdb.HSet(ctx, workersKey(), workerID, now).Err()
}

// DeregisterWorker removes the worker from the registry and deletes its job set.
func (q *Queue) DeregisterWorker(ctx context.Context, workerID string) error {
	pipe := q.rdb.Pipeline()
	pipe.HDel(ctx, workersKey(), workerID)
	pipe.Del(ctx, workerJobsKey(workerID))
	_, err := pipe.Exec(ctx)
	return err
}

// QueueStats returns a snapshot of queue depths and cumulative counters.
func (q *Queue) QueueStats(ctx context.Context, queueName string) (map[string]int64, error) {
	pipe := q.rdb.Pipeline()
	readyCmd := pipe.ZCard(ctx, readyKey(queueName))
	activeCmd := pipe.SCard(ctx, activeKey(queueName))
	delayedCmd := pipe.ZCard(ctx, delayedKey(queueName))
	deadCmd := pipe.LLen(ctx, deadKey(queueName))
	statsCmd := pipe.HGetAll(ctx, statsKey(queueName))
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("winter: queue stats: %w", err)
	}

	stats := map[string]int64{
		"ready":   readyCmd.Val(),
		"active":  activeCmd.Val(),
		"delayed": delayedCmd.Val(),
		"dead":    deadCmd.Val(),
	}

	for k, v := range statsCmd.Val() {
		n, _ := strconv.ParseInt(v, 10, 64)
		stats[k] = n
	}

	return stats, nil
}

// Pause sets a flag that causes Dequeue to return nil for the given queue.
func (q *Queue) Pause(ctx context.Context, queueName string) error {
	return q.rdb.Set(ctx, pausedKey(queueName), "1", 0).Err()
}

// Resume removes the pause flag so Dequeue resumes normal operation.
func (q *Queue) Resume(ctx context.Context, queueName string) error {
	return q.rdb.Del(ctx, pausedKey(queueName)).Err()
}

// GetJob fetches the full job hash from Redis by ID.
func (q *Queue) GetJob(ctx context.Context, jobID string) (*JobRecord, error) {
	vals, err := q.rdb.HGetAll(ctx, jobKey(jobID)).Result()
	if err != nil {
		return nil, fmt.Errorf("winter: get job: %w", err)
	}
	if len(vals) == 0 {
		return nil, nil
	}
	return parseJobRecordFromMap(vals)
}

// ListDead returns a paginated slice of jobs from the dead letter queue.
func (q *Queue) ListDead(ctx context.Context, queueName string, offset, limit int64) ([]*JobRecord, error) {
	ids, err := q.rdb.LRange(ctx, deadKey(queueName), offset, offset+limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("winter: list dead: %w", err)
	}

	records := make([]*JobRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := q.GetJob(ctx, id)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			records = append(records, rec)
		}
	}
	return records, nil
}

// PeekDead returns the first job in the dead letter queue without removing it.
func (q *Queue) PeekDead(ctx context.Context, queueName string) (*JobRecord, error) {
	ids, err := q.rdb.LRange(ctx, deadKey(queueName), 0, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("winter: peek dead: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return q.GetJob(ctx, ids[0])
}

// RetryDead removes a job from the dead list, resets its attempt counter, and
// re-enqueues it to the ready set.
func (q *Queue) RetryDead(ctx context.Context, queueName string, jobID string) error {
	keys := []string{
		jobKey(jobID),
		deadKey(queueName),
		readyKey(queueName),
		statsKey(queueName),
	}

	result, err := retryDeadScript.Run(ctx, q.rdb, keys, jobID).Result()
	if err != nil {
		return fmt.Errorf("winter: retry dead: %w", err)
	}
	n, ok := result.(int64)
	if !ok {
		return fmt.Errorf("winter: retry dead: unexpected result type %T", result)
	}
	if n == 0 {
		return fmt.Errorf("winter: job %s not found in dead queue", jobID)
	}
	return nil
}

// PurgeDead removes all jobs from the dead letter queue and deletes their hashes.
func (q *Queue) PurgeDead(ctx context.Context, queueName string) (int64, error) {
	result, err := purgeDeadScript.Run(ctx, q.rdb, []string{deadKey(queueName)}).Result()
	if err != nil {
		return 0, fmt.Errorf("winter: purge dead: %w", err)
	}
	n, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("winter: purge dead: unexpected result type %T", result)
	}
	return n, nil
}

// DeadCount returns the number of jobs in the dead letter queue.
func (q *Queue) DeadCount(ctx context.Context, queueName string) (int64, error) {
	return q.rdb.LLen(ctx, deadKey(queueName)).Result()
}

func resultKey(jobID string) string { return "winter:job:" + jobID + ":result" }

// SetResult stores a job result with a TTL.
func (q *Queue) SetResult(ctx context.Context, jobID string, data []byte, ttl time.Duration) error {
	return q.rdb.Set(ctx, resultKey(jobID), data, ttl).Err()
}

// GetResult retrieves a stored job result. Returns nil, nil if no result exists.
func (q *Queue) GetResult(ctx context.Context, jobID string) ([]byte, error) {
	data, err := q.rdb.Get(ctx, resultKey(jobID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("winter: get result: %w", err)
	}
	return data, nil
}

func parseJobRecord(vals []any) (*JobRecord, error) {
	m := make(map[string]string, len(vals)/2)
	for i := 0; i < len(vals)-1; i += 2 {
		key, _ := vals[i].(string)
		val, _ := vals[i+1].(string)
		m[key] = val
	}
	return parseJobRecordFromMap(m)
}

func parseJobRecordFromMap(m map[string]string) (*JobRecord, error) {
	priority, _ := strconv.Atoi(m["priority"])
	attempt, _ := strconv.Atoi(m["attempt"])
	maxRetries, _ := strconv.Atoi(m["max_retries"])
	createdAt, _ := strconv.ParseInt(m["created_at"], 10, 64)
	scheduledAt, _ := strconv.ParseInt(m["scheduled_at"], 10, 64)
	startedAt, _ := strconv.ParseInt(m["started_at"], 10, 64)
	completedAt, _ := strconv.ParseInt(m["completed_at"], 10, 64)

	return &JobRecord{
		ID:          m["id"],
		Kind:        m["kind"],
		Queue:       m["queue"],
		Priority:    priority,
		State:       m["state"],
		Payload:     []byte(m["payload"]),
		Attempt:     attempt,
		MaxRetries:  maxRetries,
		CreatedAt:   createdAt,
		ScheduledAt: scheduledAt,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		LastError:   m["last_error"],
		UniqueKey:   m["unique_key"],
		WorkflowID:  m["workflow_id"],
	}, nil
}
