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

var ErrDuplicate = errors.New("winter: duplicate job")

type Queue struct {
	rdb redis.UniversalClient
}

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
func workerJobsKey(id string) string { return "winter:worker:" + id + ":jobs" }
func uniqueKey(key string) string    { return "winter:unique:" + key }

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

func (q *Queue) Dequeue(ctx context.Context, queueName string, workerID string) (*JobRecord, error) {
	now := time.Now().UnixMilli()

	keys := []string{
		readyKey(queueName),
		activeKey(queueName),
		pausedKey(queueName),
		workerJobsKey(workerID),
	}

	result, err := dequeueScript.Run(ctx, q.rdb, keys,
		workerID,
		now,
	).Result()

	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("winter: dequeue: %w", err)
	}

	vals, ok := result.([]interface{})
	if !ok || len(vals) == 0 {
		return nil, nil
	}

	return parseJobRecord(vals)
}

func (q *Queue) Ack(ctx context.Context, queueName string, jobID string, workerID string) error {
	now := time.Now().UnixMilli()

	keys := []string{
		jobKey(jobID),
		activeKey(queueName),
		statsKey(queueName),
		workerJobsKey(workerID),
	}

	_, err := ackScript.Run(ctx, q.rdb, keys, jobID, now).Result()
	if err != nil {
		return fmt.Errorf("winter: ack: %w", err)
	}
	return nil
}

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
	return result.(string), nil
}

func (q *Queue) RescheduleJob(ctx context.Context, queueName string, jobID string, workerID string, newTime time.Time) error {
	keys := []string{
		jobKey(jobID),
		activeKey(queueName),
		delayedKey(queueName),
		workerJobsKey(workerID),
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

func (q *Queue) CancelJob(ctx context.Context, queueName string, jobID string, workerID string, reason string) error {
	now := time.Now().UnixMilli()

	keys := []string{
		jobKey(jobID),
		activeKey(queueName),
		workerJobsKey(workerID),
	}

	_, err := cancelScript.Run(ctx, q.rdb, keys, jobID, now, reason).Result()
	if err != nil {
		return fmt.Errorf("winter: cancel: %w", err)
	}
	return nil
}

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
	return result.(int64), nil
}

func (q *Queue) Pause(ctx context.Context, queueName string) error {
	return q.rdb.Set(ctx, pausedKey(queueName), "1", 0).Err()
}

func (q *Queue) Resume(ctx context.Context, queueName string) error {
	return q.rdb.Del(ctx, pausedKey(queueName)).Err()
}

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

func parseJobRecord(vals []interface{}) (*JobRecord, error) {
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
	}, nil
}
