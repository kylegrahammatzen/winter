package workflow

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// CreateChord enqueues all header tasks for parallel execution and stores a
// callback task that is enqueued only after every header task completes.
func (m *Manager) CreateChord(ctx context.Context, headers []TaskSpec, callback TaskSpec, queueName string) (string, error) {
	if len(headers) == 0 {
		return "", fmt.Errorf("winter: chord requires at least one header task")
	}

	id := uuid.New().String()

	for i := range headers {
		if headers[i].Queue == "" {
			headers[i].Queue = queueName
		}
	}
	if callback.Queue == "" {
		callback.Queue = queueName
	}

	rec := &Record{
		ID:       id,
		Type:     TypeChord,
		State:    "running",
		Tasks:    headers,
		Callback: &callback,
		Total:    len(headers),
		Done:     0,
		Queue:    queueName,
	}

	if err := m.saveRecord(ctx, rec); err != nil {
		return "", err
	}

	for i, spec := range headers {
		jobID, err := m.enqueueTask(ctx, spec, id)
		if err != nil {
			return "", fmt.Errorf("winter: chord enqueue header %d: %w", i, err)
		}
		// Track each header job in the deps set.
		if err := m.rdb.SAdd(ctx, depsKey(id), jobID).Err(); err != nil {
			return "", fmt.Errorf("winter: chord add dep: %w", err)
		}
	}

	return id, nil
}

// advanceChordScript atomically removes the completed job from the deps set
// and returns the remaining count. This prevents two concurrent completions
// from both seeing zero and double-firing the callback.
var advanceChordScript = redis.NewScript(`
local depsKey = KEYS[1]
local jobID = ARGV[1]

redis.call("SREM", depsKey, jobID)
return redis.call("SCARD", depsKey)
`)

func (m *Manager) advanceChord(ctx context.Context, rec *Record, jobID string) error {
	remaining, err := advanceChordScript.Run(ctx, m.rdb, []string{depsKey(rec.ID)}, jobID).Result()
	if err != nil {
		return fmt.Errorf("winter: chord advance: %w", err)
	}

	rec.Done++

	if remaining.(int64) == 0 {
		rec.State = "completed"
		if err := m.saveRecord(ctx, rec); err != nil {
			return err
		}

		// All headers done, fire the callback.
		if rec.Callback != nil {
			if _, err := m.enqueueTask(ctx, *rec.Callback, rec.ID); err != nil {
				return fmt.Errorf("winter: chord enqueue callback: %w", err)
			}
		}
		return nil
	}

	return m.saveRecord(ctx, rec)
}
