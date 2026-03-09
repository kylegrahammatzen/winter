package workflow

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// CreateGroup enqueues all tasks immediately for parallel execution and tracks
// completion. The group is complete when all tasks have been acked.
func (m *Manager) CreateGroup(ctx context.Context, tasks []TaskSpec, queueName string) (string, error) {
	if len(tasks) == 0 {
		return "", fmt.Errorf("winter: group requires at least one task")
	}

	id := uuid.New().String()

	for i := range tasks {
		if tasks[i].Queue == "" {
			tasks[i].Queue = queueName
		}
	}

	rec := &Record{
		ID:    id,
		Type:  TypeGroup,
		State: "running",
		Tasks: tasks,
		Total: len(tasks),
		Done:  0,
		Queue: queueName,
	}

	if err := m.saveRecord(ctx, rec); err != nil {
		return "", err
	}

	for i, spec := range tasks {
		if _, err := m.enqueueTask(ctx, spec, id); err != nil {
			return "", fmt.Errorf("winter: group enqueue task %d: %w", i, err)
		}
	}

	return id, nil
}

// advanceGroupScript atomically increments the group counter and returns the
// new count. This prevents two concurrent completions from losing an increment.
var advanceGroupScript = redis.NewScript(`
local counterKey = KEYS[1]
return redis.call("INCR", counterKey)
`)

func (m *Manager) advanceGroup(ctx context.Context, rec *Record) error {
	done, err := advanceGroupScript.Run(ctx, m.rdb, []string{counterKey(rec.ID)}).Int64()
	if err != nil {
		return fmt.Errorf("winter: group advance: %w", err)
	}

	rec.Done = int(done)
	if rec.Done >= rec.Total {
		rec.State = "completed"
	}

	return m.saveRecord(ctx, rec)
}
