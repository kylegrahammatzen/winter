package workflow

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// CreateChain enqueues the first task and stores the remaining tasks as pending
// steps. Each subsequent task is enqueued only when the previous one completes.
func (m *Manager) CreateChain(ctx context.Context, tasks []TaskSpec, queueName string) (string, error) {
	if len(tasks) == 0 {
		return "", fmt.Errorf("winter: chain requires at least one task")
	}

	id := uuid.New().String()

	for i := range tasks {
		if tasks[i].Queue == "" {
			tasks[i].Queue = queueName
		}
	}

	rec := &Record{
		ID:      id,
		Type:    TypeChain,
		State:   "running",
		Tasks:   tasks,
		Current: 0,
		Total:   len(tasks),
		Queue:   queueName,
	}

	if err := m.saveRecord(ctx, rec); err != nil {
		return "", err
	}

	// Enqueue the first task.
	if _, err := m.enqueueTask(ctx, tasks[0], id); err != nil {
		return "", fmt.Errorf("winter: chain enqueue first task: %w", err)
	}

	return id, nil
}

// advanceChainScript atomically increments the chain step counter and returns
// the new value, ensuring exactly one server advances each step.
var advanceChainScript = redis.NewScript(`
local counterKey = KEYS[1]
return redis.call("INCR", counterKey)
`)

func (m *Manager) advanceChain(ctx context.Context, rec *Record) error {
	current, err := advanceChainScript.Run(ctx, m.rdb, []string{counterKey(rec.ID)}).Int64()
	if err != nil {
		return fmt.Errorf("winter: chain advance: %w", err)
	}

	rec.Current = int(current)

	if rec.Current >= rec.Total {
		rec.State = "completed"
		return m.saveRecord(ctx, rec)
	}

	if err := m.saveRecord(ctx, rec); err != nil {
		return err
	}

	if _, err := m.enqueueTask(ctx, rec.Tasks[rec.Current], rec.ID); err != nil {
		return fmt.Errorf("winter: chain enqueue step %d: %w", rec.Current, err)
	}

	return nil
}
