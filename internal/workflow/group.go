package workflow

import (
	"context"
	"fmt"

	"github.com/google/uuid"
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

func (m *Manager) advanceGroup(ctx context.Context, rec *Record) error {
	rec.Done++

	if rec.Done >= rec.Total {
		rec.State = "completed"
	}

	return m.saveRecord(ctx, rec)
}
