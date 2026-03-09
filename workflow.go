package winter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kylegrahammatzen/winter/internal/workflow"
)

// Chain enqueues tasks sequentially. Each task is enqueued only after the
// previous one completes. If any task fails and exhausts retries, the chain stops.
func Chain(c *Client, ctx context.Context, tasks []Task, opts ...Option) (string, error) {
	if len(tasks) == 0 {
		return "", fmt.Errorf("winter: chain requires at least one task")
	}

	o := &insertOpts{queue: "default", priority: 5}
	for _, opt := range opts {
		opt(o)
	}

	specs, err := tasksToSpecs(tasks, o)
	if err != nil {
		return "", err
	}

	mgr := workflow.NewManager(c.queue, c.rdb)
	return mgr.CreateChain(ctx, specs, o.queue)
}

// Group enqueues all tasks immediately for parallel execution and tracks
// completion. Returns the workflow ID for status checks.
func Group(c *Client, ctx context.Context, tasks []Task, opts ...Option) (string, error) {
	if len(tasks) == 0 {
		return "", fmt.Errorf("winter: group requires at least one task")
	}

	o := &insertOpts{queue: "default", priority: 5}
	for _, opt := range opts {
		opt(o)
	}

	specs, err := tasksToSpecs(tasks, o)
	if err != nil {
		return "", err
	}

	mgr := workflow.NewManager(c.queue, c.rdb)
	return mgr.CreateGroup(ctx, specs, o.queue)
}

// Chord enqueues all header tasks for parallel execution and fires the callback
// task only after every header completes. If any header fails, the callback
// does not fire.
func Chord(c *Client, ctx context.Context, headers []Task, callback Task, opts ...Option) (string, error) {
	if len(headers) == 0 {
		return "", fmt.Errorf("winter: chord requires at least one header task")
	}

	o := &insertOpts{queue: "default", priority: 5}
	for _, opt := range opts {
		opt(o)
	}

	headerSpecs, err := tasksToSpecs(headers, o)
	if err != nil {
		return "", err
	}

	cbPayload, err := json.Marshal(callback)
	if err != nil {
		return "", fmt.Errorf("winter: marshal callback: %w", err)
	}

	cbSpec := workflow.TaskSpec{
		Kind:    callback.Kind(),
		Payload: cbPayload,
		Queue:   o.queue,
	}
	if tw, ok := callback.(TaskWithOptions); ok {
		taskOpts := tw.Options()
		if taskOpts.Queue != "" {
			cbSpec.Queue = taskOpts.Queue
		}
		if taskOpts.MaxRetries > 0 {
			cbSpec.MaxRetries = taskOpts.MaxRetries
		}
	}

	mgr := workflow.NewManager(c.queue, c.rdb)
	return mgr.CreateChord(ctx, headerSpecs, cbSpec, o.queue)
}

func tasksToSpecs(tasks []Task, o *insertOpts) ([]workflow.TaskSpec, error) {
	specs := make([]workflow.TaskSpec, len(tasks))
	for i, t := range tasks {
		payload, err := json.Marshal(t)
		if err != nil {
			return nil, fmt.Errorf("winter: marshal task[%d]: %w", i, err)
		}

		maxRetries := 3
		queue := o.queue
		if tw, ok := t.(TaskWithOptions); ok {
			taskOpts := tw.Options()
			if taskOpts.MaxRetries > 0 {
				maxRetries = taskOpts.MaxRetries
			}
			if taskOpts.Queue != "" {
				queue = taskOpts.Queue
			}
		}

		specs[i] = workflow.TaskSpec{
			Kind:       t.Kind(),
			Payload:    payload,
			Queue:      queue,
			Priority:   o.priority,
			MaxRetries: maxRetries,
		}
	}
	return specs, nil
}
