// Package workflow implements Chain, Group, and Chord primitives for composing
// multi-job pipelines. All state is stored in Redis so workflows survive server
// restarts and work correctly across multiple server instances.
package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
)

func workflowKey(id string) string { return "winter:workflow:" + id }
func depsKey(id string) string     { return "winter:workflow:" + id + ":deps" }
func counterKey(id string) string  { return "winter:workflow:" + id + ":counter" }

// WorkflowType identifies the kind of workflow.
type WorkflowType string

const (
	TypeChain WorkflowType = "chain"
	TypeGroup WorkflowType = "group"
	TypeChord WorkflowType = "chord"
)

// TaskSpec holds the serialized representation of a task to be enqueued as
// part of a workflow. The workflow package stores these and creates JobRecords
// from them when it is time to enqueue the next step.
type TaskSpec struct {
	Kind       string `json:"kind"`
	Payload    []byte `json:"payload"`
	Queue      string `json:"queue"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
}

// Record is the persisted state of a workflow stored as a Redis hash.
type Record struct {
	ID       string       `json:"id"`
	Type     WorkflowType `json:"type"`
	State    string       `json:"state"`
	Tasks    []TaskSpec   `json:"tasks"`
	Callback *TaskSpec    `json:"callback,omitempty"`
	Current  int          `json:"current"`
	Total    int          `json:"total"`
	Done     int          `json:"done"`
	Queue    string       `json:"queue"`
}

// Manager handles workflow creation and advancement backed by Redis.
type Manager struct {
	q   *queue.Queue
	rdb redis.UniversalClient
}

// NewManager creates a workflow manager.
func NewManager(q *queue.Queue, rdb redis.UniversalClient) *Manager {
	return &Manager{q: q, rdb: rdb}
}

func (m *Manager) saveRecord(ctx context.Context, rec *Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("winter: marshal workflow: %w", err)
	}
	return m.rdb.Set(ctx, workflowKey(rec.ID), string(data), 7*24*time.Hour).Err()
}

func (m *Manager) loadRecord(ctx context.Context, workflowID string) (*Record, error) {
	raw, err := m.rdb.Get(ctx, workflowKey(workflowID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("winter: load workflow: %w", err)
	}
	var rec Record
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, fmt.Errorf("winter: unmarshal workflow: %w", err)
	}
	return &rec, nil
}

func (m *Manager) enqueueTask(ctx context.Context, spec TaskSpec, workflowID string) (string, error) {
	id := uuid.New().String()
	queueName := spec.Queue
	if queueName == "" {
		queueName = "default"
	}
	maxRetries := spec.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	job := &queue.JobRecord{
		ID:         id,
		Kind:       spec.Kind,
		Queue:      queueName,
		Priority:   spec.Priority,
		State:      "pending",
		Payload:    spec.Payload,
		MaxRetries: maxRetries,
		CreatedAt:  time.Now().UnixMilli(),
		WorkflowID: workflowID,
	}

	if err := m.q.Enqueue(ctx, job, "", 0); err != nil {
		return "", err
	}
	return id, nil
}

// OnJobCompleted is called by the server after acking a job that has a workflow_id.
// It loads the workflow record and advances state depending on the workflow type.
func (m *Manager) OnJobCompleted(ctx context.Context, workflowID string, jobID string) error {
	rec, err := m.loadRecord(ctx, workflowID)
	if err != nil {
		return err
	}
	if rec == nil || rec.State == "completed" || rec.State == "failed" {
		return nil
	}

	switch rec.Type {
	case TypeChain:
		return m.advanceChain(ctx, rec)
	case TypeGroup:
		return m.advanceGroup(ctx, rec)
	case TypeChord:
		return m.advanceChord(ctx, rec, jobID)
	default:
		return fmt.Errorf("winter: unknown workflow type %q", rec.Type)
	}
}

// OnJobFailed is called when a job in a workflow exhausts retries and goes to dead.
func (m *Manager) OnJobFailed(ctx context.Context, workflowID string, jobID string) error {
	rec, err := m.loadRecord(ctx, workflowID)
	if err != nil {
		return err
	}
	if rec == nil {
		return nil
	}

	rec.State = "failed"
	return m.saveRecord(ctx, rec)
}
