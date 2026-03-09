// Package winter is a distributed task queue backed by Redis with generics-first
// type safety, workflow primitives, and a standalone gRPC server mode.
package winter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/kylegrahammatzen/winter/internal/queue"
)

// Task is the interface that all job payloads must implement.
type Task interface {
	Kind() string
}

// TaskWithOptions extends Task with default queue, retry, and rate limit configuration.
type TaskWithOptions interface {
	Task
	Options() TaskOptions
}

// TaskOptions holds per-task-kind defaults that override server-level configuration.
type TaskOptions struct {
	Queue      string
	MaxRetries int
	Timeout    time.Duration
	Backoff    BackoffStrategy
	RateLimit  Rate
}

// BackoffStrategy computes the delay before the next retry attempt.
type BackoffStrategy interface {
	Next(attempt int) time.Duration
}

// Rate configures per-task-kind rate limiting as a token bucket.
type Rate struct {
	Max int
	Per time.Duration
}

// JobState represents the current lifecycle state of a job.
type JobState string

const (
	StatePending   JobState = "pending"
	StateActive    JobState = "active"
	StateCompleted JobState = "completed"
	StateFailed    JobState = "failed"
	StateRetry     JobState = "retry"
	StateDead      JobState = "dead"
	StateCancelled JobState = "cancelled"
)

// validTransitions defines the legal state machine edges for job lifecycle.
var validTransitions = map[JobState][]JobState{
	StatePending:   {StateActive},
	StateActive:    {StateCompleted, StateFailed, StateRetry, StateCancelled},
	StateRetry:     {StatePending, StateActive},
	StateFailed:    {StateDead, StatePending},
	StateDead:      {StatePending},
	StateCancelled: {},
	StateCompleted: {},
}

// ValidTransition reports whether transitioning from one state to another is allowed.
func ValidTransition(from, to JobState) bool {
	targets, ok := validTransitions[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}

// JobEvent is passed to lifecycle hooks with context about the job and any error.
type JobEvent struct {
	ID      string
	Kind    string
	Queue   string
	Attempt int
	Err     error
}

// HookFunc is the signature for lifecycle hook callbacks.
type HookFunc func(ctx context.Context, event JobEvent)

// Job is a type-safe wrapper around a queued job and its deserialized payload.
type Job[T Task] struct {
	ID          string    `json:"id"`
	Args        T         `json:"args"`
	Kind        string    `json:"kind"`
	Queue       string    `json:"queue"`
	Priority    int       `json:"priority"`
	State       JobState  `json:"state"`
	Attempt     int       `json:"attempt"`
	MaxRetries  int       `json:"max_retries"`
	CreatedAt   time.Time `json:"created_at"`
	ScheduledAt time.Time `json:"scheduled_at,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	raw         *rawJob
}

// SetResult stores a result value to be persisted after the job completes.
// The value is JSON-serialized and stored in Redis with a 7-day TTL.
// Retrieve it later with inspector.JobResult or WaitForResult.
func (j *Job[T]) SetResult(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("winter: marshal result: %w", err)
	}
	if j.raw != nil {
		j.raw.result = data
	}
	return nil
}

// Option configures how a job is enqueued.
type Option func(*insertOpts)

type insertOpts struct {
	queue        string
	priority     int
	scheduledAt  time.Time
	uniquePeriod time.Duration
}

// Queue overrides the destination queue for a job.
func Queue(name string) Option {
	return func(o *insertOpts) {
		o.queue = name
	}
}

// Priority sets the job's priority where lower values are dequeued first.
func Priority(p int) Option {
	return func(o *insertOpts) {
		o.priority = p
	}
}

// In schedules the job to run after the given delay.
func In(d time.Duration) Option {
	return func(o *insertOpts) {
		o.scheduledAt = time.Now().Add(d)
	}
}

// At schedules the job to run at a specific time.
func At(t time.Time) Option {
	return func(o *insertOpts) {
		o.scheduledAt = t
	}
}

// Unique deduplicates jobs with the same kind and payload within the given period.
func Unique(period time.Duration) Option {
	return func(o *insertOpts) {
		o.uniquePeriod = period
	}
}

// Handler processes jobs of a specific task type.
type Handler[T Task] interface {
	Work(ctx context.Context, job *Job[T]) error
}

// HandlerFunc is an adapter to allow ordinary functions to serve as handlers.
type HandlerFunc[T Task] func(ctx context.Context, job *Job[T]) error

func (f HandlerFunc[T]) Work(ctx context.Context, job *Job[T]) error {
	return f(ctx, job)
}

// ErrSkipRetry is a sentinel error that causes a failed job to go directly to
// the dead letter queue without retrying. Wrap it with fmt.Errorf to add context.
var ErrSkipRetry = errors.New("winter: skip retry")

// ErrDuplicate is returned when enqueuing a job that matches an existing unique constraint.
var ErrDuplicate = queue.ErrDuplicate

type rescheduleError struct {
	delay time.Duration
}

func (e *rescheduleError) Error() string {
	return fmt.Sprintf("winter: reschedule in %s", e.delay)
}

// Reschedule returns an error that instructs the server to put the job back
// in the delayed set and try again after the given duration.
func Reschedule(d time.Duration) error {
	return &rescheduleError{delay: d}
}

// IsReschedule reports whether err is a reschedule sentinel and returns its delay.
func IsReschedule(err error) (time.Duration, bool) {
	var re *rescheduleError
	if errors.As(err, &re) {
		return re.delay, true
	}
	return 0, false
}

type cancelError struct {
	reason string
}

func (e *cancelError) Error() string {
	return fmt.Sprintf("winter: cancel: %s", e.reason)
}

// Cancel returns an error that instructs the server to permanently remove the
// job without retrying.
func Cancel(reason string) error {
	return &cancelError{reason: reason}
}

// IsCancel reports whether err is a cancel sentinel and returns its reason.
func IsCancel(err error) (string, bool) {
	var ce *cancelError
	if errors.As(err, &ce) {
		return ce.reason, true
	}
	return "", false
}

type exponentialBackoff struct {
	base time.Duration
}

// Exponential returns a backoff strategy that doubles the delay on each attempt.
func Exponential(base time.Duration) BackoffStrategy {
	return &exponentialBackoff{base: base}
}

const maxBackoff = 1 * time.Hour

func (b *exponentialBackoff) Next(attempt int) time.Duration {
	d := b.base * time.Duration(math.Pow(2, float64(attempt)))
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

type linearBackoff struct {
	step time.Duration
}

// Linear returns a backoff strategy that increases the delay by a fixed step each attempt.
func Linear(step time.Duration) BackoffStrategy {
	return &linearBackoff{step: step}
}

func (b *linearBackoff) Next(attempt int) time.Duration {
	return b.step * time.Duration(attempt+1)
}

type fixedBackoff struct {
	d time.Duration
}

// Fixed returns a backoff strategy that always waits the same duration between retries.
func Fixed(d time.Duration) BackoffStrategy {
	return &fixedBackoff{d: d}
}

func (b *fixedBackoff) Next(_ int) time.Duration {
	return b.d
}

// WaitForResult polls Redis until a result is available for the given job ID,
// then deserializes it into type R. Returns context.DeadlineExceeded if the
// context expires before a result appears. Poll interval defaults to 250ms.
func WaitForResult[R any](c *Client, ctx context.Context, jobID string) (*R, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		data, err := c.queue.GetResult(ctx, jobID)
		if err != nil {
			return nil, err
		}
		if data != nil {
			var result R
			if err := json.Unmarshal(data, &result); err != nil {
				return nil, fmt.Errorf("winter: unmarshal result: %w", err)
			}
			return &result, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
