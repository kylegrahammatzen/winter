package winter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

type Task interface {
	Kind() string
}

type TaskWithOptions interface {
	Task
	Options() TaskOptions
}

type TaskOptions struct {
	Queue      string
	MaxRetries int
	Timeout    time.Duration
	Backoff    BackoffStrategy
	RateLimit  Rate
}

type BackoffStrategy interface {
	Next(attempt int) time.Duration
}

type Rate struct {
	Max int
	Per time.Duration
}

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

var validTransitions = map[JobState][]JobState{
	StatePending:   {StateActive},
	StateActive:    {StateCompleted, StateFailed, StateRetry, StateCancelled},
	StateRetry:     {StatePending, StateActive},
	StateFailed:    {StateDead, StatePending},
	StateDead:      {StatePending},
	StateCancelled: {},
	StateCompleted: {},
}

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
}

type Option func(*insertOpts)

type insertOpts struct {
	queue        string
	priority     int
	scheduledAt  time.Time
	uniquePeriod time.Duration
}

func Queue(name string) Option {
	return func(o *insertOpts) {
		o.queue = name
	}
}

func Priority(p int) Option {
	return func(o *insertOpts) {
		o.priority = p
	}
}

func In(d time.Duration) Option {
	return func(o *insertOpts) {
		o.scheduledAt = time.Now().Add(d)
	}
}

func At(t time.Time) Option {
	return func(o *insertOpts) {
		o.scheduledAt = t
	}
}

func Unique(period time.Duration) Option {
	return func(o *insertOpts) {
		o.uniquePeriod = period
	}
}

type Handler[T Task] interface {
	Work(ctx context.Context, job *Job[T]) error
}

type HandlerFunc[T Task] func(ctx context.Context, job *Job[T]) error

func (f HandlerFunc[T]) Work(ctx context.Context, job *Job[T]) error {
	return f(ctx, job)
}

var ErrSkipRetry = errors.New("winter: skip retry")

var ErrDuplicate = errors.New("winter: duplicate job")

type rescheduleError struct {
	delay time.Duration
}

func (e *rescheduleError) Error() string {
	return fmt.Sprintf("winter: reschedule in %s", e.delay)
}

func Reschedule(d time.Duration) error {
	return &rescheduleError{delay: d}
}

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

func Cancel(reason string) error {
	return &cancelError{reason: reason}
}

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

func Exponential(base time.Duration) BackoffStrategy {
	return &exponentialBackoff{base: base}
}

func (b *exponentialBackoff) Next(attempt int) time.Duration {
	return b.base * time.Duration(math.Pow(2, float64(attempt)))
}

type linearBackoff struct {
	step time.Duration
}

func Linear(step time.Duration) BackoffStrategy {
	return &linearBackoff{step: step}
}

func (b *linearBackoff) Next(attempt int) time.Duration {
	return b.step * time.Duration(attempt+1)
}

type fixedBackoff struct {
	d time.Duration
}

func Fixed(d time.Duration) BackoffStrategy {
	return &fixedBackoff{d: d}
}

func (b *fixedBackoff) Next(_ int) time.Duration {
	return b.d
}
