package winter

import (
	"context"
	"fmt"
)

// Middleware wraps a handler to add cross-cutting behavior like panic recovery or logging.
type Middleware func(next HandlerFn) HandlerFn

// HandlerFn is the untyped handler signature used internally by the middleware chain.
type HandlerFn func(ctx context.Context, job *rawJob) error

// rawJob holds the deserialized Redis hash fields before they are converted
// into a typed Job[T] by the registered handler.
type rawJob struct {
	ID          string
	Kind        string
	Queue       string
	Priority    int
	State       JobState
	Attempt     int
	MaxRetries  int
	Payload     []byte
	CreatedAt   int64
	ScheduledAt int64
	StartedAt   int64
	LastError   string
}

// Recover returns a middleware that catches panics and converts them to errors.
func Recover() Middleware {
	return func(next HandlerFn) HandlerFn {
		return func(ctx context.Context, job *rawJob) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("winter: panic processing job %s: %v", job.ID, r)
				}
			}()
			return next(ctx, job)
		}
	}
}
