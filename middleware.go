package winter

import (
	"context"
	"fmt"
)

type Middleware func(next HandlerFn) HandlerFn

type HandlerFn func(ctx context.Context, job *rawJob) error

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
