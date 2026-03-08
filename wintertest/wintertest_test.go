package wintertest

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/winter"
)

type testTask struct {
	UserID int `json:"user_id"`
}

func (testTask) Kind() string { return "test.task" }

func TestRequireEnqueued(t *testing.T) {
	client := NewClient(t)
	ctx := context.Background()

	winter.Enqueue(client, ctx, testTask{UserID: 42})

	RequireEnqueued(t, client, testTask{UserID: 42})
}

func TestRequireEnqueuedN(t *testing.T) {
	client := NewClient(t)
	ctx := context.Background()

	winter.Enqueue(client, ctx, testTask{UserID: 1})
	winter.Enqueue(client, ctx, testTask{UserID: 2})
	winter.Enqueue(client, ctx, testTask{UserID: 3})

	RequireEnqueuedN(t, client, "test.task", 3)
}

func TestRequireNoneEnqueued(t *testing.T) {
	client := NewClient(t)

	RequireNoneEnqueued(t, client, "test.task")
}
