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

	_, err := winter.Enqueue(client, ctx, testTask{UserID: 42})
	if err != nil {
		t.Fatal(err)
	}

	RequireEnqueued(t, client, testTask{UserID: 42})
}

func TestRequireEnqueuedN(t *testing.T) {
	client := NewClient(t)
	ctx := context.Background()

	for _, uid := range []int{1, 2, 3} {
		_, err := winter.Enqueue(client, ctx, testTask{UserID: uid})
		if err != nil {
			t.Fatal(err)
		}
	}

	RequireEnqueuedN(t, client, "test.task", 3)
}

func TestRequireNoneEnqueued(t *testing.T) {
	client := NewClient(t)

	RequireNoneEnqueued(t, client, "test.task")
}
