package wintertest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/kylegrahammatzen/winter"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
)

func NewClient(t testing.TB) *winter.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return winter.NewClientFromRedis(rdb)
}

func RequireEnqueued[T winter.Task](t testing.TB, c *winter.Client, expected T) {
	t.Helper()

	expectedPayload, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("wintertest: marshal expected args: %v", err)
	}

	jobs, err := findJobsByKind(c, expected.Kind())
	if err != nil {
		t.Fatalf("wintertest: scan jobs: %v", err)
	}

	if len(jobs) == 0 {
		t.Fatalf("wintertest: expected job with kind %q to be enqueued, but none found", expected.Kind())
	}

	for _, job := range jobs {
		if string(job.Payload) == string(expectedPayload) {
			return
		}
	}

	t.Fatalf("wintertest: found %d job(s) with kind %q, but none matched expected args", len(jobs), expected.Kind())
}

func RequireEnqueuedN(t testing.TB, c *winter.Client, kind string, n int) {
	t.Helper()

	jobs, err := findJobsByKind(c, kind)
	if err != nil {
		t.Fatalf("wintertest: scan jobs: %v", err)
	}

	if len(jobs) != n {
		t.Fatalf("wintertest: expected %d job(s) with kind %q, got %d", n, kind, len(jobs))
	}
}

func RequireNoneEnqueued(t testing.TB, c *winter.Client, kind string) {
	t.Helper()
	RequireEnqueuedN(t, c, kind, 0)
}

func findJobsByKind(c *winter.Client, kind string) ([]*queue.JobRecord, error) {
	ctx := context.Background()
	rdb := c.Redis()

	var matched []*queue.JobRecord
	var cursor uint64

	for {
		keys, next, err := rdb.Scan(ctx, cursor, "winter:job:*", 100).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			vals, err := rdb.HGetAll(ctx, key).Result()
			if err != nil {
				continue
			}
			if vals["kind"] == kind {
				matched = append(matched, &queue.JobRecord{
					ID:      vals["id"],
					Kind:    vals["kind"],
					Queue:   vals["queue"],
					Payload: []byte(vals["payload"]),
				})
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	return matched, nil
}
