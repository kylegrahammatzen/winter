// Package wintertest provides test helpers for asserting that jobs were enqueued
// correctly. It uses miniredis so no running Redis server is required.
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

// NewClient returns a Client backed by miniredis that is automatically cleaned
// up when the test finishes.
func NewClient(t testing.TB) *winter.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return winter.NewClientFromRedis(rdb)
}

// RequireEnqueued fails the test if no job matching the expected kind and payload exists.
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

// RequireEnqueuedN fails the test if the number of jobs with the given kind does not equal n.
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

// RequireNoneEnqueued fails the test if any jobs with the given kind exist.
func RequireNoneEnqueued(t testing.TB, c *winter.Client, kind string) {
	t.Helper()
	RequireEnqueuedN(t, c, kind, 0)
}

// findJobsByKind scans all job hashes in Redis and returns those matching the given kind.
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
