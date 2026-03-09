package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func benchSetup(b *testing.B) *Queue {
	b.Helper()
	mr := miniredis.RunT(b)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	b.Cleanup(func() { rdb.Close() })
	return New(rdb)
}

func benchJob(id string) *JobRecord {
	payload, _ := json.Marshal(map[string]string{"order_id": "abc-123", "amount": "4999"})
	return &JobRecord{
		ID:         id,
		Kind:       "order.process",
		Queue:      "default",
		Priority:   5,
		State:      "pending",
		Payload:    payload,
		MaxRetries: 3,
		CreatedAt:  1700000000000,
	}
}

func BenchmarkEnqueue(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	for i := range b.N {
		id := fmt.Sprintf("job-%d", i)
		_ = q.Enqueue(ctx, benchJob(id), "", 0)
	}
}

func BenchmarkEnqueueParallel(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := fmt.Sprintf("job-p-%d", i)
			_ = q.Enqueue(ctx, benchJob(id), "", 0)
			i++
		}
	})
}

func BenchmarkDequeue(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	for i := range b.N {
		id := fmt.Sprintf("job-%d", i)
		_ = q.Enqueue(ctx, benchJob(id), "", 0)
	}

	b.ResetTimer()
	for range b.N {
		_, _ = q.Dequeue(ctx, "default", "worker-bench")
	}
}

func BenchmarkAck(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	for i := range b.N {
		id := fmt.Sprintf("job-%d", i)
		_ = q.Enqueue(ctx, benchJob(id), "", 0)
	}

	ids := make([]string, b.N)
	for i := range b.N {
		rec, _ := q.Dequeue(ctx, "default", "worker-bench")
		if rec != nil {
			ids[i] = rec.ID
		}
	}

	b.ResetTimer()
	for i := range b.N {
		if ids[i] != "" {
			_ = q.Ack(ctx, "default", ids[i], "worker-bench")
		}
	}
}

func BenchmarkEndToEnd(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	for i := range b.N {
		id := fmt.Sprintf("job-%d", i)
		_ = q.Enqueue(ctx, benchJob(id), "", 0)

		rec, _ := q.Dequeue(ctx, "default", "worker-bench")
		if rec != nil {
			_ = q.Ack(ctx, "default", rec.ID, "worker-bench")
		}
	}
}

// BenchmarkBatchEnqueue1000 measures pipelined insertion of 1000 jobs per iteration.
func BenchmarkBatchEnqueue1000(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	jobs := make([]*JobRecord, 1000)
	for i := range jobs {
		jobs[i] = benchJob(fmt.Sprintf("batch-%d", i))
	}

	b.ResetTimer()
	for range b.N {
		for i, job := range jobs {
			job.ID = fmt.Sprintf("batch-%d-%d", b.N, i)
		}
		_ = q.EnqueueMany(ctx, jobs)
	}
}

// BenchmarkEndToEndMultiQueue measures weighted multi-queue polling where
// jobs are spread across three queues with different weights.
func BenchmarkEndToEndMultiQueue(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	queues := []struct {
		name   string
		weight int
	}{
		{"critical", 3},
		{"default", 2},
		{"low", 1},
	}

	// Expand weights into the same flat list the server uses for polling.
	var weighted []string
	for _, qw := range queues {
		for range qw.weight {
			weighted = append(weighted, qw.name)
		}
	}

	for i := range b.N {
		queueName := queues[i%len(queues)].name
		job := benchJob(fmt.Sprintf("mq-%d", i))
		job.Queue = queueName
		_ = q.Enqueue(ctx, job, "", 0)
	}

	b.ResetTimer()
	processed := 0
	for processed < b.N {
		for _, queueName := range weighted {
			rec, _ := q.Dequeue(ctx, queueName, "worker-bench")
			if rec != nil {
				_ = q.Ack(ctx, queueName, rec.ID, "worker-bench")
				processed++
				if processed >= b.N {
					break
				}
			}
		}
	}
}

func BenchmarkEndToEndParallel(b *testing.B) {
	q := benchSetup(b)
	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := fmt.Sprintf("job-e2e-%d", i)
			_ = q.Enqueue(ctx, benchJob(id), "", 0)

			rec, _ := q.Dequeue(ctx, "default", "worker-bench")
			if rec != nil {
				_ = q.Ack(ctx, "default", rec.ID, "worker-bench")
			}
			i++
		}
	})
}
