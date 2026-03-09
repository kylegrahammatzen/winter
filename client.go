package winter

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
)

// RedisConfig holds connection parameters for the Redis server.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// Client wraps a Redis connection and provides the Enqueue API.
type Client struct {
	rdb   redis.UniversalClient
	queue *queue.Queue
}

// NewClient connects to Redis and returns a client ready to enqueue jobs.
func NewClient(cfg RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("winter: redis ping: %w", err)
	}

	return &Client{
		rdb:   rdb,
		queue: queue.New(rdb),
	}, nil
}

// NewClientFromRedis creates a client from an existing Redis connection.
func NewClientFromRedis(rdb redis.UniversalClient) *Client {
	return &Client{
		rdb:   rdb,
		queue: queue.New(rdb),
	}
}

// Close shuts down the underlying Redis connection.
func (c *Client) Close() error {
	if closer, ok := c.rdb.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// Queue returns the underlying queue implementation for internal use.
func (c *Client) Queue() *queue.Queue {
	return c.queue
}

// Redis returns the underlying Redis client for advanced use cases.
func (c *Client) Redis() redis.UniversalClient {
	return c.rdb
}

// Enqueue serializes the task payload and atomically inserts it into the queue.
func Enqueue[T Task](c *Client, ctx context.Context, args T, opts ...Option) (*Job[T], error) {
	o := &insertOpts{
		queue:    "default",
		priority: 5,
	}

	if tw, ok := any(args).(TaskWithOptions); ok {
		taskOpts := tw.Options()
		if taskOpts.Queue != "" {
			o.queue = taskOpts.Queue
		}
		if taskOpts.MaxRetries > 0 {
			o.priority = taskOpts.MaxRetries
		}
	}

	for _, opt := range opts {
		opt(o)
	}

	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("winter: marshal args: %w", err)
	}

	maxRetries := 3
	if tw, ok := any(args).(TaskWithOptions); ok {
		if mr := tw.Options().MaxRetries; mr > 0 {
			maxRetries = mr
		}
	}

	now := time.Now()
	id := uuid.New().String()

	var uniqueKey string
	if o.uniquePeriod > 0 {
		hash := sha256.Sum256(payload)
		uniqueKey = fmt.Sprintf("%s:%x", args.Kind(), hash)
	}

	job := &queue.JobRecord{
		ID:          id,
		Kind:        args.Kind(),
		Queue:       o.queue,
		Priority:    o.priority,
		State:       string(StatePending),
		Payload:     payload,
		MaxRetries:  maxRetries,
		CreatedAt:   now.UnixMilli(),
		ScheduledAt: o.scheduledAt.UnixMilli(),
	}

	if err := c.queue.Enqueue(ctx, job, uniqueKey, o.uniquePeriod); err != nil {
		return nil, err
	}

	return &Job[T]{
		ID:          id,
		Args:        args,
		Kind:        args.Kind(),
		Queue:       o.queue,
		Priority:    o.priority,
		State:       StatePending,
		Attempt:     0,
		MaxRetries:  maxRetries,
		CreatedAt:   now,
		ScheduledAt: o.scheduledAt,
	}, nil
}

// EnqueueMany inserts multiple tasks using Redis pipelining for high throughput.
// Options are applied uniformly to all jobs in the batch.
func EnqueueMany[T Task](c *Client, ctx context.Context, tasks []T, opts ...Option) ([]*Job[T], error) {
	o := &insertOpts{
		queue:    "default",
		priority: 5,
	}

	for _, opt := range opts {
		opt(o)
	}

	now := time.Now()
	records := make([]*queue.JobRecord, len(tasks))
	jobs := make([]*Job[T], len(tasks))

	for i, args := range tasks {
		payload, err := json.Marshal(args)
		if err != nil {
			return nil, fmt.Errorf("winter: marshal args[%d]: %w", i, err)
		}

		maxRetries := 3
		if tw, ok := any(args).(TaskWithOptions); ok {
			taskOpts := tw.Options()
			if taskOpts.MaxRetries > 0 {
				maxRetries = taskOpts.MaxRetries
			}
		}

		id := uuid.New().String()

		records[i] = &queue.JobRecord{
			ID:          id,
			Kind:        args.Kind(),
			Queue:       o.queue,
			Priority:    o.priority,
			State:       string(StatePending),
			Payload:     payload,
			MaxRetries:  maxRetries,
			CreatedAt:   now.UnixMilli(),
			ScheduledAt: o.scheduledAt.UnixMilli(),
		}

		jobs[i] = &Job[T]{
			ID:          id,
			Args:        args,
			Kind:        args.Kind(),
			Queue:       o.queue,
			Priority:    o.priority,
			State:       StatePending,
			MaxRetries:  maxRetries,
			CreatedAt:   now,
			ScheduledAt: o.scheduledAt,
		}
	}

	if err := c.queue.EnqueueMany(ctx, records); err != nil {
		return nil, err
	}

	return jobs, nil
}
