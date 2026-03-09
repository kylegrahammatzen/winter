// Package scheduler implements periodic job scheduling with cron expressions.
// It uses robfig/cron for parsing but manages its own tick loop and Redis-backed
// state to guarantee exactly-once enqueue per scheduled window.
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

const cronKey = "winter:cron"

// Entry defines a periodic job with a cron expression.
type Entry struct {
	Name     string
	Schedule string
	Queue    string
	Kind     string
	Payload  []byte
}

type parsedEntry struct {
	Entry
	sched cron.Schedule
}

// cronState is persisted in Redis as JSON for each entry.
type cronState struct {
	NextRun int64 `json:"next_run"`
}

// CronConfig controls the tick interval and logging.
type CronConfig struct {
	Interval time.Duration
	Logger   *slog.Logger
}

// Cron checks entries on a regular interval and enqueues jobs when their
// scheduled time has passed. Uses Redis HSETNX as an idempotent guard so
// multiple server instances cannot double-enqueue the same tick.
type Cron struct {
	q       *queue.Queue
	rdb     redis.UniversalClient
	entries []parsedEntry
	cfg     CronConfig
}

// NewCron parses all entry schedules and returns a ready-to-run scheduler.
func NewCron(q *queue.Queue, rdb redis.UniversalClient, entries []Entry, cfg CronConfig) (*Cron, error) {
	if cfg.Interval <= 0 {
		cfg.Interval = 1 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	parsed := make([]parsedEntry, len(entries))

	for i, e := range entries {
		sched, err := parser.Parse(e.Schedule)
		if err != nil {
			return nil, fmt.Errorf("winter: invalid cron schedule %q for %q: %w", e.Schedule, e.Name, err)
		}
		if e.Queue == "" {
			e.Queue = "default"
		}
		parsed[i] = parsedEntry{Entry: e, sched: sched}
	}

	return &Cron{
		q:       q,
		rdb:     rdb,
		entries: parsed,
		cfg:     cfg,
	}, nil
}

// Run starts the cron tick loop and blocks until ctx is cancelled.
func (c *Cron) Run(ctx context.Context) {
	// Seed initial next-run times for any entries that don't have state yet.
	c.seedNextRuns(ctx)

	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Cron) seedNextRuns(ctx context.Context) {
	now := time.Now()
	for _, e := range c.entries {
		state := cronState{NextRun: e.sched.Next(now).UnixMilli()}
		data, _ := json.Marshal(state)

		// HSETNX: only sets if the field does not already exist.
		c.rdb.HSetNX(ctx, cronKey, e.Name, string(data))
	}
}

func (c *Cron) tick(ctx context.Context) {
	now := time.Now()

	for _, e := range c.entries {
		raw, err := c.rdb.HGet(ctx, cronKey, e.Name).Result()
		if err != nil {
			continue
		}

		var state cronState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			c.cfg.Logger.Error("winter: cron state unmarshal error", "entry", e.Name, "error", err)
			continue
		}

		nextRun := time.UnixMilli(state.NextRun)
		if now.Before(nextRun) {
			continue
		}

		// Compute the following run before enqueueing so we can atomically
		// update the state. Using the next run after `now` rather than after
		// `nextRun` handles cases where the scheduler was down for a while
		// and prevents a burst of catch-up enqueues.
		following := e.sched.Next(now)
		newState := cronState{NextRun: following.UnixMilli()}
		newData, _ := json.Marshal(newState)

		// Atomically swap the state only if it still matches what we read.
		// This prevents two instances from both enqueuing the same tick.
		swapped, err := c.compareAndSwap(ctx, e.Name, raw, string(newData))
		if err != nil {
			c.cfg.Logger.Error("winter: cron cas error", "entry", e.Name, "error", err)
			continue
		}
		if !swapped {
			continue
		}

		id := fmt.Sprintf("cron:%s:%d", e.Name, state.NextRun)
		job := &queue.JobRecord{
			ID:         id,
			Kind:       e.Kind,
			Queue:      e.Queue,
			Priority:   5,
			State:      "pending",
			Payload:    e.Payload,
			MaxRetries: 3,
			CreatedAt:  now.UnixMilli(),
		}

		if err := c.q.Enqueue(ctx, job, "", 0); err != nil {
			c.cfg.Logger.Error("winter: cron enqueue error", "entry", e.Name, "error", err)
			continue
		}

		c.cfg.Logger.Info("winter: cron job enqueued",
			"entry", e.Name,
			"job_id", id,
			"next_run", following.Format(time.RFC3339),
		)
	}
}

// compareAndSwapScript atomically updates a hash field only if its current
// value matches the expected value.
var compareAndSwapScript = redis.NewScript(`
local key = KEYS[1]
local field = ARGV[1]
local expected = ARGV[2]
local newVal = ARGV[3]

local current = redis.call("HGET", key, field)
if current == expected then
    redis.call("HSET", key, field, newVal)
    return 1
end
return 0
`)

func (c *Cron) compareAndSwap(ctx context.Context, field, expected, newVal string) (bool, error) {
	result, err := compareAndSwapScript.Run(ctx, c.rdb, []string{cronKey}, field, expected, newVal).Result()
	if err != nil {
		return false, err
	}
	return result.(int64) == 1, nil
}
