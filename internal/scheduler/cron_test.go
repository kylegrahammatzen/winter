package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (*queue.Queue, redis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return queue.New(rdb), rdb
}

// Parses a valid cron expression and rejects an invalid one.
func TestNewCronValidation(t *testing.T) {
	q, rdb := setup(t)

	_, err := NewCron(q, rdb, []Entry{
		{Name: "good", Schedule: "* * * * *", Kind: "test.job"},
	}, CronConfig{})
	require.NoError(t, err)

	_, err = NewCron(q, rdb, []Entry{
		{Name: "bad", Schedule: "not a cron", Kind: "test.job"},
	}, CronConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cron schedule")
}

// Seeds initial state for entries that have no Redis state yet.
func TestCronSeedsNextRun(t *testing.T) {
	q, rdb := setup(t)
	ctx := context.Background()

	cron, err := NewCron(q, rdb, []Entry{
		{Name: "every-minute", Schedule: "* * * * *", Kind: "test.job"},
	}, CronConfig{})
	require.NoError(t, err)

	cron.seedNextRuns(ctx)

	raw, err := rdb.HGet(ctx, cronKey, "every-minute").Result()
	require.NoError(t, err)

	var state cronState
	require.NoError(t, json.Unmarshal([]byte(raw), &state))
	assert.Greater(t, state.NextRun, time.Now().UnixMilli())
}

// Fires a cron entry whose next-run is in the past and verifies a job is enqueued.
func TestCronTickEnqueuesJob(t *testing.T) {
	q, rdb := setup(t)
	ctx := context.Background()

	cron, err := NewCron(q, rdb, []Entry{
		{Name: "every-minute", Schedule: "* * * * *", Kind: "test.job", Payload: []byte(`{"x":1}`)},
	}, CronConfig{})
	require.NoError(t, err)

	// Set next-run to 1 second in the past so the tick fires immediately.
	pastState := cronState{NextRun: time.Now().Add(-1 * time.Second).UnixMilli()}
	data, _ := json.Marshal(pastState)
	rdb.HSet(ctx, cronKey, "every-minute", string(data))

	cron.tick(ctx)

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats["ready"])
	assert.Equal(t, int64(1), stats["enqueued"])
}

// Verifies that a future next-run time does not trigger an enqueue.
func TestCronTickSkipsFuture(t *testing.T) {
	q, rdb := setup(t)
	ctx := context.Background()

	cron, err := NewCron(q, rdb, []Entry{
		{Name: "every-minute", Schedule: "* * * * *", Kind: "test.job"},
	}, CronConfig{})
	require.NoError(t, err)

	futureState := cronState{NextRun: time.Now().Add(5 * time.Minute).UnixMilli()}
	data, _ := json.Marshal(futureState)
	rdb.HSet(ctx, cronKey, "every-minute", string(data))

	cron.tick(ctx)

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats["ready"])
}

// Two concurrent ticks on the same entry only enqueue one job thanks to CAS.
func TestCronTickIdempotent(t *testing.T) {
	q, rdb := setup(t)
	ctx := context.Background()

	cron1, err := NewCron(q, rdb, []Entry{
		{Name: "every-minute", Schedule: "* * * * *", Kind: "test.job"},
	}, CronConfig{})
	require.NoError(t, err)

	cron2, err := NewCron(q, rdb, []Entry{
		{Name: "every-minute", Schedule: "* * * * *", Kind: "test.job"},
	}, CronConfig{})
	require.NoError(t, err)

	pastState := cronState{NextRun: time.Now().Add(-1 * time.Second).UnixMilli()}
	data, _ := json.Marshal(pastState)
	rdb.HSet(ctx, cronKey, "every-minute", string(data))

	cron1.tick(ctx)
	cron2.tick(ctx)

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats["enqueued"])
}

// After a tick fires, the next-run state advances to a future timestamp.
func TestCronTickAdvancesNextRun(t *testing.T) {
	q, rdb := setup(t)
	ctx := context.Background()

	cron, err := NewCron(q, rdb, []Entry{
		{Name: "every-minute", Schedule: "* * * * *", Kind: "test.job"},
	}, CronConfig{})
	require.NoError(t, err)

	pastState := cronState{NextRun: time.Now().Add(-1 * time.Second).UnixMilli()}
	data, _ := json.Marshal(pastState)
	rdb.HSet(ctx, cronKey, "every-minute", string(data))

	cron.tick(ctx)

	raw, err := rdb.HGet(ctx, cronKey, "every-minute").Result()
	require.NoError(t, err)

	var newState cronState
	require.NoError(t, json.Unmarshal([]byte(raw), &newState))
	assert.Greater(t, newState.NextRun, time.Now().UnixMilli())
}

// Uses a custom queue name from the entry config.
func TestCronCustomQueue(t *testing.T) {
	q, rdb := setup(t)
	ctx := context.Background()

	cron, err := NewCron(q, rdb, []Entry{
		{Name: "maintenance", Schedule: "* * * * *", Kind: "cleanup.run", Queue: "maintenance"},
	}, CronConfig{})
	require.NoError(t, err)

	pastState := cronState{NextRun: time.Now().Add(-1 * time.Second).UnixMilli()}
	data, _ := json.Marshal(pastState)
	rdb.HSet(ctx, cronKey, "maintenance", string(data))

	cron.tick(ctx)

	stats, err := q.QueueStats(ctx, "maintenance")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats["enqueued"])

	// Default queue should be empty.
	defaultStats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(0), defaultStats["enqueued"])
}
