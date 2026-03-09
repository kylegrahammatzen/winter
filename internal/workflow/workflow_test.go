package workflow

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (*Manager, *queue.Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	q := queue.New(rdb)
	return NewManager(q, rdb), q, mr
}

func spec(kind string) TaskSpec {
	return TaskSpec{
		Kind:    kind,
		Payload: []byte(`{}`),
		Queue:   "default",
	}
}

// Creates a chain of 3 tasks, completes each in order, and verifies the next
// step is enqueued only after the previous one finishes.
func TestChainSequentialExecution(t *testing.T) {
	m, q, _ := setup(t)
	ctx := context.Background()

	wfID, err := m.CreateChain(ctx, []TaskSpec{
		spec("step.one"),
		spec("step.two"),
		spec("step.three"),
	}, "default")
	require.NoError(t, err)
	require.NotEmpty(t, wfID)

	// Step 1 should be enqueued.
	rec1, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec1)
	assert.Equal(t, "step.one", rec1.Kind)
	assert.Equal(t, wfID, rec1.WorkflowID)

	// No second job yet.
	empty, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, empty)

	// Complete step 1 triggers step 2.
	err = q.Ack(ctx, "default", rec1.ID, "worker-1")
	require.NoError(t, err)
	err = m.OnJobCompleted(ctx, wfID, rec1.ID)
	require.NoError(t, err)

	rec2, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec2)
	assert.Equal(t, "step.two", rec2.Kind)

	// Complete step 2 triggers step 3.
	err = q.Ack(ctx, "default", rec2.ID, "worker-1")
	require.NoError(t, err)
	err = m.OnJobCompleted(ctx, wfID, rec2.ID)
	require.NoError(t, err)

	rec3, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec3)
	assert.Equal(t, "step.three", rec3.Kind)

	// Complete step 3 finishes the chain.
	err = q.Ack(ctx, "default", rec3.ID, "worker-1")
	require.NoError(t, err)
	err = m.OnJobCompleted(ctx, wfID, rec3.ID)
	require.NoError(t, err)

	wf, err := m.loadRecord(ctx, wfID)
	require.NoError(t, err)
	assert.Equal(t, "completed", wf.State)
}

// Creates a chain and fails the first task, verifying the chain stops.
func TestChainStopsOnFailure(t *testing.T) {
	m, q, _ := setup(t)
	ctx := context.Background()

	wfID, err := m.CreateChain(ctx, []TaskSpec{
		spec("step.one"),
		spec("step.two"),
	}, "default")
	require.NoError(t, err)

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec)

	err = m.OnJobFailed(ctx, wfID, rec.ID)
	require.NoError(t, err)

	wf, err := m.loadRecord(ctx, wfID)
	require.NoError(t, err)
	assert.Equal(t, "failed", wf.State)

	// Step 2 should NOT be enqueued.
	empty, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, empty)
}

// Creates a group of 3 tasks and verifies all are enqueued immediately.
func TestGroupParallelEnqueue(t *testing.T) {
	m, q, _ := setup(t)
	ctx := context.Background()

	wfID, err := m.CreateGroup(ctx, []TaskSpec{
		spec("task.a"),
		spec("task.b"),
		spec("task.c"),
	}, "default")
	require.NoError(t, err)

	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats["ready"])

	// Complete all 3.
	for range 3 {
		rec, err := q.Dequeue(ctx, "default", "worker-1")
		require.NoError(t, err)
		require.NotNil(t, rec)
		assert.Equal(t, wfID, rec.WorkflowID)

		err = q.Ack(ctx, "default", rec.ID, "worker-1")
		require.NoError(t, err)
		err = m.OnJobCompleted(ctx, wfID, rec.ID)
		require.NoError(t, err)
	}

	wf, err := m.loadRecord(ctx, wfID)
	require.NoError(t, err)
	assert.Equal(t, "completed", wf.State)
}

// Group is not complete until all tasks finish.
func TestGroupPartialCompletion(t *testing.T) {
	m, q, _ := setup(t)
	ctx := context.Background()

	wfID, err := m.CreateGroup(ctx, []TaskSpec{
		spec("task.a"),
		spec("task.b"),
	}, "default")
	require.NoError(t, err)

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	err = q.Ack(ctx, "default", rec.ID, "worker-1")
	require.NoError(t, err)
	err = m.OnJobCompleted(ctx, wfID, rec.ID)
	require.NoError(t, err)

	wf, err := m.loadRecord(ctx, wfID)
	require.NoError(t, err)
	assert.Equal(t, "running", wf.State)
}

// Creates a chord with 3 headers and a callback. Completing all headers fires the callback.
func TestChordFiresCallback(t *testing.T) {
	m, q, _ := setup(t)
	ctx := context.Background()

	wfID, err := m.CreateChord(ctx, []TaskSpec{
		spec("build.linux"),
		spec("build.darwin"),
		spec("build.windows"),
	}, spec("deploy"), "default")
	require.NoError(t, err)

	// All 3 headers should be enqueued.
	stats, err := q.QueueStats(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats["ready"])

	// Complete all 3 headers.
	for range 3 {
		rec, err := q.Dequeue(ctx, "default", "worker-1")
		require.NoError(t, err)
		require.NotNil(t, rec)

		err = q.Ack(ctx, "default", rec.ID, "worker-1")
		require.NoError(t, err)
		err = m.OnJobCompleted(ctx, wfID, rec.ID)
		require.NoError(t, err)
	}

	// The callback should now be enqueued.
	callback, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, callback)
	assert.Equal(t, "deploy", callback.Kind)

	wf, err := m.loadRecord(ctx, wfID)
	require.NoError(t, err)
	assert.Equal(t, "completed", wf.State)
}

// Chord callback does not fire if only some headers are complete.
func TestChordPartialDoesNotFireCallback(t *testing.T) {
	m, q, _ := setup(t)
	ctx := context.Background()

	wfID, err := m.CreateChord(ctx, []TaskSpec{
		spec("build.linux"),
		spec("build.darwin"),
	}, spec("deploy"), "default")
	require.NoError(t, err)

	// Complete only 1 of 2.
	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	err = q.Ack(ctx, "default", rec.ID, "worker-1")
	require.NoError(t, err)
	err = m.OnJobCompleted(ctx, wfID, rec.ID)
	require.NoError(t, err)

	// Dequeue the second header but don't complete it.
	rec2, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	require.NotNil(t, rec2)

	// No callback yet since only 1 of 2 completed.
	empty, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)
	assert.Nil(t, empty)

	wf, err := m.loadRecord(ctx, wfID)
	require.NoError(t, err)
	assert.Equal(t, "running", wf.State)
}

// Chord with a failed header marks the workflow as failed and does not fire the callback.
func TestChordFailedHeaderStopsCallback(t *testing.T) {
	m, q, _ := setup(t)
	ctx := context.Background()

	wfID, err := m.CreateChord(ctx, []TaskSpec{
		spec("build.linux"),
		spec("build.darwin"),
	}, spec("deploy"), "default")
	require.NoError(t, err)

	rec, err := q.Dequeue(ctx, "default", "worker-1")
	require.NoError(t, err)

	err = m.OnJobFailed(ctx, wfID, rec.ID)
	require.NoError(t, err)

	wf, err := m.loadRecord(ctx, wfID)
	require.NoError(t, err)
	assert.Equal(t, "failed", wf.State)
}

// An empty task list is rejected for all workflow types.
func TestEmptyTasksRejected(t *testing.T) {
	m, _, _ := setup(t)
	ctx := context.Background()

	_, err := m.CreateChain(ctx, nil, "default")
	require.Error(t, err)

	_, err = m.CreateGroup(ctx, nil, "default")
	require.Error(t, err)

	_, err = m.CreateChord(ctx, nil, spec("cb"), "default")
	require.Error(t, err)
}
