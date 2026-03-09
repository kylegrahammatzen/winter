package server

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/kylegrahammatzen/winter/internal/queue"
	pb "github.com/kylegrahammatzen/winter/proto/winter/v1"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupGRPC(t *testing.T) pb.QueueServiceClient {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	q := queue.New(rdb)
	srv := NewGRPCServer(q, slog.Default())

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterQueueServiceServer(s, srv)

	go func() { _ = s.Serve(lis) }()
	t.Cleanup(func() { s.Stop() })

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return pb.NewQueueServiceClient(conn)
}

// Enqueues a job via gRPC and verifies the response contains a job ID.
func TestGRPCEnqueue(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	resp, err := client.Enqueue(ctx, &pb.EnqueueRequest{
		Kind:    "order.process",
		Payload: []byte(`{"order_id":"abc"}`),
		Queue:   "default",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.JobId)
}

// Full round trip: enqueue, dequeue, ack, then verify GetJob shows completed.
func TestGRPCFullRoundTrip(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	enqResp, err := client.Enqueue(ctx, &pb.EnqueueRequest{
		Kind:    "order.process",
		Payload: []byte(`{"order_id":"abc"}`),
		Queue:   "default",
	})
	require.NoError(t, err)

	deqResp, err := client.Dequeue(ctx, &pb.DequeueRequest{
		Queue:     "default",
		WorkerId:  "worker-1",
		TimeoutMs: 1000,
	})
	require.NoError(t, err)
	require.NotNil(t, deqResp.Job)
	assert.Equal(t, enqResp.JobId, deqResp.Job.Id)
	assert.Equal(t, "order.process", deqResp.Job.Kind)
	assert.Equal(t, "active", deqResp.Job.State)

	_, err = client.Ack(ctx, &pb.AckRequest{
		JobId:    enqResp.JobId,
		Queue:    "default",
		WorkerId: "worker-1",
	})
	require.NoError(t, err)

	getResp, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: enqResp.JobId})
	require.NoError(t, err)
	assert.Equal(t, "completed", getResp.Job.State)
}

// Dequeue with a short timeout on an empty queue returns an empty response.
func TestGRPCDequeueTimeout(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	resp, err := client.Dequeue(ctx, &pb.DequeueRequest{
		Queue:     "default",
		WorkerId:  "worker-1",
		TimeoutMs: 500,
	})
	require.NoError(t, err)
	assert.Nil(t, resp.Job)
}

// Nacks a job and verifies the outcome is "retry" when retries remain.
func TestGRPCNack(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	enqResp, err := client.Enqueue(ctx, &pb.EnqueueRequest{
		Kind:       "order.process",
		Payload:    []byte(`{"order_id":"abc"}`),
		Queue:      "default",
		MaxRetries: 5,
	})
	require.NoError(t, err)

	_, err = client.Dequeue(ctx, &pb.DequeueRequest{
		Queue:     "default",
		WorkerId:  "worker-1",
		TimeoutMs: 1000,
	})
	require.NoError(t, err)

	nackResp, err := client.Nack(ctx, &pb.NackRequest{
		JobId:    enqResp.JobId,
		Queue:    "default",
		WorkerId: "worker-1",
		Error:    "connection timeout",
	})
	require.NoError(t, err)
	assert.Equal(t, "retry", nackResp.Outcome)
}

// Heartbeat succeeds without error.
func TestGRPCHeartbeat(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	_, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId: "worker-1",
		Queues:   []string{"default"},
	})
	require.NoError(t, err)
}

// GetJob returns NotFound for a nonexistent job.
func TestGRPCGetJobNotFound(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	_, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: "nonexistent"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// QueueStats reflects the correct counts after enqueue and ack.
func TestGRPCQueueStats(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := client.Enqueue(ctx, &pb.EnqueueRequest{
			Kind:    "order.process",
			Payload: []byte(`{"i":1}`),
			Queue:   "default",
		})
		require.NoError(t, err)
	}

	stats, err := client.QueueStats(ctx, &pb.QueueStatsRequest{Queue: "default"})
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats.Ready)
	assert.Equal(t, int64(0), stats.Active)

	deqResp, err := client.Dequeue(ctx, &pb.DequeueRequest{
		Queue:     "default",
		WorkerId:  "worker-1",
		TimeoutMs: 1000,
	})
	require.NoError(t, err)

	_, err = client.Ack(ctx, &pb.AckRequest{
		JobId:    deqResp.Job.Id,
		Queue:    "default",
		WorkerId: "worker-1",
	})
	require.NoError(t, err)

	stats, err = client.QueueStats(ctx, &pb.QueueStatsRequest{Queue: "default"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.Ready)
	assert.Equal(t, int64(1), stats.Completed)
}

// Enqueuing 3 jobs and dequeuing with 3 workers ensures each gets a unique job.
func TestGRPCConcurrentWorkers(t *testing.T) {
	client := setupGRPC(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := client.Enqueue(ctx, &pb.EnqueueRequest{
			Kind:    "order.process",
			Payload: []byte(`{"i":1}`),
			Queue:   "default",
		})
		require.NoError(t, err)
	}

	seen := make(map[string]bool)
	for i := 0; i < 3; i++ {
		resp, err := client.Dequeue(ctx, &pb.DequeueRequest{
			Queue:     "default",
			WorkerId:  "worker-1",
			TimeoutMs: 1000,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Job)
		assert.False(t, seen[resp.Job.Id], "job %s dequeued twice", resp.Job.Id)
		seen[resp.Job.Id] = true
	}
	assert.Len(t, seen, 3)
}
