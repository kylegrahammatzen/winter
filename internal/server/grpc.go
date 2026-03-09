// Package server implements the gRPC QueueService backed by the internal queue package.
package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/kylegrahammatzen/winter/internal/queue"
	pb "github.com/kylegrahammatzen/winter/proto/winter/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the winterv1.QueueServiceServer interface.
type GRPCServer struct {
	pb.UnimplementedQueueServiceServer
	q      *queue.Queue
	logger *slog.Logger
}

// NewGRPCServer creates a gRPC server backed by the given queue.
func NewGRPCServer(q *queue.Queue, logger *slog.Logger) *GRPCServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &GRPCServer{q: q, logger: logger}
}

// Enqueue creates a new job and inserts it into the queue.
func (s *GRPCServer) Enqueue(ctx context.Context, req *pb.EnqueueRequest) (*pb.EnqueueResponse, error) {
	if req.Kind == "" {
		return nil, status.Error(codes.InvalidArgument, "kind is required")
	}

	queueName := req.Queue
	if queueName == "" {
		queueName = "default"
	}

	maxRetries := int(req.MaxRetries)
	if maxRetries == 0 {
		maxRetries = 3
	}

	id := uuid.New().String()
	now := time.Now().UnixMilli()

	job := &queue.JobRecord{
		ID:          id,
		Kind:        req.Kind,
		Queue:       queueName,
		Priority:    int(req.Priority),
		State:       "pending",
		Payload:     req.Payload,
		MaxRetries:  maxRetries,
		CreatedAt:   now,
		ScheduledAt: req.ScheduledAt,
	}

	var uniqueKey string
	var uniquePeriod time.Duration
	if req.UniquePeriodMs > 0 {
		uniquePeriod = time.Duration(req.UniquePeriodMs) * time.Millisecond
		hash := sha256.Sum256(req.Payload)
		uniqueKey = fmt.Sprintf("%s:%x", req.Kind, hash)
	}

	if err := s.q.Enqueue(ctx, job, uniqueKey, uniquePeriod); err != nil {
		if errors.Is(err, queue.ErrDuplicate) {
			return nil, status.Error(codes.AlreadyExists, "duplicate job")
		}
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}

	return &pb.EnqueueResponse{JobId: id}, nil
}

// Dequeue polls for a ready job with jittered sleep until one is available or
// the timeout expires.
func (s *GRPCServer) Dequeue(ctx context.Context, req *pb.DequeueRequest) (*pb.DequeueResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)

	for {
		rec, err := s.q.Dequeue(ctx, req.Queue, req.WorkerId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "dequeue: %v", err)
		}

		if rec != nil {
			return &pb.DequeueResponse{Job: recordToProto(rec)}, nil
		}

		if time.Now().After(deadline) {
			return &pb.DequeueResponse{}, nil
		}

		// Jittered sleep between 100ms and 300ms.
		jitter := 100 + rand.IntN(200)
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.Canceled, "context cancelled")
		case <-time.After(time.Duration(jitter) * time.Millisecond):
		}
	}
}

// Ack marks a job as completed.
func (s *GRPCServer) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	if req.JobId == "" || req.Queue == "" || req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id, queue, and worker_id are required")
	}

	if err := s.q.Ack(ctx, req.Queue, req.JobId, req.WorkerId); err != nil {
		return nil, status.Errorf(codes.Internal, "ack: %v", err)
	}

	return &pb.AckResponse{}, nil
}

// Nack records a job failure and either retries or sends to the dead letter queue.
func (s *GRPCServer) Nack(ctx context.Context, req *pb.NackRequest) (*pb.NackResponse, error) {
	if req.JobId == "" || req.Queue == "" || req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id, queue, and worker_id are required")
	}

	backoffMs := int64(time.Second / time.Millisecond)
	result, err := s.q.Nack(ctx, req.Queue, req.JobId, req.WorkerId, req.Error, backoffMs, req.SkipRetry)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "nack: %v", err)
	}

	return &pb.NackResponse{Outcome: result}, nil
}

// Heartbeat updates the worker's last-seen timestamp.
func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	if err := s.q.Heartbeat(ctx, req.WorkerId); err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
	}

	return &pb.HeartbeatResponse{}, nil
}

// GetJob fetches the full details of a job by ID.
func (s *GRPCServer) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	rec, err := s.q.GetJob(ctx, req.JobId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job: %v", err)
	}
	if rec == nil {
		return nil, status.Error(codes.NotFound, "job not found")
	}

	return &pb.GetJobResponse{Job: recordToProto(rec)}, nil
}

// QueueStats returns a snapshot of queue depths and cumulative counters.
func (s *GRPCServer) QueueStats(ctx context.Context, req *pb.QueueStatsRequest) (*pb.QueueStatsResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}

	stats, err := s.q.QueueStats(ctx, req.Queue)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "queue stats: %v", err)
	}

	return &pb.QueueStatsResponse{
		Queue:     req.Queue,
		Ready:     stats["ready"],
		Active:    stats["active"],
		Delayed:   stats["delayed"],
		Dead:      stats["dead"],
		Completed: stats["completed"],
		Failed:    stats["failed"],
	}, nil
}

func recordToProto(rec *queue.JobRecord) *pb.Job {
	return &pb.Job{
		Id:          rec.ID,
		Kind:        rec.Kind,
		Payload:     rec.Payload,
		Queue:       rec.Queue,
		Priority:    int32(rec.Priority),
		State:       rec.State,
		Attempt:     int32(rec.Attempt),
		MaxRetries:  int32(rec.MaxRetries),
		CreatedAt:   rec.CreatedAt,
		ScheduledAt: rec.ScheduledAt,
		StartedAt:   rec.StartedAt,
		CompletedAt: rec.CompletedAt,
		LastError:   rec.LastError,
	}
}
