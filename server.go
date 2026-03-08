package winter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
)

type QueueWeight struct {
	Name   string
	Weight int
}

func Queues(args ...interface{}) []QueueWeight {
	var queues []QueueWeight
	for i := 0; i < len(args)-1; i += 2 {
		name, _ := args[i].(string)
		weight, _ := args[i+1].(int)
		queues = append(queues, QueueWeight{Name: name, Weight: weight})
	}
	return queues
}

type ServerConfig struct {
	Concurrency  int
	Queues       []QueueWeight
	PollInterval time.Duration
	Logger       *slog.Logger
}

type handlerEntry struct {
	kind    string
	handler HandlerFn
}

type Server struct {
	client     *Client
	cfg        ServerConfig
	workerID   string
	handlers   map[string]HandlerFn
	middleware []Middleware
	logger     *slog.Logger
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewServer(redisCfg RedisConfig, cfg ServerConfig) (*Server, error) {
	client, err := NewClient(redisCfg)
	if err != nil {
		return nil, err
	}
	return newServer(client, cfg), nil
}

func NewServerFromRedis(rdb redis.UniversalClient, cfg ServerConfig) *Server {
	client := NewClientFromRedis(rdb)
	return newServer(client, cfg)
}

func newServer(client *Client, cfg ServerConfig) *Server {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []QueueWeight{{Name: "default", Weight: 1}}
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Server{
		client:   client,
		cfg:      cfg,
		workerID: uuid.New().String(),
		handlers: make(map[string]HandlerFn),
		logger:   cfg.Logger,
	}
}

func Handle[T Task](s *Server, h Handler[T]) {
	var zero T
	kind := zero.Kind()

	s.handlers[kind] = func(ctx context.Context, rj *rawJob) error {
		var args T
		if err := json.Unmarshal(rj.Payload, &args); err != nil {
			return fmt.Errorf("winter: unmarshal %s: %w", kind, err)
		}

		job := &Job[T]{
			ID:          rj.ID,
			Args:        args,
			Kind:        rj.Kind,
			Queue:       rj.Queue,
			Priority:    rj.Priority,
			State:       rj.State,
			Attempt:     rj.Attempt,
			MaxRetries:  rj.MaxRetries,
			CreatedAt:   time.UnixMilli(rj.CreatedAt),
			ScheduledAt: time.UnixMilli(rj.ScheduledAt),
			StartedAt:   time.UnixMilli(rj.StartedAt),
			LastError:   rj.LastError,
		}

		return h.Work(ctx, job)
	}
}

func HandleFunc[T Task](s *Server, fn func(ctx context.Context, job *Job[T]) error) {
	Handle(s, HandlerFunc[T](fn))
}

func (s *Server) Use(mw ...Middleware) {
	s.middleware = append(s.middleware, mw...)
}

func (s *Server) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	s.logger.Info("winter: starting server",
		"worker_id", s.workerID,
		"concurrency", s.cfg.Concurrency,
		"queues", s.cfg.Queues,
	)

	sem := make(chan struct{}, s.cfg.Concurrency)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.pollLoop(ctx, sem)
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.promoteLoop(ctx)
	}()

	select {
	case <-sig:
		s.logger.Info("winter: received shutdown signal")
	case <-ctx.Done():
	}

	cancel()
	s.wg.Wait()
	s.logger.Info("winter: server stopped")
	return nil
}

func (s *Server) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Server) pollLoop(ctx context.Context, sem chan struct{}) {
	queueNames := s.buildWeightedQueues()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetched := false
		for _, queueName := range queueNames {
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}

			rec, err := s.client.queue.Dequeue(ctx, queueName, s.workerID)
			if err != nil {
				s.logger.Error("winter: dequeue error", "queue", queueName, "error", err)
				<-sem
				continue
			}

			if rec == nil {
				<-sem
				continue
			}

			fetched = true
			s.wg.Add(1)
			go func(rec *queue.JobRecord) {
				defer s.wg.Done()
				defer func() { <-sem }()
				s.processJob(ctx, rec)
			}(rec)
		}

		if !fetched {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.cfg.PollInterval):
			}
		}
	}
}

func (s *Server) promoteLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, qw := range s.cfg.Queues {
				promoted, err := s.client.queue.Promote(ctx, qw.Name, 100)
				if err != nil && !errors.Is(err, context.Canceled) {
					s.logger.Error("winter: promote error", "queue", qw.Name, "error", err)
				}
				if promoted > 0 {
					s.logger.Debug("winter: promoted delayed jobs", "queue", qw.Name, "count", promoted)
				}
			}
		}
	}
}

func (s *Server) processJob(ctx context.Context, rec *queue.JobRecord) {
	start := time.Now()

	event := []any{
		"job_id", rec.ID,
		"kind", rec.Kind,
		"queue", rec.Queue,
		"priority", rec.Priority,
		"attempt", rec.Attempt,
		"max_retries", rec.MaxRetries,
		"worker_id", s.workerID,
	}

	handler, ok := s.handlers[rec.Kind]
	if !ok {
		event = append(event, "outcome", "dead", "duration_ms", time.Since(start).Milliseconds(), "error", "no handler registered")
		s.logger.Error("winter: job processed", event...)
		_, _ = s.client.queue.Nack(ctx, rec.Queue, rec.ID, s.workerID, "no handler registered", 0, true)
		return
	}

	rj := &rawJob{
		ID:          rec.ID,
		Kind:        rec.Kind,
		Queue:       rec.Queue,
		Priority:    rec.Priority,
		State:       JobState(rec.State),
		Attempt:     rec.Attempt,
		MaxRetries:  rec.MaxRetries,
		Payload:     rec.Payload,
		CreatedAt:   rec.CreatedAt,
		ScheduledAt: rec.ScheduledAt,
		StartedAt:   rec.StartedAt,
		LastError:   rec.LastError,
	}

	fn := handler
	for i := len(s.middleware) - 1; i >= 0; i-- {
		fn = s.middleware[i](fn)
	}

	jobCtx := withClient(ctx, s.client)
	err := fn(jobCtx, rj)
	durationMs := time.Since(start).Milliseconds()
	event = append(event, "duration_ms", durationMs)

	if err == nil {
		if ackErr := s.client.queue.Ack(ctx, rec.Queue, rec.ID, s.workerID); ackErr != nil {
			event = append(event, "outcome", "ack_error", "error", ackErr.Error())
			s.logger.Error("winter: job processed", event...)
			return
		}
		event = append(event, "outcome", "completed")
		s.logger.Info("winter: job processed", event...)
		return
	}

	event = append(event, "error", err.Error())

	if delay, ok := IsReschedule(err); ok {
		event = append(event, "outcome", "rescheduled", "reschedule_delay_ms", delay.Milliseconds())
		if rsErr := s.client.queue.RescheduleJob(ctx, rec.Queue, rec.ID, s.workerID, time.Now().Add(delay)); rsErr != nil {
			event = append(event, "reschedule_error", rsErr.Error())
			s.logger.Error("winter: job processed", event...)
			return
		}
		s.logger.Info("winter: job processed", event...)
		return
	}

	if reason, ok := IsCancel(err); ok {
		event = append(event, "outcome", "cancelled", "cancel_reason", reason)
		if cErr := s.client.queue.CancelJob(ctx, rec.Queue, rec.ID, s.workerID, reason); cErr != nil {
			event = append(event, "cancel_error", cErr.Error())
			s.logger.Error("winter: job processed", event...)
			return
		}
		s.logger.Warn("winter: job processed", event...)
		return
	}

	skipRetry := errors.Is(err, ErrSkipRetry)
	backoffMs := int64(0)
	if !skipRetry {
		backoffMs = int64(Exponential(time.Second).Next(rec.Attempt) / time.Millisecond)
	}

	result, nackErr := s.client.queue.Nack(ctx, rec.Queue, rec.ID, s.workerID, err.Error(), backoffMs, skipRetry)
	if nackErr != nil {
		event = append(event, "outcome", "nack_error", "nack_error_detail", nackErr.Error())
		s.logger.Error("winter: job processed", event...)
		return
	}

	event = append(event, "outcome", result, "backoff_ms", backoffMs)
	if result == "dead" {
		s.logger.Warn("winter: job processed", event...)
	} else {
		s.logger.Info("winter: job processed", event...)
	}
}

func (s *Server) buildWeightedQueues() []string {
	var queues []string
	for _, qw := range s.cfg.Queues {
		for range qw.Weight {
			queues = append(queues, qw.Name)
		}
	}
	return queues
}
