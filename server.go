package winter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/kylegrahammatzen/winter/internal/ratelimit"
	"github.com/kylegrahammatzen/winter/internal/scheduler"
	"github.com/kylegrahammatzen/winter/internal/worker"
	"github.com/kylegrahammatzen/winter/internal/workflow"
	"github.com/redis/go-redis/v9"
)

// QueueWeight pairs a queue name with its relative polling weight.
type QueueWeight struct {
	Name   string
	Weight int
}

// Queues builds a weighted queue list from alternating name/weight pairs.
func Queues(args ...any) []QueueWeight {
	var queues []QueueWeight
	for i := 0; i < len(args)-1; i += 2 {
		name, _ := args[i].(string)
		weight, _ := args[i+1].(int)
		queues = append(queues, QueueWeight{Name: name, Weight: weight})
	}
	return queues
}

// CronEntry defines a periodic job to be scheduled by the server.
type CronEntry struct {
	Name     string
	Schedule string
	Queue    string
	Kind     string
	Payload  []byte
}

// ServerConfig controls concurrency, queue weights, poll interval, and logging.
// When StrictPriority is true, queues are polled in descending weight order
// and lower-priority queues are only checked when all higher ones are empty.
type ServerConfig struct {
	Concurrency     int
	Queues          []QueueWeight
	StrictPriority  bool
	Cron            []CronEntry
	PollInterval    time.Duration
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
}

// Server polls queues, dispatches jobs to registered handlers, and manages
// the worker lifecycle including graceful shutdown.
type Server struct {
	client     *Client
	cfg        ServerConfig
	workerID   string
	handlers   map[string]HandlerFn
	rateLimits map[string]Rate
	limiter    *ratelimit.Limiter
	middleware []Middleware
	hooks      serverHooks
	logger     *slog.Logger
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

type serverHooks struct {
	onStart    []HookFunc
	onComplete []HookFunc
	onError    []HookFunc
	onDead     []HookFunc
}

// NewServer connects to Redis and returns a server ready to process jobs.
func NewServer(redisCfg RedisConfig, cfg ServerConfig) (*Server, error) {
	client, err := NewClient(redisCfg)
	if err != nil {
		return nil, err
	}
	return newServer(client, cfg), nil
}

// NewServerFromRedis creates a server from an existing Redis connection.
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
	for i := range cfg.Queues {
		if cfg.Queues[i].Weight < 1 {
			cfg.Queues[i].Weight = 1
		}
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Server{
		client:     client,
		cfg:        cfg,
		workerID:   uuid.New().String(),
		handlers:   make(map[string]HandlerFn),
		rateLimits: make(map[string]Rate),
		limiter:    ratelimit.New(client.rdb),
		logger:     cfg.Logger,
	}
}

// Handle registers a typed handler for its task kind on the server.
// If the task implements TaskWithOptions and specifies a RateLimit,
// the server enforces a per-kind token bucket before dispatching.
func Handle[T Task](s *Server, h Handler[T]) {
	var zero T
	kind := zero.Kind()

	if tw, ok := any(zero).(TaskWithOptions); ok {
		if r := tw.Options().RateLimit; r.Max > 0 && r.Per > 0 {
			s.rateLimits[kind] = r
		}
	}

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
			raw:         rj,
		}

		return h.Work(ctx, job)
	}
}

// HandleFunc registers a function as a handler for its task kind.
func HandleFunc[T Task](s *Server, fn func(ctx context.Context, job *Job[T]) error) {
	Handle(s, HandlerFunc[T](fn))
}

// SetRateLimit configures a rate limit for the given task kind. This overrides
// any limit set via TaskWithOptions. Max is the number of tokens (jobs) allowed
// per interval.
func (s *Server) SetRateLimit(kind string, max int, per time.Duration) {
	s.rateLimits[kind] = Rate{Max: max, Per: per}
}

// OnStart registers a hook that fires when a job is picked up for processing,
// before the handler runs.
func (s *Server) OnStart(fn HookFunc) {
	s.hooks.onStart = append(s.hooks.onStart, fn)
}

// OnComplete registers a hook that fires after a job completes successfully.
func (s *Server) OnComplete(fn HookFunc) {
	s.hooks.onComplete = append(s.hooks.onComplete, fn)
}

// OnError registers a hook that fires when a job fails but will be retried.
func (s *Server) OnError(fn HookFunc) {
	s.hooks.onError = append(s.hooks.onError, fn)
}

// OnDead registers a hook that fires when a job exhausts retries and enters
// the dead letter queue.
func (s *Server) OnDead(fn HookFunc) {
	s.hooks.onDead = append(s.hooks.onDead, fn)
}

func (s *Server) fireHooks(ctx context.Context, hooks []HookFunc, event JobEvent) {
	for _, fn := range hooks {
		fn(ctx, event)
	}
}

// Use appends middleware to the server's handler chain.
func (s *Server) Use(mw ...Middleware) {
	s.middleware = append(s.middleware, mw...)
}

// Start begins polling queues and blocks until a shutdown signal is received.
// On shutdown it stops fetching new jobs, waits for in-flight jobs to finish
// (up to ShutdownTimeout), deregisters the worker, and closes the connection.
func (s *Server) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sig)

	s.logger.Info("winter: starting server",
		"worker_id", s.workerID,
		"concurrency", s.cfg.Concurrency,
		"queues", s.cfg.Queues,
	)

	// Initial heartbeat so recovery knows we exist.
	if err := s.client.queue.Heartbeat(ctx, s.workerID); err != nil {
		s.logger.Error("winter: initial heartbeat failed", "error", err)
	}

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

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.heartbeatLoop(ctx)
	}()

	queueNames := make([]string, len(s.cfg.Queues))
	for i, qw := range s.cfg.Queues {
		queueNames[i] = qw.Name
	}

	recovery := worker.NewRecovery(s.client.queue, worker.RecoveryConfig{
		Queues: queueNames,
		Logger: s.logger,
	})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		recovery.Run(ctx)
	}()

	if len(s.cfg.Cron) > 0 {
		entries := make([]scheduler.Entry, len(s.cfg.Cron))
		for i, ce := range s.cfg.Cron {
			entries[i] = scheduler.Entry{
				Name:     ce.Name,
				Schedule: ce.Schedule,
				Queue:    ce.Queue,
				Kind:     ce.Kind,
				Payload:  ce.Payload,
			}
		}
		cronSched, err := scheduler.NewCron(s.client.queue, s.client.rdb, entries, scheduler.CronConfig{
			Logger: s.logger,
		})
		if err != nil {
			cancel()
			return fmt.Errorf("winter: cron setup: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			cronSched.Run(ctx)
		}()
	}

	select {
	case <-sig:
		s.logger.Info("winter: received shutdown signal")
	case <-ctx.Done():
	}

	cancel()

	// Wait for in-flight jobs with a hard deadline.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("winter: all in-flight jobs drained")
	case <-time.After(s.cfg.ShutdownTimeout):
		s.logger.Warn("winter: shutdown timeout reached, force stopping")
	}

	// Deregister worker so recovery does not try to recover our jobs
	// that already completed.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := s.client.queue.DeregisterWorker(shutdownCtx, s.workerID); err != nil {
		s.logger.Error("winter: deregister worker failed", "error", err)
	}

	s.logger.Info("winter: server stopped", "worker_id", s.workerID)
	return nil
}

// Stop cancels the server context and begins graceful shutdown.
func (s *Server) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

// pollLoop cycles through the queue list, dequeuing jobs and dispatching
// them to handlers bounded by the concurrency semaphore. In strict mode
// higher-weight queues are always drained before lower ones.
func (s *Server) pollLoop(ctx context.Context, sem chan struct{}) {
	if s.cfg.StrictPriority {
		s.pollStrict(ctx, sem)
	} else {
		s.pollWeighted(ctx, sem)
	}
}

func (s *Server) pollWeighted(ctx context.Context, sem chan struct{}) {
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

func (s *Server) pollStrict(ctx context.Context, sem chan struct{}) {
	queueNames := s.buildStrictQueues()

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

			// Restart from the highest-priority queue after each successful dequeue.
			break
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

// heartbeatLoop sends periodic heartbeats and extends leases for in-flight jobs
// so the recovery goroutine does not reclaim them.
func (s *Server) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.client.queue.Heartbeat(ctx, s.workerID); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.logger.Error("winter: heartbeat failed", "error", err)
			}
		}
	}
}

// promoteLoop periodically moves delayed jobs whose scheduled time has passed
// back into the ready set.
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

// processJob runs the middleware chain and handler for a single job, then acks,
// nacks, reschedules, or cancels it based on the handler's return value. Every
// job emits exactly one canonical log line with all context.
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

	if rl, has := s.rateLimits[rec.Kind]; has {
		res, rlErr := s.limiter.Allow(ctx, rec.Kind, rl.Max, rl.Per)
		if rlErr != nil {
			s.logger.Error("winter: rate limit check failed", "kind", rec.Kind, "error", rlErr)
		} else if !res.Allowed {
			delay := res.RetryIn
			if delay < 50*time.Millisecond {
				delay = 50 * time.Millisecond
			}
			event = append(event, "outcome", "rate_limited", "retry_in_ms", delay.Milliseconds(), "duration_ms", time.Since(start).Milliseconds())
			s.logger.Info("winter: job processed", event...)
			_ = s.client.queue.RescheduleJob(ctx, rec.Queue, rec.ID, s.workerID, time.Now().Add(delay))
			return
		}
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

	je := JobEvent{ID: rec.ID, Kind: rec.Kind, Queue: rec.Queue, Attempt: rec.Attempt}
	s.fireHooks(ctx, s.hooks.onStart, je)

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
		if len(rj.result) > 0 {
			if resErr := s.client.queue.SetResult(ctx, rec.ID, rj.result, 7*24*time.Hour); resErr != nil {
				s.logger.Error("winter: store result failed", "job_id", rec.ID, "error", resErr)
			}
		}
		if rec.WorkflowID != "" {
			mgr := workflow.NewManager(s.client.queue, s.client.rdb)
			if wfErr := mgr.OnJobCompleted(ctx, rec.WorkflowID, rec.ID); wfErr != nil {
				s.logger.Error("winter: workflow advance error", "workflow_id", rec.WorkflowID, "error", wfErr)
			}
		}
		s.fireHooks(ctx, s.hooks.onComplete, je)
		event = append(event, "outcome", "completed")
		s.logger.Info("winter: job processed", event...)
		return
	}

	je.Err = err
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
		s.fireHooks(ctx, s.hooks.onDead, je)
		if rec.WorkflowID != "" {
			mgr := workflow.NewManager(s.client.queue, s.client.rdb)
			if wfErr := mgr.OnJobFailed(ctx, rec.WorkflowID, rec.ID); wfErr != nil {
				s.logger.Error("winter: workflow failure error", "workflow_id", rec.WorkflowID, "error", wfErr)
			}
		}
		s.logger.Warn("winter: job processed", event...)
	} else {
		s.fireHooks(ctx, s.hooks.onError, je)
		s.logger.Info("winter: job processed", event...)
	}
}

// buildWeightedQueues expands queue weights into a flat list so higher-weight
// queues appear more often in the poll rotation.
func (s *Server) buildWeightedQueues() []string {
	var queues []string
	for _, qw := range s.cfg.Queues {
		for range qw.Weight {
			queues = append(queues, qw.Name)
		}
	}
	return queues
}

// buildStrictQueues returns queue names sorted by weight descending for
// strict priority polling where higher-weight queues are always drained first.
func (s *Server) buildStrictQueues() []string {
	sorted := make([]QueueWeight, len(s.cfg.Queues))
	copy(sorted, s.cfg.Queues)
	slices.SortFunc(sorted, func(a, b QueueWeight) int {
		return b.Weight - a.Weight
	})
	names := make([]string, len(sorted))
	for i, qw := range sorted {
		names[i] = qw.Name
	}
	return names
}
