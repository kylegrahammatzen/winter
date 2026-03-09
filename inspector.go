package winter

import (
	"context"
	"time"

	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
)

// Inspector provides read and management operations for queues and dead
// letter jobs without requiring a running server.
type Inspector struct {
	client *Client
}

// NewInspector connects to Redis and returns an inspector.
func NewInspector(cfg RedisConfig) (*Inspector, error) {
	client, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Inspector{client: client}, nil
}

// NewInspectorFromRedis creates an inspector from an existing Redis connection.
func NewInspectorFromRedis(rdb redis.UniversalClient) *Inspector {
	return &Inspector{client: NewClientFromRedis(rdb)}
}

// QueueInfo holds a snapshot of queue depths and counters.
type QueueInfo struct {
	Name      string
	Ready     int64
	Active    int64
	Delayed   int64
	Dead      int64
	Completed int64
	Enqueued  int64
}

// Queue returns a snapshot of depths and counters for the named queue.
func (i *Inspector) Queue(ctx context.Context, name string) (*QueueInfo, error) {
	stats, err := i.client.queue.QueueStats(ctx, name)
	if err != nil {
		return nil, err
	}
	return &QueueInfo{
		Name:      name,
		Ready:     stats["ready"],
		Active:    stats["active"],
		Delayed:   stats["delayed"],
		Dead:      stats["dead"],
		Completed: stats["completed"],
		Enqueued:  stats["enqueued"],
	}, nil
}

// DeadJob is a dead letter job with its metadata exposed for inspection.
type DeadJob struct {
	ID          string
	Kind        string
	Queue       string
	Payload     []byte
	Attempt     int
	MaxRetries  int
	LastError   string
	CreatedAt   time.Time
	CompletedAt time.Time
}

func deadJobFromRecord(rec *queue.JobRecord) *DeadJob {
	return &DeadJob{
		ID:          rec.ID,
		Kind:        rec.Kind,
		Queue:       rec.Queue,
		Payload:     rec.Payload,
		Attempt:     rec.Attempt,
		MaxRetries:  rec.MaxRetries,
		LastError:   rec.LastError,
		CreatedAt:   time.UnixMilli(rec.CreatedAt),
		CompletedAt: time.UnixMilli(rec.CompletedAt),
	}
}

// Dead returns a paginated list of dead letter jobs for the named queue.
func (i *Inspector) Dead(ctx context.Context, queueName string, offset, limit int64) ([]*DeadJob, error) {
	records, err := i.client.queue.ListDead(ctx, queueName, offset, limit)
	if err != nil {
		return nil, err
	}
	jobs := make([]*DeadJob, len(records))
	for idx, rec := range records {
		jobs[idx] = deadJobFromRecord(rec)
	}
	return jobs, nil
}

// PeekDead returns the first dead letter job without removing it.
func (i *Inspector) PeekDead(ctx context.Context, queueName string) (*DeadJob, error) {
	rec, err := i.client.queue.PeekDead(ctx, queueName)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return deadJobFromRecord(rec), nil
}

// Retry removes a job from the dead letter queue, resets its attempt counter,
// and re-enqueues it to the ready set.
func (i *Inspector) Retry(ctx context.Context, queueName string, jobID string) error {
	return i.client.queue.RetryDead(ctx, queueName, jobID)
}

// PurgeDead removes all jobs from the dead letter queue and deletes their data.
func (i *Inspector) PurgeDead(ctx context.Context, queueName string) (int64, error) {
	return i.client.queue.PurgeDead(ctx, queueName)
}

// DeadCount returns the number of jobs in the dead letter queue.
func (i *Inspector) DeadCount(ctx context.Context, queueName string) (int64, error) {
	return i.client.queue.DeadCount(ctx, queueName)
}

// Pause stops workers from dequeuing jobs from the named queue.
func (i *Inspector) Pause(ctx context.Context, queueName string) error {
	return i.client.queue.Pause(ctx, queueName)
}

// Resume allows workers to dequeue jobs from a previously paused queue.
func (i *Inspector) Resume(ctx context.Context, queueName string) error {
	return i.client.queue.Resume(ctx, queueName)
}

// JobResult returns the stored result bytes for a completed job, or nil if
// no result was stored.
func (i *Inspector) JobResult(ctx context.Context, jobID string) ([]byte, error) {
	return i.client.queue.GetResult(ctx, jobID)
}

// Close shuts down the underlying Redis connection.
func (i *Inspector) Close() error {
	return i.client.Close()
}
