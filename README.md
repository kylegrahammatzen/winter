# Winter

[![Go](https://img.shields.io/badge/Go-1.24+-00ADD8.svg)](https://go.dev/) [![Redis](https://img.shields.io/badge/Redis-7-red.svg)](https://redis.io/) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Winter is an open-source distributed task queue for Go, backed by Redis. With Winter, you get:

- Generics-first type safety (your struct is the task, no `[]byte` payloads)
- Workflow primitives (Chain, Group, Chord)
- Standalone gRPC server for language-agnostic workers
- Priority queues with weighted dequeue
- Delayed, scheduled, and cron jobs
- Retries with configurable backoff (exponential, linear, fixed)
- Dead letter queue with inspection and retry
- Unique job deduplication
- Lease-based worker recovery
- Rate limiting with Redis-backed token bucket
- Result storage with typed retrieval
- Lifecycle hooks (OnStart, OnComplete, OnError, OnDead)
- CLI for queue management

## Install

```bash
go get github.com/kylegrahammatzen/winter
```

## Usage

### Define a task

```go
type SendEmail struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

func (SendEmail) Kind() string { return "email.send" }
```

### Enqueue

```go
client, _ := winter.NewClient(winter.RedisConfig{Addr: "localhost:6379"})

winter.Enqueue(client, ctx, SendEmail{
    To:      "user@example.com",
    Subject: "Welcome",
    Body:    "Thanks for signing up.",
})

// With options
winter.Enqueue(client, ctx, SendEmail{To: "user@example.com", Subject: "Reminder"},
    winter.In(5 * time.Minute),
    winter.Priority(0),
    winter.Unique(1 * time.Hour),
)
```

### Process

```go
server, _ := winter.NewServer(
    winter.RedisConfig{Addr: "localhost:6379"},
    winter.ServerConfig{
        Concurrency: 20,
        Queues:      winter.Queues("critical", 6, "default", 3, "low", 1),
    },
)

winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[SendEmail]) error {
    return smtp.Send(job.Args.To, job.Args.Subject, job.Args.Body)
})

server.Use(winter.Recover())
server.Start()
```

### Workflows

Chain runs tasks sequentially. Each step only starts after the previous one completes.

```go
winter.Chain(client, ctx, []winter.Task{
    ProcessOrder{OrderID: "abc"},
    GenerateInvoice{OrderID: "abc"},
    SendReceipt{OrderID: "abc"},
})
```

Group runs all tasks in parallel and tracks completion.

```go
winter.Group(client, ctx, []winter.Task{
    SyncInventory{ProductIDs: []string{"sku-1"}},
    SyncInventory{ProductIDs: []string{"sku-2"}},
    SyncInventory{ProductIDs: []string{"sku-3"}},
})
```

Chord runs all header tasks in parallel, then fires a callback when all are done.

```go
winter.Chord(client, ctx,
    []winter.Task{
        Build{OS: "linux", Arch: "amd64"},
        Build{OS: "darwin", Arch: "arm64"},
    },
    Deploy{Version: "1.4.0", Env: "staging"},
)
```

### Cron Jobs

Register periodic jobs in the server config.

```go
server, _ := winter.NewServer(redisCfg, winter.ServerConfig{
    Concurrency: 20,
    Queues:      winter.Queues("default", 1),
    Cron: []winter.CronEntry{
        {Name: "daily-cleanup", Schedule: "0 2 * * *", Kind: "cleanup.run", Queue: "maintenance"},
    },
})
```

### Worker-side control flow

```go
func (h *Handler) Work(ctx context.Context, job *winter.Job[SyncInventory]) error {
    resp, err := callExternalAPI()
    if isRateLimited(err) {
        return winter.Reschedule(30 * time.Second)
    }
    if isInvalidData(err) {
        return fmt.Errorf("bad data: %w", winter.SkipRetry)
    }
    if shouldAbandon(err) {
        return winter.Cancel("resource deleted")
    }

    client := winter.ClientFromContext(ctx)
    winter.Enqueue(client, ctx, NotifyUser{UserID: job.Args.UserID})

    return nil
}
```

### Rate Limiting

Limit how fast tasks of a given kind are processed using a Redis-backed token bucket.

```go
// On the task itself
func (SendEmail) Options() winter.TaskOptions {
    return winter.TaskOptions{
        RateLimit: &winter.RateLimit{Max: 10, Per: time.Second},
    }
}

// Or set it on the server directly
server.SetRateLimit("email.send", 10, time.Second)
```

When the rate limit is exceeded, the job is re-enqueued after the bucket refills.

### Result Storage

Store results from completed jobs and retrieve them later.

```go
// Inside a handler, set a result on the job
winter.HandleFunc(server, func(ctx context.Context, job *winter.Job[ProcessImage]) error {
    url := resize(job.Args.Path)
    job.SetResult(map[string]string{"url": url})
    return nil
})

// Later, poll for the result
var result map[string]string
err := winter.WaitForResult(client, ctx, jobID, &result)
```

Results are stored in Redis with a 7-day TTL.

### Lifecycle Hooks

Register callbacks that fire at specific points in a job's lifecycle.

```go
server.OnStart(func(ctx context.Context, ev winter.JobEvent) {
    log.Printf("starting %s (attempt %d)", ev.Kind, ev.Attempt)
})
server.OnComplete(func(ctx context.Context, ev winter.JobEvent) {
    metrics.Increment("jobs.completed")
})
server.OnError(func(ctx context.Context, ev winter.JobEvent) {
    alerting.Notify(ev.Err)
})
server.OnDead(func(ctx context.Context, ev winter.JobEvent) {
    log.Printf("job %s is dead: %v", ev.ID, ev.Err)
})
```

### Task options

Tasks can declare their own defaults by implementing `TaskWithOptions`:

```go
func (SyncInventory) Options() winter.TaskOptions {
    return winter.TaskOptions{
        Queue:      "low",
        MaxRetries: 10,
        Backoff:    winter.Exponential(2 * time.Second),
    }
}
```

### Inspector

Inspect and manage queues and dead letter jobs without a running server.

```go
inspector, _ := winter.NewInspector(winter.RedisConfig{Addr: "localhost:6379"})

info, _ := inspector.Queue(ctx, "default")
dead, _ := inspector.Dead(ctx, "default", 0, 50)
inspector.Retry(ctx, "default", "job-id")
inspector.PurgeDead(ctx, "default")
inspector.Pause(ctx, "default")
inspector.Resume(ctx, "default")
```

## gRPC Server

Winter includes a standalone gRPC server that lets non-Go workers (Python, TypeScript, etc.) interact with the queue.

```bash
# Start with defaults
go run ./cmd/server

# Start with config
go run ./cmd/server --config winter.yaml
```

The server exposes Enqueue, Dequeue, Ack, Nack, Heartbeat, GetJob, and QueueStats RPCs. Health checking and reflection are enabled by default.

## CLI

```bash
# Start the server
winter server --config winter.yaml

# Enqueue a job
winter enqueue --kind "order.process" --payload '{"order_id":"abc"}' --queue emails

# Check queue status
winter status --queue default

# Inspect a job
winter jobs <job-id>

# List dead letter jobs
winter dead list --queue default

# Retry a dead job
winter retry <job-id> --queue default

# Purge dead letter queue
winter dead purge --queue default
```

## Configuration

Winter can be configured via YAML:

```yaml
redis:
  addr: "localhost:6379"
  password: ""
  db: 0

server:
  grpc_port: "50051"
  log_level: "info"

queues:
  emails:
    max_retries: 5
    backoff: "exponential"
    backoff_base: "1s"
  payments:
    max_retries: 10
    backoff: "exponential"
    backoff_base: "500ms"

workers:
  concurrency: 20
  heartbeat_interval: "15s"
  recovery_interval: "30s"

cron:
  - name: "daily-cleanup"
    schedule: "0 2 * * *"
    queue: "maintenance"
    kind: "cleanup.run"
    payload: '{"task":"cleanup"}'
```

## Testing

Winter includes the `wintertest` package for testing your workers without Redis:

```go
func TestSignupEnqueuesWelcome(t *testing.T) {
    client := wintertest.NewClient(t)

    handleSignup(client, User{ID: 42})

    wintertest.RequireEnqueued(t, client, tasks.SendWelcome{UserID: 42})
    wintertest.RequireEnqueuedN(t, client, "notification.send", 2)
}
```

```bash
# Unit tests
go test ./...

# Integration tests (requires Docker)
go test -tags=integration ./test/integration/...

# Benchmarks
go test -bench=. -benchmem ./internal/queue/...
```

## Contributing

Winter is open source and welcomes contributions, issues, and feedback.

## License

MIT License ([LICENSE](LICENSE))
