# Winter

[![Go](https://img.shields.io/badge/Go-1.24+-00ADD8.svg)](https://go.dev/) [![Redis](https://img.shields.io/badge/Redis-7-red.svg)](https://redis.io/) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Winter is an open-source distributed task queue for Go, backed by Redis.

- Generics-first type safety (your struct is the task, no `[]byte` payloads)
- Priority queues with weighted dequeue
- Delayed and scheduled jobs
- Retries with configurable backoff (exponential, linear, fixed)
- Dead letter queue
- Unique job deduplication
- Worker-side control flow (reschedule, cancel, skip retry)
- Canonical log lines (one structured log per job with full context)

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

## Contributing

Winter is open source and welcomes contributions, issues, and feedback.

## License

MIT License ([LICENSE](LICENSE))
