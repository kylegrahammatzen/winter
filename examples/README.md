# Examples

All examples require Redis running on `localhost:6379`. Start one with Docker:

```
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

## basic

Defines a task, enqueues it (immediate and delayed), and processes it. Demonstrates `Enqueue`, `HandleFunc`, and `In` for scheduling.

```
go run ./examples/basic
```

## workflows

Creates a Chain, Group, and Chord to show multi-job composition. The chain runs three steps in sequence, the group runs three builds in parallel, and the chord runs parallel builds then fires a deploy callback.

```
go run ./examples/workflows
```

## multi-queue

Split into a producer and a worker. The producer enqueues tasks that route to different queues via `TaskOptions`. The worker processes all three queues with weighted polling.

```
go run ./examples/multi-queue/producer
go run ./examples/multi-queue/worker
```

## cron

Registers periodic jobs with cron expressions. The server schedules a session cleanup every minute and a daily report at 9am.

```
go run ./examples/cron
```

## priority

Enqueues five tasks with different priority values and processes them with concurrency 1 to demonstrate ordering. Lower values are dequeued first.

```
go run ./examples/priority
```

## graceful-shutdown

Enqueues a job that runs to completion while the server is stopping. Press ctrl+c mid-run and the server drains the job, acking it on a shutdown-surviving context so a re-run does not reprocess it.

```
go run ./examples/graceful-shutdown
```
