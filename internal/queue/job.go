package queue

type JobRecord struct {
	ID          string `redis:"id"`
	Kind        string `redis:"kind"`
	Queue       string `redis:"queue"`
	Priority    int    `redis:"priority"`
	State       string `redis:"state"`
	Payload     []byte `redis:"payload"`
	Attempt     int    `redis:"attempt"`
	MaxRetries  int    `redis:"max_retries"`
	CreatedAt   int64  `redis:"created_at"`
	ScheduledAt int64  `redis:"scheduled_at"`
	StartedAt   int64  `redis:"started_at"`
	CompletedAt int64  `redis:"completed_at"`
	LastError   string `redis:"last_error"`
}
