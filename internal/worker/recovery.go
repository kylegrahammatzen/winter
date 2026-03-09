// Package worker implements background goroutines for lease recovery and
// worker lifecycle management.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/kylegrahammatzen/winter/internal/queue"
)

// RecoveryConfig controls how often and how many expired leases are recovered per sweep.
type RecoveryConfig struct {
	Interval time.Duration
	Queues   []string
	Limit    int
	Logger   *slog.Logger
}

// Recovery periodically scans the lease ZSET for expired entries and moves
// those jobs back to the ready set so they can be picked up by another worker.
type Recovery struct {
	q   *queue.Queue
	cfg RecoveryConfig
}

// NewRecovery creates a Recovery with sensible defaults for any zero-valued config fields.
func NewRecovery(q *queue.Queue, cfg RecoveryConfig) *Recovery {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Recovery{q: q, cfg: cfg}
}

// Run starts the recovery loop and blocks until ctx is cancelled.
func (r *Recovery) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

func (r *Recovery) sweep(ctx context.Context) {
	for _, queueName := range r.cfg.Queues {
		recovered, err := r.q.RecoverExpiredLeases(ctx, queueName, r.cfg.Limit)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.cfg.Logger.Error("winter: lease recovery error", "queue", queueName, "error", err)
			continue
		}
		if len(recovered) > 0 {
			r.cfg.Logger.Warn("winter: recovered expired leases",
				"queue", queueName,
				"count", len(recovered),
				"job_ids", recovered,
			)
		}
	}
}
