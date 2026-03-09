// Command winter is the CLI for managing Winter task queues.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"
	"time"

	"github.com/kylegrahammatzen/winter"
	"github.com/kylegrahammatzen/winter/internal/config"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

var (
	cfgFile   string
	redisAddr string
)

func main() {
	root := &cobra.Command{
		Use:   "winter",
		Short: "Winter task queue CLI",
	}

	root.PersistentFlags().StringVar(&cfgFile, "config", "", "path to winter.yaml")
	root.PersistentFlags().StringVar(&redisAddr, "redis", "localhost:6379", "redis address")

	root.AddCommand(
		serverCmd(),
		enqueueCmd(),
		statusCmd(),
		jobsCmd(),
		deadCmd(),
		retryCmd(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func getConfig() *config.Config {
	if cfgFile != "" {
		cfg, err := config.Load(cfgFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return cfg
	}
	return &config.Config{
		Redis: config.RedisConfig{Addr: redisAddr},
	}
}

func connectRedis() (redis.UniversalClient, *queue.Queue) {
	cfg := getConfig()
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "redis connection failed: %v\n", err)
		os.Exit(1)
	}
	return rdb, queue.New(rdb)
}

func serverCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start the Winter server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := getConfig()

			redisCfg := winter.RedisConfig{
				Addr:     cfg.Redis.Addr,
				Password: cfg.Redis.Password,
				DB:       cfg.Redis.DB,
			}

			queues := []winter.QueueWeight{{Name: "default", Weight: 1}}
			if len(cfg.Queues) > 0 {
				queues = queues[:0]
				for name := range cfg.Queues {
					queues = append(queues, winter.QueueWeight{Name: name, Weight: 1})
				}
			}

			var cronEntries []winter.CronEntry
			for _, c := range cfg.Cron {
				cronEntries = append(cronEntries, winter.CronEntry{
					Name:     c.Name,
					Schedule: c.Schedule,
					Queue:    c.Queue,
					Kind:     c.Kind,
					Payload:  []byte(c.Payload),
				})
			}

			serverCfg := winter.ServerConfig{
				Concurrency:     cfg.Workers.Concurrency,
				Queues:          queues,
				PollInterval:    config.ParseDuration(cfg.Scheduler.PollInterval, 500*time.Millisecond),
				ShutdownTimeout: 30 * time.Second,
				Cron:            cronEntries,
				Logger:          slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
			}

			srv, err := winter.NewServer(redisCfg, serverCfg)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}

			return srv.Start()
		},
	}
	return cmd
}

func enqueueCmd() *cobra.Command {
	var (
		queueName string
		kind      string
		payload   string
		delay     time.Duration
		priority  int
	)

	cmd := &cobra.Command{
		Use:   "enqueue",
		Short: "Enqueue a job",
		RunE: func(cmd *cobra.Command, args []string) error {
			if kind == "" {
				return fmt.Errorf("--kind is required")
			}

			rdb, q := connectRedis()
			defer rdb.(interface{ Close() error }).Close()

			ctx := context.Background()

			job := &queue.JobRecord{
				ID:         fmt.Sprintf("cli-%d", time.Now().UnixNano()),
				Kind:       kind,
				Queue:      queueName,
				Priority:   priority,
				State:      "pending",
				Payload:    []byte(payload),
				MaxRetries: 3,
				CreatedAt:  time.Now().UnixMilli(),
			}

			if delay > 0 {
				job.ScheduledAt = time.Now().Add(delay).UnixMilli()
			}

			if err := q.Enqueue(ctx, job, "", 0); err != nil {
				return err
			}

			fmt.Printf("enqueued %s (kind=%s queue=%s)\n", job.ID, kind, queueName)
			return nil
		},
	}

	cmd.Flags().StringVar(&queueName, "queue", "default", "destination queue")
	cmd.Flags().StringVar(&kind, "kind", "", "task kind")
	cmd.Flags().StringVar(&payload, "payload", "{}", "JSON payload")
	cmd.Flags().DurationVar(&delay, "delay", 0, "schedule delay")
	cmd.Flags().IntVar(&priority, "priority", 5, "job priority")

	return cmd
}

func statusCmd() *cobra.Command {
	var queueName string

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show queue stats",
		RunE: func(cmd *cobra.Command, args []string) error {
			rdb, q := connectRedis()
			defer rdb.(interface{ Close() error }).Close()

			ctx := context.Background()

			stats, err := q.QueueStats(ctx, queueName)
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintf(w, "Queue\t%s\n", queueName)
			fmt.Fprintf(w, "Ready\t%d\n", stats["ready"])
			fmt.Fprintf(w, "Active\t%d\n", stats["active"])
			fmt.Fprintf(w, "Delayed\t%d\n", stats["delayed"])
			fmt.Fprintf(w, "Dead\t%d\n", stats["dead"])
			fmt.Fprintf(w, "Completed\t%d\n", stats["completed"])
			fmt.Fprintf(w, "Enqueued\t%d\n", stats["enqueued"])
			w.Flush()
			return nil
		},
	}

	cmd.Flags().StringVar(&queueName, "queue", "default", "queue to inspect")
	return cmd
}

func jobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs [job-id]",
		Short: "Show job details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rdb, q := connectRedis()
			defer rdb.(interface{ Close() error }).Close()

			ctx := context.Background()

			rec, err := q.GetJob(ctx, args[0])
			if err != nil {
				return err
			}
			if rec == nil {
				return fmt.Errorf("job %s not found", args[0])
			}

			data, _ := json.MarshalIndent(rec, "", "  ")
			fmt.Println(string(data))
			return nil
		},
	}
	return cmd
}

func deadCmd() *cobra.Command {
	var queueName string

	cmd := &cobra.Command{
		Use:   "dead",
		Short: "List or manage dead letter jobs",
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List dead letter jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			rdb, q := connectRedis()
			defer rdb.(interface{ Close() error }).Close()

			ctx := context.Background()
			records, err := q.ListDead(ctx, queueName, 0, 50)
			if err != nil {
				return err
			}

			if len(records) == 0 {
				fmt.Println("no dead jobs")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintf(w, "ID\tKind\tAttempt\tError\n")
			for _, rec := range records {
				errMsg := rec.LastError
				if len(errMsg) > 60 {
					errMsg = errMsg[:60] + "..."
				}
				fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", rec.ID, rec.Kind, rec.Attempt, errMsg)
			}
			w.Flush()
			return nil
		},
	}

	purgeCmd := &cobra.Command{
		Use:   "purge",
		Short: "Purge all dead letter jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			rdb, q := connectRedis()
			defer rdb.(interface{ Close() error }).Close()

			ctx := context.Background()
			purged, err := q.PurgeDead(ctx, queueName)
			if err != nil {
				return err
			}
			fmt.Printf("purged %d dead jobs from %s\n", purged, queueName)
			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&queueName, "queue", "default", "queue to inspect")
	cmd.AddCommand(listCmd, purgeCmd)
	return cmd
}

func retryCmd() *cobra.Command {
	var queueName string

	cmd := &cobra.Command{
		Use:   "retry [job-id]",
		Short: "Retry a dead job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			rdb, q := connectRedis()
			defer rdb.(interface{ Close() error }).Close()

			ctx := context.Background()
			err := q.RetryDead(ctx, queueName, args[0])
			if err != nil {
				return err
			}
			fmt.Printf("retried %s from %s dead queue\n", args[0], queueName)
			return nil
		},
	}

	cmd.Flags().StringVar(&queueName, "queue", "default", "queue containing the dead job")
	return cmd
}
