// Command server runs the standalone Winter gRPC server.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kylegrahammatzen/winter/internal/config"
	"github.com/kylegrahammatzen/winter/internal/queue"
	"github.com/kylegrahammatzen/winter/internal/scheduler"
	"github.com/kylegrahammatzen/winter/internal/server"
	"github.com/kylegrahammatzen/winter/internal/worker"
	pb "github.com/kylegrahammatzen/winter/proto/winter/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

func main() {
	configPath := flag.String("config", "", "path to winter.yaml config file")
	flag.Parse()

	cfg := defaultConfig()
	if *configPath != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			slog.Error("failed to load config", "error", err)
			os.Exit(1)
		}
		cfg = loaded
	}

	var logLevel slog.Level
	switch cfg.Server.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	redisAddr := cfg.Redis.Addr
	grpcPort := cfg.Server.GRPCPort

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Error("redis connection failed", "addr", redisAddr, "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	q := queue.New(rdb)
	grpcSrv := server.NewGRPCServer(q, logger)

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			server.RecoveryInterceptor(),
			server.LoggingInterceptor(logger),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	pb.RegisterQueueServiceServer(s, grpcSrv)

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	healthSrv.SetServingStatus("winter.v1.QueueService", healthpb.HealthCheckResponse_SERVING)

	reflection.Register(s)

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Error("failed to listen", "port", grpcPort, "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start lease recovery for configured queues.
	queueNames := queueNamesFromConfig(cfg)
	recovery := worker.NewRecovery(q, worker.RecoveryConfig{
		Queues:   queueNames,
		Interval: config.ParseDuration(cfg.Workers.RecoveryInterval, 30*time.Second),
		Logger:   logger,
	})
	go recovery.Run(ctx)

	// Start cron scheduler if entries are configured.
	if len(cfg.Cron) > 0 {
		entries := make([]scheduler.Entry, len(cfg.Cron))
		for i, c := range cfg.Cron {
			entries[i] = scheduler.Entry{
				Name:     c.Name,
				Schedule: c.Schedule,
				Queue:    c.Queue,
				Kind:     c.Kind,
				Payload:  []byte(c.Payload),
			}
		}
		cronSched, err := scheduler.NewCron(q, rdb, entries, scheduler.CronConfig{
			Logger: logger,
		})
		if err != nil {
			logger.Error("cron setup failed", "error", err)
			os.Exit(1)
		}
		go cronSched.Run(ctx)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sig
		logger.Info("shutting down gRPC server")
		healthSrv.SetServingStatus("winter.v1.QueueService", healthpb.HealthCheckResponse_NOT_SERVING)
		s.GracefulStop()
		cancel()
	}()

	logger.Info("starting gRPC server", "port", grpcPort, "redis", redisAddr, "queues", queueNames)
	if err := s.Serve(lis); err != nil {
		logger.Error("gRPC server error", "error", err)
		os.Exit(1)
	}
}

func defaultConfig() *config.Config {
	return &config.Config{
		Redis: config.RedisConfig{
			Addr: envOr("REDIS_ADDR", "localhost:6379"),
		},
		Server: config.ServerConfig{
			GRPCPort: envOr("GRPC_PORT", "50051"),
			LogLevel: "info",
		},
		Workers: config.WorkerConfig{
			Concurrency:       10,
			HeartbeatInterval: "15s",
			StaleThreshold:    "60s",
			RecoveryInterval:  "30s",
		},
		Scheduler: config.SchedulerConfig{
			PollInterval: "500ms",
		},
	}
}

func queueNamesFromConfig(cfg *config.Config) []string {
	if len(cfg.Queues) == 0 {
		return []string{"default"}
	}
	names := make([]string, 0, len(cfg.Queues))
	for name := range cfg.Queues {
		names = append(names, name)
	}
	return names
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
