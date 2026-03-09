// Command server runs the standalone Winter gRPC server.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kylegrahammatzen/winter/internal/queue"
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
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	grpcPort := envOr("GRPC_PORT", "50051")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
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

	// Start lease recovery for default queue.
	recovery := worker.NewRecovery(q, worker.RecoveryConfig{
		Queues: []string{"default"},
		Logger: logger,
	})
	go recovery.Run(ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sig
		logger.Info("shutting down gRPC server")
		healthSrv.SetServingStatus("winter.v1.QueueService", healthpb.HealthCheckResponse_NOT_SERVING)
		s.GracefulStop()
		cancel()
	}()

	logger.Info("starting gRPC server", "port", grpcPort, "redis", redisAddr)
	if err := s.Serve(lis); err != nil {
		logger.Error("gRPC server error", "error", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
