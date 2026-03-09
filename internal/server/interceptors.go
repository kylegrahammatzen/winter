package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoggingInterceptor emits a structured log line for every unary RPC call
// with the method name, duration, and any error code.
func LoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		durationMs := time.Since(start).Milliseconds()

		attrs := []any{
			"method", info.FullMethod,
			"duration_ms", durationMs,
		}

		if err != nil {
			code := status.Code(err)
			attrs = append(attrs, "code", code.String(), "error", err.Error())
			if code == codes.Internal {
				logger.Error("grpc", attrs...)
			} else {
				logger.Warn("grpc", attrs...)
			}
		} else {
			attrs = append(attrs, "code", "OK")
			logger.Info("grpc", attrs...)
		}

		return resp, err
	}
}

// RecoveryInterceptor catches panics in RPC handlers and converts them to
// Internal errors so the gRPC server does not crash.
func RecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = status.Errorf(codes.Internal, "panic: %v", r)
			}
		}()
		return handler(ctx, req)
	}
}
