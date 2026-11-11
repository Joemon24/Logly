// Command agent-v2 runs the schema-v2 collection runtime: authentication log to
// SQLite outbox to Kafka. The v1 agent in cmd/agent is unchanged.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"agent/runtimev2"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := runtimev2.LoadConfig()
	if err != nil {
		logger.Error("configuration error", "error", err)
		os.Exit(2)
	}

	rt, err := runtimev2.New(cfg, logger, nil)
	if err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			logger.Error("close failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := rt.Run(ctx); err != nil {
		logger.Error("agent failed", "error", err)
		os.Exit(1)
	}
}
