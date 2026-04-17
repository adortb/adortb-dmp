package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adortb/adortb-dmp/internal/api"
	"github.com/adortb/adortb-dmp/internal/consumer"
	"github.com/adortb/adortb-dmp/internal/expand"
	"github.com/adortb/adortb-dmp/internal/lookup"
	"github.com/adortb/adortb-dmp/internal/metrics"
	"github.com/adortb/adortb-dmp/internal/store"
	"github.com/adortb/adortb-dmp/internal/tagging"
)

const (
	listenAddr   = ":8086"
	redisAddr    = "localhost:6379"
	redisPoolSize = 50
	chEndpoint   = "http://localhost:8123"
	kafkaBroker  = "localhost:9092"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// ---- 存储层 ----
	redisClient := store.NewRedisClient(envOrDefault("REDIS_ADDR", redisAddr), redisPoolSize)
	redisStore := store.NewRedisStore(redisClient)

	chClient := store.NewClickHouseClient(envOrDefault("CLICKHOUSE_HTTP", chEndpoint))

	// 建表（幂等）
	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()
	if err := chClient.CreateTables(initCtx); err != nil {
		logger.Warn("clickhouse create tables failed (will retry on next start)", "error", err)
	}

	// ---- 标签引擎 ----
	engine := tagging.NewEngine(redisStore, tagging.DefaultRules)

	// ---- 处理管道 ----
	counters := metrics.Global
	pipeline := api.NewPipeline(redisStore, chClient, engine, logger, counters)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipeline.StartCHFlusher(ctx)

	// ---- 服务层 ----
	lookupSvc := lookup.NewService(redisStore)
	expandSvc := expand.NewService(redisStore) // redisStore 实现了 expand.TagStore 接口

	server := api.NewServer(pipeline, lookupSvc, expandSvc, logger, counters)

	// ---- Kafka 消费 ----
	brokers := []string{envOrDefault("KAFKA_BROKER", kafkaBroker)}
	topics := consumer.TopicsFromEnv(os.Getenv("KAFKA_TOPICS"))
	kafkaConsumer := consumer.NewConsumer(brokers, topics, pipeline, logger)
	kafkaConsumer.Start(ctx)

	// ---- HTTP 服务 ----
	httpServer := &http.Server{
		Addr:         envOrDefault("LISTEN_ADDR", listenAddr),
		Handler:      server,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("dmp server starting", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "error", err)
			cancel()
		}
	}()

	// ---- 优雅关机 ----
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
		logger.Info("shutdown signal received")
	case <-ctx.Done():
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}

	cancel()
	kafkaConsumer.Wait()
	logger.Info("dmp server stopped")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
