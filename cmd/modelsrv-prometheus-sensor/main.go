package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"emeland.io/modelsrv-prometheus-sensor/internal/config"
	"emeland.io/modelsrv-prometheus-sensor/internal/eval"
	"emeland.io/modelsrv-prometheus-sensor/internal/prometheus"
	"emeland.io/modelsrv-prometheus-sensor/internal/sensor"
)

func main() {
	var (
		configPath   = flag.String("config", envOrDefault("SENSOR_CONFIG", "config/sensor.yaml"), "Path to YAML config")
		listenAddr   = flag.String("listen", envOrDefault("SENSOR_LISTEN_ADDR", "localhost:24200"), "HTTP listen address for this sensor's modelsrv API")
		pollInterval = flag.Duration("poll-interval", envDurationOrDefault("SENSOR_POLL_INTERVAL", 0), "Metric evaluation interval (overrides config when > 0)")
	)
	flag.Parse()

	// Development logger attaches stack traces to Warn+ by default, which looks
	// like a panic for benign warnings. Only attach stacks at Error and above.
	log := zap.Must(zap.NewDevelopmentConfig().Build(zap.AddStacktrace(zap.ErrorLevel)))
	slog := log.Sugar()
	defer func() { _ = log.Sync() }()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Fatalw("failed to load config", "path", *configPath, "error", err)
	}
	if *pollInterval > 0 {
		cfg.PollInterval = *pollInterval
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg, *listenAddr, slog); err != nil {
		slog.Fatalw("sensor exited with error", "error", err)
	}
}

func run(ctx context.Context, cfg config.Config, listenAddr string, log *zap.SugaredLogger) error {
	srv, err := sensor.New(listenAddr, cfg.Subscribers, log)
	if err != nil {
		return fmt.Errorf("start sensor: %w", err)
	}
	defer func() { _ = srv.Close() }()

	// Subscribe to the upstream node so it pushes Metric definitions into our
	// local model. The callback URL is our own API base URL.
	callbackURL := fmt.Sprintf("http://%s/api/", listenAddr)
	if err := srv.RegisterUpstream(cfg.Upstream, callbackURL); err != nil {
		return fmt.Errorf("register with upstream: %w", err)
	}

	promClient, err := prometheus.NewClient(cfg.PrometheusURL)
	if err != nil {
		return fmt.Errorf("build prometheus client: %w", err)
	}

	evaluator := eval.New(srv.Model(), promClient, srv, log)

	log.Infow("prometheus sensor running",
		"listen", listenAddr,
		"upstream", cfg.Upstream,
		"prometheusUrl", cfg.PrometheusURL,
		"pollInterval", cfg.PollInterval.String(),
		"subscribers", len(cfg.Subscribers),
	)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// Evaluate once at startup, then on every tick.
	evaluate(ctx, evaluator, log)
	for {
		select {
		case <-ctx.Done():
			log.Infow("shutting down")
			return nil
		case <-ticker.C:
			evaluate(ctx, evaluator, log)
		}
	}
}

func evaluate(ctx context.Context, evaluator *eval.Evaluator, log *zap.SugaredLogger) {
	if err := evaluator.EvaluateOnce(ctx); err != nil {
		log.Errorw("evaluation cycle failed", "error", err)
	}
}

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return fallback
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
	}
	return fallback
}
