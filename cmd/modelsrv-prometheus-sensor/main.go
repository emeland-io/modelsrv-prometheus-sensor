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
	"emeland.io/modelsrv-prometheus-sensor/internal/fanout"
	"emeland.io/modelsrv-prometheus-sensor/internal/prometheus"
	"emeland.io/modelsrv-prometheus-sensor/internal/sensor"
)

func main() {
	var (
		configPath   = flag.String("config", envOrDefault("SENSOR_CONFIG", "config/sensor.yaml"), "Path to YAML config")
		listenAddr   = flag.String("listen", envOrDefault("SENSOR_LISTEN_ADDR", "localhost:24200"), "HTTP listen address for this sensor's modelsrv API")
		callbackURL  = flag.String("callback-url", envOrDefault("SENSOR_CALLBACK_URL", ""), "Callback base URL the upstream uses to reach this sensor (e.g. http://sensor.ns.svc:80/api/). Defaults to http://<listen>/api/ when empty")
		pollInterval = flag.Duration("poll-interval", envDurationOrDefault("SENSOR_POLL_INTERVAL", 0), "Metric fan-out interval (overrides config when > 0)")
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

	if err := run(ctx, cfg, *listenAddr, *callbackURL, slog); err != nil {
		slog.Fatalw("sensor exited with error", "error", err)
	}
}

func run(ctx context.Context, cfg config.Config, listenAddr, callbackURL string, log *zap.SugaredLogger) error {
	srv, err := sensor.New(listenAddr, cfg.Subscribers, log)
	if err != nil {
		return fmt.Errorf("start sensor: %w", err)
	}
	defer func() { _ = srv.Close() }()

	// Optionally subscribe to an upstream node so it pushes Metric definitions
	// into our local model. When no upstream is configured, the sensor does not
	// register anywhere; Metrics are supplied by other means (e.g. pushed
	// directly to this sensor's /api/events/push, or authored locally). This
	// keeps the sensor a pure source (like the git/k8s sensors) with no return
	// subscription edge from the filter.
	if cfg.Upstream != "" {
		// The callback URL is where the upstream reaches this sensor; it
		// defaults to our own listen address but can be overridden (e.g. with a
		// Kubernetes Service DNS name when the bind address is 0.0.0.0).
		if callbackURL == "" {
			callbackURL = fmt.Sprintf("http://%s/api/", listenAddr)
		}
		if err := srv.RegisterUpstream(cfg.Upstream, callbackURL); err != nil {
			return fmt.Errorf("register with upstream: %w", err)
		}
	} else {
		log.Infow("no upstream configured; not registering as a subscriber (Metrics must be pushed to this sensor directly)")
	}

	promClient, err := prometheus.NewClient(cfg.PrometheusURL)
	if err != nil {
		return fmt.Errorf("build prometheus client: %w", err)
	}

	evaluator := fanout.New(srv.Model(), promClient, srv, log)

	log.Infow("prometheus sensor running",
		"listen", listenAddr,
		"upstream", cfg.Upstream,
		"prometheusUrl", cfg.PrometheusURL,
		"pollInterval", cfg.PollInterval.String(),
		"subscribers", len(cfg.Subscribers),
	)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// Run once at startup, then on every tick.
	poll(ctx, evaluator, log)
	for {
		select {
		case <-ctx.Done():
			log.Infow("shutting down")
			return nil
		case <-ticker.C:
			poll(ctx, evaluator, log)
		}
	}
}

// poll runs one fan-out cycle: read Metrics, query Prometheus, and emit the
// derived MetricInstances and MetricValues (reconciling away disappeared groups).
func poll(ctx context.Context, evaluator *fanout.Evaluator, log *zap.SugaredLogger) {
	if err := evaluator.EvaluateOnce(ctx); err != nil {
		log.Errorw("fan-out cycle failed", "error", err)
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
