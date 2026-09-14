package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultPollInterval is used when the config does not specify one.
const DefaultPollInterval = 30 * time.Second

// Config is the on-disk YAML configuration for the prometheus sensor.
type Config struct {
	// Upstream is the base API URL of the emeland node that holds the Metric
	// definitions. The sensor registers itself there so Metric events are
	// pushed to it (e.g. "http://modelsrv:24000/api/").
	Upstream string `yaml:"upstream"`

	// Subscribers are downstream model servers that receive the MetricValue
	// events this sensor emits (e.g. "http://modelsrv:24000/api/").
	Subscribers []string `yaml:"subscribers"`

	// PrometheusURL is the base URL of the Prometheus server to query
	// (e.g. "http://prometheus:9090").
	PrometheusURL string `yaml:"prometheusUrl"`

	// PollInterval is how often the sensor re-evaluates all known Metrics.
	PollInterval time.Duration `yaml:"pollInterval"`
}

// Load reads, normalizes and validates the YAML config at path.
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	normalize(&cfg)
	if err := validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func normalize(cfg *Config) {
	cfg.Upstream = strings.TrimSpace(cfg.Upstream)
	cfg.PrometheusURL = strings.TrimSpace(cfg.PrometheusURL)

	// Prometheus and upstream base URLs should not carry a trailing slash so
	// callers can join paths predictably.
	cfg.PrometheusURL = strings.TrimRight(cfg.PrometheusURL, "/")

	subs := make([]string, 0, len(cfg.Subscribers))
	for _, s := range cfg.Subscribers {
		s = strings.TrimSpace(s)
		if s != "" {
			subs = append(subs, s)
		}
	}
	cfg.Subscribers = subs

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
}

func validate(cfg Config) error {
	if cfg.Upstream == "" {
		return fmt.Errorf("config: missing upstream")
	}
	if err := validateHTTPURL("upstream", cfg.Upstream); err != nil {
		return err
	}
	if cfg.PrometheusURL == "" {
		return fmt.Errorf("config: missing prometheusUrl")
	}
	if err := validateHTTPURL("prometheusUrl", cfg.PrometheusURL); err != nil {
		return err
	}
	for i, s := range cfg.Subscribers {
		if err := validateHTTPURL(fmt.Sprintf("subscribers[%d]", i), s); err != nil {
			return err
		}
	}
	return nil
}

func validateHTTPURL(field, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("config: %s: invalid URL %q: %w", field, raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("config: %s: URL %q must use http or https scheme", field, raw)
	}
	if u.Host == "" {
		return fmt.Errorf("config: %s: URL %q must include a host", field, raw)
	}
	return nil
}
