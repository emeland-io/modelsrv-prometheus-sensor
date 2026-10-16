// Package config loads and validates the prometheus-sensor YAML configuration:
// the local listen address, the upstream emeland node to subscribe to for
// Metric definitions, the downstream subscribers to forward MetricValues to,
// the Prometheus base URL to query, and the poll interval.
package config
