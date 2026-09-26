package prometheus

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// promAPI is the subset of the Prometheus v1 API used by this package. It is
// an interface so tests can inject a fake without an HTTP round trip.
type promAPI interface {
	Query(ctx context.Context, query string, ts time.Time, opts ...promv1.Option) (model.Value, promv1.Warnings, error)
}

// Client runs instant PromQL queries against a single Prometheus server. It
// performs no authentication.
type Client struct {
	api promAPI
}

// NewClient builds a Client for the Prometheus server at baseURL
// (e.g. "http://prometheus:9090").
func NewClient(baseURL string) (*Client, error) {
	c, err := promapi.NewClient(promapi.Config{Address: baseURL})
	if err != nil {
		return nil, fmt.Errorf("prometheus: build client for %q: %w", baseURL, err)
	}
	return &Client{api: promv1.NewAPI(c)}, nil
}

// Sample is one series of an instant-vector result: its label set (without the
// internal __name__ label) and its value formatted as a string that preserves
// the exact numeric formatting.
type Sample struct {
	// Labels are the series labels, excluding the __name__ metric-name label.
	Labels map[string]string
	// Value is the sample value, formatted like QueryScalar's output.
	Value string
}

// QueryScalar runs an instant query at the current time and returns the single
// scalar result as a string, preserving the exact numeric formatting.
//
// A query result is accepted when it is a PromQL scalar, or an instant vector
// containing exactly one sample. Empty results, multi-sample vectors, range
// matrices and string results are rejected: a scalar reading is expected.
func (c *Client) QueryScalar(ctx context.Context, query string) (string, error) {
	val, _, err := c.query(ctx, query)
	if err != nil {
		return "", err
	}

	switch v := val.(type) {
	case *model.Scalar:
		return formatSampleValue(v.Value), nil
	case model.Vector:
		switch len(v) {
		case 0:
			return "", fmt.Errorf("prometheus: query %q returned an empty vector", query)
		case 1:
			return formatSampleValue(v[0].Value), nil
		default:
			return "", fmt.Errorf("prometheus: query %q returned %d samples, expected a single scalar", query, len(v))
		}
	default:
		return "", fmt.Errorf("prometheus: query %q returned unsupported result type %s, expected a scalar", query, val.Type())
	}
}

// QueryVector runs an instant query at the current time and returns every
// series in the resulting instant vector as a [Sample]. It is used to discover
// the label-set groups produced by an aggregated query such as
// `sum by (namespace, job) (some_metric)`.
//
// An empty vector yields an empty (non-nil) slice, not an error: a metric with
// no series present in Prometheus is a valid, if uninteresting, state. A PromQL
// scalar is returned as a single label-less Sample. Matrix and string results
// are rejected.
func (c *Client) QueryVector(ctx context.Context, query string) ([]Sample, error) {
	val, _, err := c.query(ctx, query)
	if err != nil {
		return nil, err
	}

	switch v := val.(type) {
	case model.Vector:
		out := make([]Sample, 0, len(v))
		for _, s := range v {
			out = append(out, Sample{
				Labels: metricToLabels(s.Metric),
				Value:  formatSampleValue(s.Value),
			})
		}
		return out, nil
	case *model.Scalar:
		return []Sample{{Labels: map[string]string{}, Value: formatSampleValue(v.Value)}}, nil
	default:
		return nil, fmt.Errorf("prometheus: query %q returned unsupported result type %s, expected an instant vector", query, val.Type())
	}
}

// query is the shared instant-query entrypoint. Prometheus query warnings are
// non-fatal and not surfaced in this version.
func (c *Client) query(ctx context.Context, query string) (model.Value, promv1.Warnings, error) {
	val, warnings, err := c.api.Query(ctx, query, time.Now())
	if err != nil {
		return nil, nil, fmt.Errorf("prometheus: query %q: %w", query, err)
	}
	return val, warnings, nil
}

// metricToLabels converts a Prometheus metric label set into a plain string map,
// dropping the internal __name__ metric-name label so only the grouping labels
// remain.
func metricToLabels(m model.Metric) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if k == model.MetricNameLabel {
			continue
		}
		out[string(k)] = string(v)
	}
	return out
}

func formatSampleValue(v model.SampleValue) string {
	return strconv.FormatFloat(float64(v), 'f', -1, 64)
}

// LabelKeys returns the sorted label keys of a Sample, useful for deterministic
// identity derivation.
func (s Sample) LabelKeys() []string {
	keys := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
