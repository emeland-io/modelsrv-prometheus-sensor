package prometheus

import (
	"context"
	"fmt"
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
	Rules(ctx context.Context, matches []string) (promv1.RulesResult, error)
}

// Client runs instant PromQL queries against a single Prometheus server and
// reduces the result to a scalar value, and fetches alerting rules. It performs
// no authentication.
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

// QueryScalar runs an instant query at the current time and returns the single
// scalar result as a string, preserving the exact numeric formatting.
//
// A query result is accepted when it is a PromQL scalar, or an instant vector
// containing exactly one sample. Empty results, multi-sample vectors, range
// matrices and string results are rejected: this sensor supports scalar
// metrics only.
func (c *Client) QueryScalar(ctx context.Context, query string) (string, error) {
	// Prometheus query warnings are non-fatal and not surfaced in this version.
	val, _, err := c.api.Query(ctx, query, time.Now())
	if err != nil {
		return "", fmt.Errorf("prometheus: query %q: %w", query, err)
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

func formatSampleValue(v model.SampleValue) string {
	return strconv.FormatFloat(float64(v), 'f', -1, 64)
}

// AlertingRule is the minimal, decoupled view of a Prometheus alerting rule
// this package exposes: the identity (group + name + labels), the PromQL
// expression, the pending duration, and the rule's labels/annotations.
type AlertingRule struct {
	// Group is the rule group name the rule belongs to (part of its identity).
	Group string
	// Name is the alert name.
	Name string
	// Query is the rule's PromQL expression (typically a boolean condition).
	Query string
	// Duration is the rule's "for" pending duration, in seconds.
	Duration float64
	// Labels are the rule's labels (identity + severity etc.).
	Labels map[string]string
	// Annotations are the rule's annotations (summary/description etc.).
	Annotations map[string]string
}

// FetchAlertingRules returns all alerting rules configured in Prometheus
// (recording rules are ignored). It reads /api/v1/rules; the definitions, not
// the firing state, are what this sensor consumes.
func (c *Client) FetchAlertingRules(ctx context.Context) ([]AlertingRule, error) {
	res, err := c.api.Rules(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("prometheus: fetch rules: %w", err)
	}

	var out []AlertingRule
	for _, g := range res.Groups {
		for _, r := range g.Rules {
			ar, ok := r.(promv1.AlertingRule)
			if !ok {
				// Recording rule (or unknown) — not an alert, skip.
				continue
			}
			out = append(out, AlertingRule{
				Group:       g.Name,
				Name:        ar.Name,
				Query:       ar.Query,
				Duration:    ar.Duration,
				Labels:      labelSetToMap(ar.Labels),
				Annotations: labelSetToMap(ar.Annotations),
			})
		}
	}
	return out, nil
}

func labelSetToMap(ls model.LabelSet) map[string]string {
	if len(ls) == 0 {
		return nil
	}
	m := make(map[string]string, len(ls))
	for k, v := range ls {
		m[string(k)] = string(v)
	}
	return m
}
