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

// queryAPI is the subset of the Prometheus v1 API used by this package. It is
// an interface so tests can inject a fake without an HTTP round trip.
type queryAPI interface {
	Query(ctx context.Context, query string, ts time.Time, opts ...promv1.Option) (model.Value, promv1.Warnings, error)
}

// Client runs instant PromQL queries against a single Prometheus server and
// reduces the result to a scalar value. It performs no authentication.
type Client struct {
	api queryAPI
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
