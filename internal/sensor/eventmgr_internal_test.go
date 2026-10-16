package sensor

import (
	"testing"

	"go.emeland.io/modelsrv/pkg/events"
)

// TestForwardableToSubscribers pins the forward policy that prevents the
// Metric ping-pong loop: a Metric is consumed as input (and may arrive from an
// upstream that is also a subscriber), so it must never be forwarded back out.
// The derived MetricInstance/MetricValue resources, which the sensor authors,
// must be forwarded.
func TestForwardableToSubscribers(t *testing.T) {
	cases := []struct {
		name    string
		resType events.ResourceType
		want    bool
	}{
		{"metric is not forwarded (would loop)", events.MetricResource, false},
		{"metric instance is forwarded", events.MetricInstanceResource, true},
		{"metric value is forwarded", events.MetricValueResource, true},
		{"unrelated resource is forwarded", events.NodeResource, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := forwardableToSubscribers(tc.resType); got != tc.want {
				t.Fatalf("forwardableToSubscribers(%v) = %v, want %v", tc.resType, got, tc.want)
			}
		})
	}
}
