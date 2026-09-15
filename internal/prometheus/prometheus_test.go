package prometheus

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

func TestPrometheus(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "prometheus")
}

// fakeAPI is a promAPI stub returning preset query/rules values or errors.
type fakeAPI struct {
	val      model.Value
	warnings promv1.Warnings
	err      error

	gotQuery string

	rules    promv1.RulesResult
	rulesErr error
}

func (f *fakeAPI) Query(_ context.Context, query string, _ time.Time, _ ...promv1.Option) (model.Value, promv1.Warnings, error) {
	f.gotQuery = query
	return f.val, f.warnings, f.err
}

func (f *fakeAPI) Rules(_ context.Context, _ []string) (promv1.RulesResult, error) {
	return f.rules, f.rulesErr
}

var _ = Describe("Client.QueryScalar", func() {
	ctx := context.Background()

	It("returns the value of a PromQL scalar", func() {
		f := &fakeAPI{val: &model.Scalar{Value: 42}}
		c := &Client{api: f}
		got, err := c.QueryScalar(ctx, "vector(42)")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("42"))
		Expect(f.gotQuery).To(Equal("vector(42)"))
	})

	It("preserves fractional formatting", func() {
		f := &fakeAPI{val: &model.Scalar{Value: 0.125}}
		c := &Client{api: f}
		got, err := c.QueryScalar(ctx, "q")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("0.125"))
	})

	It("accepts an instant vector with a single sample", func() {
		f := &fakeAPI{val: model.Vector{{Value: 7}}}
		c := &Client{api: f}
		got, err := c.QueryScalar(ctx, "up")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("7"))
	})

	It("rejects an empty vector", func() {
		f := &fakeAPI{val: model.Vector{}}
		c := &Client{api: f}
		_, err := c.QueryScalar(ctx, "up")
		Expect(err).To(MatchError(ContainSubstring("empty vector")))
	})

	It("rejects a multi-sample vector", func() {
		f := &fakeAPI{val: model.Vector{{Value: 1}, {Value: 2}}}
		c := &Client{api: f}
		_, err := c.QueryScalar(ctx, "up")
		Expect(err).To(MatchError(ContainSubstring("expected a single scalar")))
	})

	It("rejects a matrix result", func() {
		f := &fakeAPI{val: model.Matrix{}}
		c := &Client{api: f}
		_, err := c.QueryScalar(ctx, "rate(x[5m])")
		Expect(err).To(MatchError(ContainSubstring("unsupported result type")))
	})

	It("propagates query errors", func() {
		f := &fakeAPI{err: errors.New("boom")}
		c := &Client{api: f}
		_, err := c.QueryScalar(ctx, "bad")
		Expect(err).To(MatchError(ContainSubstring("boom")))
	})
})

var _ = Describe("Client.FetchAlertingRules", func() {
	ctx := context.Background()

	It("returns alerting rules and ignores recording rules", func() {
		f := &fakeAPI{rules: promv1.RulesResult{Groups: []promv1.RuleGroup{
			{
				Name: "slo",
				Rules: promv1.Rules{
					promv1.AlertingRule{
						Name:        "HighLatency",
						Query:       "histogram_quantile(0.99, x) > 0.5",
						Duration:    300,
						Labels:      model.LabelSet{"severity": "page"},
						Annotations: model.LabelSet{"summary": "latency too high"},
					},
					promv1.RecordingRule{Name: "job:latency:p99", Query: "histogram_quantile(0.99, x)"},
				},
			},
		}}}
		c := &Client{api: f}

		rules, err := c.FetchAlertingRules(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(rules).To(HaveLen(1))
		Expect(rules[0].Group).To(Equal("slo"))
		Expect(rules[0].Name).To(Equal("HighLatency"))
		Expect(rules[0].Query).To(Equal("histogram_quantile(0.99, x) > 0.5"))
		Expect(rules[0].Duration).To(Equal(float64(300)))
		Expect(rules[0].Labels).To(HaveKeyWithValue("severity", "page"))
		Expect(rules[0].Annotations).To(HaveKeyWithValue("summary", "latency too high"))
	})

	It("returns an empty slice when there are no rules", func() {
		c := &Client{api: &fakeAPI{rules: promv1.RulesResult{}}}
		rules, err := c.FetchAlertingRules(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(rules).To(BeEmpty())
	})

	It("propagates fetch errors", func() {
		c := &Client{api: &fakeAPI{rulesErr: errors.New("rules boom")}}
		_, err := c.FetchAlertingRules(ctx)
		Expect(err).To(MatchError(ContainSubstring("rules boom")))
	})
})
