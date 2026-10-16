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

// fakeAPI is a promAPI stub returning a preset query value or error.
type fakeAPI struct {
	val      model.Value
	warnings promv1.Warnings
	err      error

	gotQuery string
}

func (f *fakeAPI) Query(_ context.Context, query string, _ time.Time, _ ...promv1.Option) (model.Value, promv1.Warnings, error) {
	f.gotQuery = query
	return f.val, f.warnings, f.err
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

var _ = Describe("Client.QueryVector", func() {
	ctx := context.Background()

	It("returns each series with its labels and value, dropping __name__", func() {
		f := &fakeAPI{val: model.Vector{
			{
				Metric: model.Metric{
					model.MetricNameLabel: "emeland_resource_count",
					"namespace":           "emeland-demo",
					"service":             "emeland-demo-filter",
				},
				Value: 24,
			},
			{
				Metric: model.Metric{
					model.MetricNameLabel: "emeland_resource_count",
					"namespace":           "emeland-demo",
					"service":             "emeland-demo-server",
				},
				Value: 25,
			},
		}}
		c := &Client{api: f}

		got, err := c.QueryVector(ctx, "sum by (namespace, job) (emeland_resource_count)")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))

		Expect(got[0].Labels).To(Equal(map[string]string{
			"namespace": "emeland-demo",
			"service":   "emeland-demo-filter",
		}))
		Expect(got[0].Labels).NotTo(HaveKey("__name__"))
		Expect(got[0].Value).To(Equal("24"))
		Expect(got[1].Value).To(Equal("25"))
	})

	It("returns an empty (non-nil) slice for an empty vector", func() {
		c := &Client{api: &fakeAPI{val: model.Vector{}}}
		got, err := c.QueryVector(ctx, "sum by (namespace) (missing_metric)")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		Expect(got).To(BeEmpty())
	})

	It("returns a scalar as a single label-less sample", func() {
		c := &Client{api: &fakeAPI{val: &model.Scalar{Value: 3}}}
		got, err := c.QueryVector(ctx, "vector(3)")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))
		Expect(got[0].Labels).To(BeEmpty())
		Expect(got[0].Value).To(Equal("3"))
	})

	It("rejects a matrix result", func() {
		c := &Client{api: &fakeAPI{val: model.Matrix{}}}
		_, err := c.QueryVector(ctx, "rate(x[5m])")
		Expect(err).To(MatchError(ContainSubstring("unsupported result type")))
	})

	It("propagates query errors", func() {
		c := &Client{api: &fakeAPI{err: errors.New("boom")}}
		_, err := c.QueryVector(ctx, "bad")
		Expect(err).To(MatchError(ContainSubstring("boom")))
	})
})

var _ = Describe("Sample.LabelKeys", func() {
	It("returns sorted label keys", func() {
		s := Sample{Labels: map[string]string{"service": "s", "namespace": "n", "type": "t"}}
		Expect(s.LabelKeys()).To(Equal([]string{"namespace", "service", "type"}))
	})

	It("returns an empty slice for no labels", func() {
		s := Sample{Labels: map[string]string{}}
		Expect(s.LabelKeys()).To(BeEmpty())
	})
})
