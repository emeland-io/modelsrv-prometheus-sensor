package fanout

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"emeland.io/modelsrv-prometheus-sensor/internal/prometheus"

	"go.emeland.io/modelsrv/pkg/events"
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"
)

func TestFanout(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "fanout")
}

// fakeSource returns a fixed set of Metrics.
type fakeSource struct {
	metrics []mdlobs.Metric
	err     error
}

func (f *fakeSource) GetMetrics() ([]mdlobs.Metric, error) { return f.metrics, f.err }

// fakeQuerier returns a preset vector per call; queries are recorded.
type fakeQuerier struct {
	samples []prometheus.Sample
	err     error
	queries []string
}

func (f *fakeQuerier) QueryVector(_ context.Context, query string) ([]prometheus.Sample, error) {
	f.queries = append(f.queries, query)
	return f.samples, f.err
}

// recordingEmitter captures every emitted event.
type recordingEmitter struct {
	events []events.Event
	err    error
}

func (r *recordingEmitter) Emit(ev events.Event) error {
	if r.err != nil {
		return r.err
	}
	r.events = append(r.events, ev)
	return nil
}

func newMetric(id uuid.UUID, ann map[string]string) mdlobs.Metric {
	m := mdlobs.NewMetric(id)
	m.SetDisplayName("Resource count")
	for k, v := range ann {
		m.GetAnnotations().Add(k, v)
	}
	return m
}

var _ = Describe("query building", func() {
	It("builds the discovery query with default group-by and aggregation", func() {
		Expect(buildAggregateQuery("sum", DefaultGroupBy, "emeland_resource_count")).
			To(Equal("sum by (namespace, job) (emeland_resource_count)"))
	})

	It("scopes the per-group query with sorted label matchers", func() {
		got := buildGroupQuery("avg", []string{"namespace", "service"}, "m",
			map[string]string{"service": "svc", "namespace": "ns"})
		Expect(got).To(Equal(`avg by (namespace, service) (m{namespace="ns", service="svc"})`))
	})

	It("omits the selector braces when there are no labels", func() {
		Expect(buildGroupQuery("sum", []string{"x"}, "m", map[string]string{})).
			To(Equal("sum by (x) (m)"))
	})
})

var _ = Describe("parseGroupBy", func() {
	It("falls back to the default when empty", func() {
		Expect(parseGroupBy("")).To(Equal(DefaultGroupBy))
	})
	It("splits and trims a custom list", func() {
		Expect(parseGroupBy(" namespace , service , type ")).
			To(Equal([]string{"namespace", "service", "type"}))
	})
})

var _ = Describe("instanceIDFor", func() {
	metricID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	It("is deterministic and label-order independent", func() {
		a := instanceIDFor(metricID, map[string]string{"namespace": "n", "service": "s"})
		b := instanceIDFor(metricID, map[string]string{"service": "s", "namespace": "n"})
		Expect(a).To(Equal(b))
	})

	It("differs per group", func() {
		a := instanceIDFor(metricID, map[string]string{"service": "a"})
		b := instanceIDFor(metricID, map[string]string{"service": "b"})
		Expect(a).NotTo(Equal(b))
	})

	It("differs per Metric", func() {
		other := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		a := instanceIDFor(metricID, map[string]string{"service": "a"})
		b := instanceIDFor(other, map[string]string{"service": "a"})
		Expect(a).NotTo(Equal(b))
	})
})

var _ = Describe("Evaluator.EvaluateOnce", func() {
	ctx := context.Background()
	metricID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	newEval := func(src Source, q Querier) (*Evaluator, *recordingEmitter) {
		em := &recordingEmitter{}
		return New(src, q, em, zap.NewNop().Sugar()), em
	}

	It("emits one MetricInstance and one MetricValue per group", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{
			newMetric(metricID, map[string]string{AnnotationSource: "m", AnnotationAggregation: "sum"}),
		}}
		q := &fakeQuerier{samples: []prometheus.Sample{
			{Labels: map[string]string{"namespace": "ns", "job": "a"}, Value: "24"},
			{Labels: map[string]string{"namespace": "ns", "job": "b"}, Value: "25"},
		}}
		ev, em := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())

		var mi, mv int
		for _, e := range em.events {
			switch e.ResourceType {
			case events.MetricInstanceResource:
				mi++
				Expect(e.Operation).To(Equal(events.CreateOperation))
			case events.MetricValueResource:
				mv++
			}
		}
		Expect(mi).To(Equal(2))
		Expect(mv).To(Equal(2))
		Expect(q.queries[0]).To(Equal("sum by (namespace, job) (m)"))
	})

	It("derives instance display names from the metric and group labels", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{
			newMetric(metricID, map[string]string{AnnotationSource: "m"}),
		}}
		q := &fakeQuerier{samples: []prometheus.Sample{
			{Labels: map[string]string{"namespace": "ns", "job": "svc"}, Value: "1"},
		}}
		ev, em := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())

		mi := em.events[0].Objects[0].(mdlobs.MetricInstance)
		Expect(mi.GetDisplayName()).To(Equal("Resource count (ns/svc)"))
		Expect(mi.GetAnnotations().GetValue(AnnotationExpression)).
			To(Equal(`sum by (namespace, job) (m{namespace="ns", job="svc"})`))
		Expect(mi.GetAnnotations().GetValue(AnnotationLanguage)).To(Equal(LanguagePromQL))
		Expect(mi.GetAnnotations().GetValue("emeland.io/metric.label.namespace")).To(Equal("ns"))
	})

	It("skips a metric without a source annotation", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{newMetric(metricID, map[string]string{})}}
		q := &fakeQuerier{}
		ev, em := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(q.queries).To(BeEmpty())
		Expect(em.events).To(BeEmpty())
	})

	It("skips a metric with an unsupported aggregation", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{
			newMetric(metricID, map[string]string{AnnotationSource: "m", AnnotationAggregation: "median"}),
		}}
		q := &fakeQuerier{}
		ev, em := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(q.queries).To(BeEmpty())
		Expect(em.events).To(BeEmpty())
	})

	It("honours a custom group-by", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{
			newMetric(metricID, map[string]string{
				AnnotationSource:  "emeland_resource_count",
				AnnotationGroupBy: "namespace,service,type",
			}),
		}}
		q := &fakeQuerier{}
		ev, _ := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(q.queries[0]).To(Equal("sum by (namespace, service, type) (emeland_resource_count)"))
	})

	It("does not re-emit an unchanged value on the next poll", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{
			newMetric(metricID, map[string]string{AnnotationSource: "m"}),
		}}
		q := &fakeQuerier{samples: []prometheus.Sample{
			{Labels: map[string]string{"namespace": "ns", "service": "a"}, Value: "10"},
		}}
		ev, em := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		first := len(em.events)
		Expect(first).To(Equal(2)) // one instance + one value

		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(em.events).To(HaveLen(first)) // nothing new emitted
	})

	It("emits an Update when the value changes", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{
			newMetric(metricID, map[string]string{AnnotationSource: "m"}),
		}}
		q := &fakeQuerier{samples: []prometheus.Sample{
			{Labels: map[string]string{"namespace": "ns", "service": "a"}, Value: "10"},
		}}
		ev, em := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())

		q.samples = []prometheus.Sample{
			{Labels: map[string]string{"namespace": "ns", "service": "a"}, Value: "11"},
		}
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())

		var lastValueOp events.Operation
		for _, e := range em.events {
			if e.ResourceType == events.MetricValueResource {
				lastValueOp = e.Operation
			}
		}
		Expect(lastValueOp).To(Equal(events.UpdateOperation))
	})

	It("reconciles away a group that disappears, deleting value then instance", func() {
		src := &fakeSource{metrics: []mdlobs.Metric{
			newMetric(metricID, map[string]string{AnnotationSource: "m"}),
		}}
		q := &fakeQuerier{samples: []prometheus.Sample{
			{Labels: map[string]string{"namespace": "ns", "service": "a"}, Value: "1"},
			{Labels: map[string]string{"namespace": "ns", "service": "b"}, Value: "2"},
		}}
		ev, em := newEval(src, q)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())

		gone := instanceIDFor(metricID, map[string]string{"namespace": "ns", "service": "b"})
		goneValue := valueIDFor(gone)

		// Drop service "b".
		q.samples = []prometheus.Sample{
			{Labels: map[string]string{"namespace": "ns", "service": "a"}, Value: "1"},
		}
		em.events = nil
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())

		Expect(em.events).To(HaveLen(2))
		Expect(em.events[0].ResourceType).To(Equal(events.MetricValueResource))
		Expect(em.events[0].Operation).To(Equal(events.DeleteOperation))
		Expect(em.events[0].ResourceId).To(Equal(goneValue))
		Expect(em.events[1].ResourceType).To(Equal(events.MetricInstanceResource))
		Expect(em.events[1].Operation).To(Equal(events.DeleteOperation))
		Expect(em.events[1].ResourceId).To(Equal(gone))
	})
})
