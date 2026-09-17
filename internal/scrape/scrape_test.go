package scrape_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.emeland.io/modelsrv/pkg/events"
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"

	"emeland.io/modelsrv-prometheus-sensor/internal/eval"
	"emeland.io/modelsrv-prometheus-sensor/internal/prometheus"
	"emeland.io/modelsrv-prometheus-sensor/internal/scrape"
)

func TestScrape(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "scrape")
}

type fakeFetcher struct {
	rules []prometheus.AlertingRule
	err   error
}

func (f *fakeFetcher) FetchAlertingRules(_ context.Context) ([]prometheus.AlertingRule, error) {
	return f.rules, f.err
}

// fakeEmitter records emits and can be told to fail emits of a given type.
type fakeEmitter struct {
	emitted  []events.Event
	failType events.ResourceType
}

func (f *fakeEmitter) Emit(ev events.Event) error {
	if f.failType != events.UnknownResourceType && ev.ResourceType == f.failType {
		return errors.New("emit failed")
	}
	f.emitted = append(f.emitted, ev)
	return nil
}

func eventsOfType(evs []events.Event, rt events.ResourceType) []events.Event {
	var out []events.Event
	for _, ev := range evs {
		if ev.ResourceType == rt {
			out = append(out, ev)
		}
	}
	return out
}

func sampleRule() prometheus.AlertingRule {
	return prometheus.AlertingRule{
		Group:       "slo",
		Name:        "HighLatency",
		Query:       "histogram_quantile(0.99, x) > 0.5",
		Duration:    300,
		Labels:      map[string]string{"severity": "page"},
		Annotations: map[string]string{"description": "p99 latency too high"},
	}
}

func onlyMI(evs []events.Event) mdlobs.MetricInstance {
	e := eventsOfType(evs, events.MetricInstanceResource)
	Expect(e).To(HaveLen(1))
	return e[0].Objects[0].(mdlobs.MetricInstance)
}

func onlyTh(evs []events.Event) mdlobs.Threshold {
	e := eventsOfType(evs, events.ThresholdResource)
	Expect(e).To(HaveLen(1))
	return e[0].Objects[0].(mdlobs.Threshold)
}

var _ = Describe("Scraper.ScrapeOnce", func() {
	ctx := context.Background()

	It("splits the rule into a measurement instance and an operator/limit threshold", func() {
		f := &fakeFetcher{rules: []prometheus.AlertingRule{sampleRule()}}
		em := &fakeEmitter{}
		Expect(scrape.New(f, em, nil).ScrapeOnce(ctx)).To(Succeed())

		mi := onlyMI(em.emitted)
		// MetricInstance carries only the measurement (left of the comparison).
		Expect(mi.GetAnnotations().GetValue(eval.AnnotationExpression)).To(Equal("histogram_quantile(0.99, x)"))
		Expect(mi.GetAnnotations().GetValue(eval.AnnotationLanguage)).To(Equal(eval.LanguagePromQL))

		th := onlyTh(em.emitted)
		Expect(th.GetMetricInstanceId()).To(Equal(mi.GetMetricInstanceId()))
		// Threshold carries the structured operator + limit and NO expression.
		Expect(th.GetAnnotations().GetValue(eval.AnnotationThresholdOperator)).To(Equal("gt"))
		Expect(th.GetAnnotations().GetValue(eval.AnnotationThresholdLimit)).To(Equal("0.5"))
		Expect(th.GetAnnotations().GetValue("emeland.io/threshold.expression")).To(BeEmpty())
	})

	It("falls back to a boolean measurement + gt 0 for an un-splittable rule", func() {
		r := sampleRule()
		r.Query = "predict_linear(x[1h], 3600) > on(job) group_left y" // non-numeric RHS
		f := &fakeFetcher{rules: []prometheus.AlertingRule{r}}
		em := &fakeEmitter{}
		Expect(scrape.New(f, em, nil).ScrapeOnce(ctx)).To(Succeed())

		mi := onlyMI(em.emitted)
		Expect(mi.GetAnnotations().GetValue(eval.AnnotationExpression)).To(Equal(r.Query))
		th := onlyTh(em.emitted)
		Expect(th.GetAnnotations().GetValue(eval.AnnotationThresholdOperator)).To(Equal("gt"))
		Expect(th.GetAnnotations().GetValue(eval.AnnotationThresholdLimit)).To(Equal("0"))
	})

	It("does not re-emit an unchanged rule on the next scrape", func() {
		f := &fakeFetcher{rules: []prometheus.AlertingRule{sampleRule()}}
		em := &fakeEmitter{}
		s := scrape.New(f, em, nil)
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		Expect(em.emitted).To(HaveLen(2)) // instance + threshold

		em.emitted = nil
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		Expect(em.emitted).To(BeEmpty())
	})

	It("re-emits when the query changes, keeping the same ids", func() {
		f := &fakeFetcher{rules: []prometheus.AlertingRule{sampleRule()}}
		em := &fakeEmitter{}
		s := scrape.New(f, em, nil)
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		firstMI := eventsOfType(em.emitted, events.MetricInstanceResource)[0].ResourceId

		changed := sampleRule()
		changed.Query = "histogram_quantile(0.99, x) > 0.9"
		f.rules = []prometheus.AlertingRule{changed}
		em.emitted = nil
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		mi := eventsOfType(em.emitted, events.MetricInstanceResource)
		Expect(mi).To(HaveLen(1))
		Expect(mi[0].ResourceId).To(Equal(firstMI))
		Expect(onlyTh(em.emitted).GetAnnotations().GetValue(eval.AnnotationThresholdLimit)).To(Equal("0.9"))
	})

	It("keeps the same ids when only labels change (identity is group+name)", func() {
		f := &fakeFetcher{rules: []prometheus.AlertingRule{sampleRule()}}
		em := &fakeEmitter{}
		s := scrape.New(f, em, nil)
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		firstMI := eventsOfType(em.emitted, events.MetricInstanceResource)[0].ResourceId

		relabeled := sampleRule()
		relabeled.Labels = map[string]string{"severity": "warning"} // only labels differ
		f.rules = []prometheus.AlertingRule{relabeled}
		em.emitted = nil
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		// Label-only change does not change the signature, so nothing is re-emitted
		// and the id is unchanged (no orphan/duplicate).
		Expect(em.emitted).To(BeEmpty())
		_ = firstMI
	})

	It("deletes both resources when a rule disappears", func() {
		f := &fakeFetcher{rules: []prometheus.AlertingRule{sampleRule()}}
		em := &fakeEmitter{}
		s := scrape.New(f, em, nil)
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		miID := eventsOfType(em.emitted, events.MetricInstanceResource)[0].ResourceId
		thID := eventsOfType(em.emitted, events.ThresholdResource)[0].ResourceId

		f.rules = nil
		em.emitted = nil
		Expect(s.ScrapeOnce(ctx)).To(Succeed())

		delTh := eventsOfType(em.emitted, events.ThresholdResource)
		delMI := eventsOfType(em.emitted, events.MetricInstanceResource)
		Expect(delTh).To(HaveLen(1))
		Expect(delTh[0].Operation).To(Equal(events.DeleteOperation))
		Expect(delTh[0].ResourceId).To(Equal(thID))
		Expect(delMI).To(HaveLen(1))
		Expect(delMI[0].Operation).To(Equal(events.DeleteOperation))
		Expect(delMI[0].ResourceId).To(Equal(miID))
	})

	It("retries a rule whose threshold emit failed (instance not recorded as done)", func() {
		f := &fakeFetcher{rules: []prometheus.AlertingRule{sampleRule()}}
		em := &fakeEmitter{failType: events.ThresholdResource}
		s := scrape.New(f, em, nil)

		// First scrape: instance emits, threshold fails -> not recorded.
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		Expect(eventsOfType(em.emitted, events.ThresholdResource)).To(BeEmpty())

		// Recover; next scrape must retry (emit instance + threshold), not skip.
		em.failType = events.UnknownResourceType
		em.emitted = nil
		Expect(s.ScrapeOnce(ctx)).To(Succeed())
		Expect(eventsOfType(em.emitted, events.MetricInstanceResource)).To(HaveLen(1))
		Expect(eventsOfType(em.emitted, events.ThresholdResource)).To(HaveLen(1))
	})

	It("propagates fetch errors", func() {
		s := scrape.New(&fakeFetcher{err: errors.New("rules down")}, &fakeEmitter{}, nil)
		Expect(s.ScrapeOnce(ctx)).To(MatchError(ContainSubstring("rules down")))
	})
})
