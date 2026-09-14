package eval_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/google/uuid"

	"go.emeland.io/modelsrv/pkg/events"
	mdlfinding "go.emeland.io/modelsrv/pkg/model/finding"
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"

	"emeland.io/modelsrv-prometheus-sensor/internal/eval"
)

func TestEval(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "eval")
}

type fakeSource struct {
	instances  []mdlobs.MetricInstance
	thresholds []mdlobs.Threshold
	err        error
	thErr      error
}

func (f *fakeSource) GetMetricInstances() ([]mdlobs.MetricInstance, error) {
	return f.instances, f.err
}
func (f *fakeSource) GetThresholds() ([]mdlobs.Threshold, error) { return f.thresholds, f.thErr }

type fakeQuerier struct {
	result string
	err    error
	calls  []string
}

func (f *fakeQuerier) QueryScalar(_ context.Context, q string) (string, error) {
	f.calls = append(f.calls, q)
	return f.result, f.err
}

type fakeEmitter struct {
	emitted []events.Event
	err     error
}

func (f *fakeEmitter) Emit(ev events.Event) error {
	if f.err != nil {
		return f.err
	}
	f.emitted = append(f.emitted, ev)
	return nil
}

// newMetricInstance builds a MetricInstance carrying (optionally) a PromQL
// expression and language annotation. This is what the sensor evaluates.
func newMetricInstance(name, expr, lang string) mdlobs.MetricInstance {
	mi := mdlobs.NewMetricInstance(uuid.New())
	if name != "" {
		mi.SetDisplayName(name)
	}
	if expr != "" {
		mi.GetAnnotations().Add(eval.AnnotationExpression, expr)
	}
	if lang != "" {
		mi.GetAnnotations().Add(eval.AnnotationLanguage, lang)
	}
	return mi
}

// newThreshold builds a Threshold referencing instanceID with the given
// operator and limit annotations.
func newThreshold(name string, instanceID uuid.UUID, operator, limit string) mdlobs.Threshold {
	t := mdlobs.NewThreshold(uuid.New())
	if name != "" {
		t.SetDisplayName(name)
	}
	t.SetMetricInstanceById(instanceID)
	if operator != "" {
		t.GetAnnotations().Add(eval.AnnotationThresholdOperator, operator)
	}
	if limit != "" {
		t.GetAnnotations().Add(eval.AnnotationThresholdLimit, limit)
	}
	return t
}

func findingEvents(evs []events.Event) []events.Event {
	var out []events.Event
	for _, ev := range evs {
		if ev.ResourceType == events.FindingResource {
			out = append(out, ev)
		}
	}
	return out
}

func metricValueEvents(evs []events.Event) []events.Event {
	var out []events.Event
	for _, ev := range evs {
		if ev.ResourceType == events.MetricValueResource {
			out = append(out, ev)
		}
	}
	return out
}

var _ = Describe("Evaluator.EvaluateOnce", func() {
	ctx := context.Background()

	It("emits a MetricValue Create for a metric instance with promql", func() {
		mi := newMetricInstance("p99 latency (payments)", "histogram_quantile(0.99, x)", "promql")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}}
		q := &fakeQuerier{result: "412"}
		em := &fakeEmitter{}
		ev := eval.New(src, q, em, nil)

		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(q.calls).To(Equal([]string{"histogram_quantile(0.99, x)"}))

		// The sensor does not create MetricInstances; it only emits MetricValues.
		mvs := metricValueEvents(em.emitted)
		Expect(mvs).To(HaveLen(1))
		Expect(mvs[0].Operation).To(Equal(events.CreateOperation))
		mv, ok := mvs[0].Objects[0].(mdlobs.MetricValue)
		Expect(ok).To(BeTrue())
		Expect(mv.GetValue()).To(Equal("412"))
		Expect(mv.GetMetricInstanceId()).To(Equal(mi.GetMetricInstanceId()))
		Expect(mv.GetDisplayName()).To(Equal("p99 latency (payments)"))
	})

	It("treats an instance without a language annotation as promql", func() {
		mi := newMetricInstance("m", "up", "")
		ev := eval.New(&fakeSource{instances: []mdlobs.MetricInstance{mi}}, &fakeQuerier{result: "1"}, &fakeEmitter{}, nil)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
	})

	It("skips instances without an expression", func() {
		mi := newMetricInstance("no-expr", "", "")
		q := &fakeQuerier{result: "1"}
		em := &fakeEmitter{}
		ev := eval.New(&fakeSource{instances: []mdlobs.MetricInstance{mi}}, q, em, nil)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(q.calls).To(BeEmpty())
		Expect(em.emitted).To(BeEmpty())
	})

	It("skips instances with a non-promql language", func() {
		mi := newMetricInstance("cel-instance", "a + b", "cel")
		q := &fakeQuerier{result: "1"}
		em := &fakeEmitter{}
		ev := eval.New(&fakeSource{instances: []mdlobs.MetricInstance{mi}}, q, em, nil)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(q.calls).To(BeEmpty())
		Expect(em.emitted).To(BeEmpty())
	})

	It("emits Update on change and skips unchanged readings", func() {
		mi := newMetricInstance("m", "up", "promql")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}}
		q := &fakeQuerier{result: "1"}
		em := &fakeEmitter{}
		ev := eval.New(src, q, em, nil)

		// First run: Create.
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		mvs := metricValueEvents(em.emitted)
		Expect(mvs).To(HaveLen(1))
		Expect(mvs[0].Operation).To(Equal(events.CreateOperation))

		// Second run, same value: no new MetricValue.
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(metricValueEvents(em.emitted)).To(HaveLen(1))

		// Third run, new value: Update with the same id.
		q.result = "2"
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		mvs = metricValueEvents(em.emitted)
		Expect(mvs).To(HaveLen(2))
		Expect(mvs[1].Operation).To(Equal(events.UpdateOperation))
		Expect(mvs[1].ResourceId).To(Equal(mvs[0].ResourceId))
	})

	It("uses a deterministic MetricValue id derived from the instance id", func() {
		mi := newMetricInstance("m", "up", "promql")

		run := func() uuid.UUID {
			em := &fakeEmitter{}
			ev := eval.New(&fakeSource{instances: []mdlobs.MetricInstance{mi}}, &fakeQuerier{result: "1"}, em, nil)
			Expect(ev.EvaluateOnce(ctx)).To(Succeed())
			return metricValueEvents(em.emitted)[0].ResourceId
		}
		first := run()
		second := run()
		Expect(first).To(Equal(second))
		Expect(first).NotTo(Equal(mi.GetMetricInstanceId()))
	})

	It("continues past a failing query", func() {
		good := newMetricInstance("good", "up", "promql")
		bad := newMetricInstance("bad", "boom", "promql")
		src := &fakeSource{instances: []mdlobs.MetricInstance{bad, good}}
		q := &fakeQuerier{err: errors.New("scrape failed")}
		em := &fakeEmitter{}
		ev := eval.New(src, q, em, nil)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(em.emitted).To(BeEmpty())
	})

	It("returns an error when listing metric instances fails", func() {
		src := &fakeSource{err: errors.New("model down")}
		ev := eval.New(src, &fakeQuerier{}, &fakeEmitter{}, nil)
		Expect(ev.EvaluateOnce(ctx)).To(MatchError(ContainSubstring("model down")))
	})
})

var _ = Describe("Evaluator threshold evaluation", func() {
	ctx := context.Background()

	It("derives the documented ThresholdBreached FindingType UUID", func() {
		// Locks the UUID hardcoded in examples/findingtype.yaml and the README
		// to the code, so documentation cannot silently drift.
		Expect(mdlfinding.TypeIDForKind(eval.ThresholdBreachedKind).String()).
			To(Equal("8c34ad70-b8e1-55bf-9423-48d904f5fd7a"))
	})

	It("emits a Finding when a threshold is breached, referencing the threshold and instance", func() {
		mi := newMetricInstance("latency", "up", "promql")
		th := newThreshold("latency too high", mi.GetMetricInstanceId(), "gt", "100")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thresholds: []mdlobs.Threshold{th}}
		q := &fakeQuerier{result: "150"} // 150 > 100 -> breached
		em := &fakeEmitter{}
		ev := eval.New(src, q, em, nil)

		Expect(ev.EvaluateOnce(ctx)).To(Succeed())

		fes := findingEvents(em.emitted)
		Expect(fes).To(HaveLen(1))
		Expect(fes[0].Operation).To(Equal(events.CreateOperation))

		f, ok := fes[0].Objects[0].(mdlfinding.Finding)
		Expect(ok).To(BeTrue())
		Expect(f.GetFindingTypeId()).To(Equal(mdlfinding.TypeIDForKind(eval.ThresholdBreachedKind)))
		res := f.GetResources()
		Expect(res).To(HaveLen(2))
		Expect(res[0].ResourceId).To(Equal(th.GetThresholdId()))
		Expect(res[0].ResourceType).To(Equal(events.ThresholdResource))
		Expect(res[1].ResourceId).To(Equal(mi.GetMetricInstanceId()))
		Expect(res[1].ResourceType).To(Equal(events.MetricInstanceResource))
	})

	It("does not emit a Finding when the threshold is not breached", func() {
		mi := newMetricInstance("latency", "up", "promql")
		th := newThreshold("latency too high", mi.GetMetricInstanceId(), "gt", "100")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thresholds: []mdlobs.Threshold{th}}
		q := &fakeQuerier{result: "50"} // 50 > 100 is false
		em := &fakeEmitter{}
		ev := eval.New(src, q, em, nil)

		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(findingEvents(em.emitted)).To(BeEmpty())
	})

	It("emits the Finding once on breach, then a Delete when it clears", func() {
		mi := newMetricInstance("latency", "up", "promql")
		th := newThreshold("latency too high", mi.GetMetricInstanceId(), "gt", "100")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thresholds: []mdlobs.Threshold{th}}
		q := &fakeQuerier{result: "150"}
		em := &fakeEmitter{}
		ev := eval.New(src, q, em, nil)

		// First poll: breach -> Create.
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		fes := findingEvents(em.emitted)
		Expect(fes).To(HaveLen(1))
		Expect(fes[0].Operation).To(Equal(events.CreateOperation))
		createdID := fes[0].ResourceId

		// Second poll: still breached, unchanged -> no new finding event.
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(findingEvents(em.emitted)).To(HaveLen(1))

		// Third poll: value drops -> Delete of the same finding id.
		q.result = "50"
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		fes = findingEvents(em.emitted)
		Expect(fes).To(HaveLen(2))
		Expect(fes[1].Operation).To(Equal(events.DeleteOperation))
		Expect(fes[1].ResourceId).To(Equal(createdID))
	})

	It("uses a deterministic finding id across evaluators", func() {
		mi := newMetricInstance("latency", "up", "promql")
		th := newThreshold("t", mi.GetMetricInstanceId(), "gt", "100")

		run := func() uuid.UUID {
			src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thresholds: []mdlobs.Threshold{th}}
			em := &fakeEmitter{}
			ev := eval.New(src, &fakeQuerier{result: "150"}, em, nil)
			Expect(ev.EvaluateOnce(ctx)).To(Succeed())
			return findingEvents(em.emitted)[0].ResourceId
		}
		Expect(run()).To(Equal(run()))
	})

	It("skips thresholds with a missing operator or limit", func() {
		mi := newMetricInstance("latency", "up", "promql")
		th := newThreshold("t", mi.GetMetricInstanceId(), "gt", "") // no limit
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thresholds: []mdlobs.Threshold{th}}
		em := &fakeEmitter{}
		ev := eval.New(src, &fakeQuerier{result: "150"}, em, nil)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(findingEvents(em.emitted)).To(BeEmpty())
	})

	It("skips a threshold whose instance produced no value this poll", func() {
		// Instance has no expression, so it yields no current value.
		mi := newMetricInstance("latency", "", "")
		th := newThreshold("t", mi.GetMetricInstanceId(), "gt", "100")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thresholds: []mdlobs.Threshold{th}}
		em := &fakeEmitter{}
		ev := eval.New(src, &fakeQuerier{result: "150"}, em, nil)
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(findingEvents(em.emitted)).To(BeEmpty())
	})

	It("returns an error when listing thresholds fails", func() {
		mi := newMetricInstance("latency", "up", "promql")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thErr: errors.New("threshold store down")}
		ev := eval.New(src, &fakeQuerier{result: "1"}, &fakeEmitter{}, nil)
		Expect(ev.EvaluateOnce(ctx)).To(MatchError(ContainSubstring("threshold store down")))
	})

	It("retries the breach Finding on a later poll when the first emit fails (no orphan Delete)", func() {
		mi := newMetricInstance("latency", "up", "promql")
		th := newThreshold("t", mi.GetMetricInstanceId(), "gt", "100")
		src := &fakeSource{instances: []mdlobs.MetricInstance{mi}, thresholds: []mdlobs.Threshold{th}}
		q := &fakeQuerier{result: "150"} // breached
		em := &flakyEmitter{failFindings: true}
		ev := eval.New(src, q, em, nil)

		// First poll: the Finding Create emit fails; breach state must NOT be
		// recorded, so no Finding is considered created.
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		Expect(findingEvents(em.recorded)).To(BeEmpty())

		// Recover the emitter. Next poll (still breached) must retry with a
		// Create, not skip it and not emit a Delete.
		em.failFindings = false
		Expect(ev.EvaluateOnce(ctx)).To(Succeed())
		fes := findingEvents(em.recorded)
		Expect(fes).To(HaveLen(1))
		Expect(fes[0].Operation).To(Equal(events.CreateOperation))
	})
})

// flakyEmitter records only successful emits and can be configured to fail
// Finding emits, to exercise the retry-on-failure path.
type flakyEmitter struct {
	failFindings bool
	recorded     []events.Event
}

func (f *flakyEmitter) Emit(ev events.Event) error {
	if f.failFindings && ev.ResourceType == events.FindingResource {
		return errors.New("downstream unavailable")
	}
	f.recorded = append(f.recorded, ev)
	return nil
}
