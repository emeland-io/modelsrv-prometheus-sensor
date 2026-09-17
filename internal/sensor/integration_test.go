package sensor_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"go.emeland.io/modelsrv/pkg/client"
	"go.emeland.io/modelsrv/pkg/events"
	mdlfinding "go.emeland.io/modelsrv/pkg/model/finding"
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"

	"emeland.io/modelsrv-prometheus-sensor/internal/eval"
	"emeland.io/modelsrv-prometheus-sensor/internal/sensor"
)

// stubQuerier is a fixed PromQL result, standing in for Prometheus.
type stubQuerier struct{ value string }

func (s stubQuerier) QueryScalar(_ context.Context, _ string) (string, error) {
	return s.value, nil
}

var _ = Describe("integration: consume MetricInstance from upstream, emit MetricValue", func() {
	It("receives a MetricInstance pushed over HTTP and emits a MetricValue for it", func() {
		log := zap.NewNop().Sugar()

		// Start our sensor on an ephemeral port; it exposes the real modelsrv
		// web endpoint including POST /api/events/push.
		srv, err := sensor.New("127.0.0.1:0", nil, log)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = srv.Close() }()

		addr := srv.Addr()
		Expect(addr).NotTo(BeNil())
		baseURL := fmt.Sprintf("http://%s/api/", addr.String())

		// Simulate the upstream node (e.g. git-sensor or an AlertManager
		// scraper) pushing a MetricInstance carrying a PromQL expression over
		// the genuine replication wire. No parent Metric is needed (the ref is
		// optional), mirroring a scraped instance.
		instanceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
		mi := mdlobs.NewMetricInstance(instanceID)
		mi.SetDisplayName("Up targets")
		mi.GetAnnotations().Add(eval.AnnotationExpression, "sum(up)")
		mi.GetAnnotations().Add(eval.AnnotationLanguage, eval.LanguagePromQL)

		upstream, err := client.NewModelSrvClient(baseURL)
		Expect(err).NotTo(HaveOccurred())

		Expect(upstream.PostEvent(context.Background(), &events.Event{
			ResourceType: events.MetricInstanceResource,
			Operation:    events.CreateOperation,
			ResourceId:   instanceID,
			Objects:      []any{mi},
		})).To(Succeed())

		// The pushed MetricInstance must land in our local model.
		Eventually(func() mdlobs.MetricInstance {
			return srv.Model().GetMetricInstanceById(instanceID)
		}, 2*time.Second, 20*time.Millisecond).ShouldNot(BeNil())

		// Run the real evaluator over the sensor's model with a stubbed
		// Prometheus, emitting through the real sensor server.
		evaluator := eval.New(srv.Model(), stubQuerier{value: "42"}, srv, log)
		Expect(evaluator.EvaluateOnce(context.Background())).To(Succeed())

		// The sensor does not create MetricInstances; it emits a MetricValue
		// referencing the pushed instance.
		var mvEvents []events.Event
		for _, ev := range srv.MasterEvents() {
			if ev.ResourceType == events.MetricValueResource {
				mvEvents = append(mvEvents, ev)
			}
		}
		Expect(mvEvents).To(HaveLen(1))
		Expect(mvEvents[0].Operation).To(Equal(events.CreateOperation))
		mv, ok := mvEvents[0].Objects[0].(mdlobs.MetricValue)
		Expect(ok).To(BeTrue())
		Expect(mv.GetValue()).To(Equal("42"))
		Expect(mv.GetMetricInstanceId()).To(Equal(instanceID))

		// The MetricValue is persisted in the local model (AddMetricValue
		// validated the MetricInstance ref, proving the consume->produce link).
		Expect(srv.Model().GetMetricValueById(mv.GetMetricValueId())).NotTo(BeNil())
	})

	It("receives a MetricInstance and Threshold from upstream and emits a breach Finding", func() {
		log := zap.NewNop().Sugar()
		srv, err := sensor.New("127.0.0.1:0", nil, log)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = srv.Close() }()

		baseURL := fmt.Sprintf("http://%s/api/", srv.Addr().String())
		upstream, err := client.NewModelSrvClient(baseURL)
		Expect(err).NotTo(HaveOccurred())

		// Push the MetricInstance first (the Threshold references it; the model
		// rejects a Threshold whose MetricInstance is absent).
		instanceID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		mi := mdlobs.NewMetricInstance(instanceID)
		mi.SetDisplayName("latency")
		mi.GetAnnotations().Add(eval.AnnotationExpression, "latency_query")
		mi.GetAnnotations().Add(eval.AnnotationLanguage, eval.LanguagePromQL)
		Expect(upstream.PostEvent(context.Background(), &events.Event{
			ResourceType: events.MetricInstanceResource, Operation: events.CreateOperation,
			ResourceId: instanceID, Objects: []any{mi},
		})).To(Succeed())
		Eventually(func() mdlobs.MetricInstance { return srv.Model().GetMetricInstanceById(instanceID) },
			2*time.Second, 20*time.Millisecond).ShouldNot(BeNil())

		// Push a Threshold: breach when the instance value > 100.
		thresholdID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
		th := mdlobs.NewThreshold(thresholdID)
		th.SetDisplayName("latency too high")
		th.SetMetricInstanceById(instanceID)
		th.GetAnnotations().Add(eval.AnnotationThresholdOperator, "gt")
		th.GetAnnotations().Add(eval.AnnotationThresholdLimit, "100")
		Expect(upstream.PostEvent(context.Background(), &events.Event{
			ResourceType: events.ThresholdResource, Operation: events.CreateOperation,
			ResourceId: thresholdID, Objects: []any{th},
		})).To(Succeed())
		Eventually(func() mdlobs.Threshold { return srv.Model().GetThresholdById(thresholdID) },
			2*time.Second, 20*time.Millisecond).ShouldNot(BeNil())

		// Instance evaluates to 150 (> 100) -> breach -> Finding emitted.
		evaluator := eval.New(srv.Model(), stubQuerier{value: "150"}, srv, log)
		Expect(evaluator.EvaluateOnce(context.Background())).To(Succeed())

		var findings []events.Event
		for _, ev := range srv.MasterEvents() {
			if ev.ResourceType == events.FindingResource {
				findings = append(findings, ev)
			}
		}
		Expect(findings).To(HaveLen(1))
		Expect(findings[0].Operation).To(Equal(events.CreateOperation))

		f, ok := findings[0].Objects[0].(mdlfinding.Finding)
		Expect(ok).To(BeTrue())
		Expect(f.GetResources()[0].ResourceId).To(Equal(thresholdID))
		Expect(f.GetResources()[1].ResourceId).To(Equal(instanceID))
		// The Finding is persisted in the local model.
		Expect(srv.Model().GetFindingById(f.GetFindingId())).NotTo(BeNil())
	})
})
