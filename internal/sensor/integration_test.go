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
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"

	"emeland.io/modelsrv-prometheus-sensor/internal/fanout"
	"emeland.io/modelsrv-prometheus-sensor/internal/prometheus"
	"emeland.io/modelsrv-prometheus-sensor/internal/sensor"
)

// stubQuerier returns a fixed instant-vector result, standing in for Prometheus.
type stubQuerier struct {
	samples []prometheus.Sample
	gotQ    string
}

func (s *stubQuerier) QueryVector(_ context.Context, query string) ([]prometheus.Sample, error) {
	s.gotQ = query
	return s.samples, nil
}

var _ = Describe("integration: fan a Metric out into MetricInstances and MetricValues", func() {
	It("receives a Metric pushed over HTTP and emits an instance+value per group", func() {
		log := zap.NewNop().Sugar()

		// Start our sensor on an ephemeral port; it exposes the real modelsrv
		// web endpoint including the replication push handler.
		srv, err := sensor.New("127.0.0.1:0", nil, log)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = srv.Close() }()

		addr := srv.Addr()
		Expect(addr).NotTo(BeNil())
		baseURL := fmt.Sprintf("http://%s/api/", addr.String())

		// Simulate the upstream node (e.g. git-sensor) pushing a manually
		// authored Metric that names a Prometheus series and an aggregation.
		metricID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
		m := mdlobs.NewMetric(metricID)
		m.SetDisplayName("Resource count")
		m.GetAnnotations().Add(fanout.AnnotationSource, "emeland_resource_count")
		m.GetAnnotations().Add(fanout.AnnotationAggregation, "sum")

		upstream, err := client.NewModelSrvClient(baseURL)
		Expect(err).NotTo(HaveOccurred())

		Expect(upstream.PostEvent(context.Background(), &events.Event{
			ResourceType: events.MetricResource,
			Operation:    events.CreateOperation,
			ResourceId:   metricID,
			Objects:      []any{m},
		})).To(Succeed())

		// The pushed Metric must land in our local model.
		Eventually(func() mdlobs.Metric {
			return srv.Model().GetMetricById(metricID)
		}, 2*time.Second, 20*time.Millisecond).ShouldNot(BeNil())

		// Two groups, matching our live Prometheus (filter + server jobs).
		q := &stubQuerier{samples: []prometheus.Sample{
			{Labels: map[string]string{"namespace": "emeland-demo", "job": "emeland-demo-filter"}, Value: "24"},
			{Labels: map[string]string{"namespace": "emeland-demo", "job": "emeland-demo-server"}, Value: "25"},
		}}

		evaluator := fanout.New(srv.Model(), q, srv, log)
		Expect(evaluator.EvaluateOnce(context.Background())).To(Succeed())

		// The discovery query aggregates by the default namespace,job group.
		Expect(q.gotQ).To(Equal("sum by (namespace, job) (emeland_resource_count)"))

		// One MetricInstance and one MetricValue per group.
		var miEvents, mvEvents []events.Event
		for _, ev := range srv.MasterEvents() {
			switch ev.ResourceType {
			case events.MetricInstanceResource:
				miEvents = append(miEvents, ev)
			case events.MetricValueResource:
				mvEvents = append(mvEvents, ev)
			}
		}
		Expect(miEvents).To(HaveLen(2))
		Expect(mvEvents).To(HaveLen(2))

		// Every derived MetricInstance references the parent Metric and is
		// persisted in the local model.
		for _, ev := range miEvents {
			Expect(ev.Operation).To(Equal(events.CreateOperation))
			mi, ok := ev.Objects[0].(mdlobs.MetricInstance)
			Expect(ok).To(BeTrue())
			Expect(mi.GetMetricId()).To(Equal(metricID))
			Expect(srv.Model().GetMetricInstanceById(mi.GetMetricInstanceId())).NotTo(BeNil())
		}

		// Values are persisted and carry the group readings.
		gotValues := map[string]bool{}
		for _, ev := range mvEvents {
			mv, ok := ev.Objects[0].(mdlobs.MetricValue)
			Expect(ok).To(BeTrue())
			Expect(srv.Model().GetMetricValueById(mv.GetMetricValueId())).NotTo(BeNil())
			gotValues[mv.GetValue()] = true
		}
		Expect(gotValues).To(HaveKey("24"))
		Expect(gotValues).To(HaveKey("25"))
	})

	It("reconciles away a MetricInstance whose group disappears", func() {
		log := zap.NewNop().Sugar()
		srv, err := sensor.New("127.0.0.1:0", nil, log)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = srv.Close() }()

		metricID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		m := mdlobs.NewMetric(metricID)
		m.SetDisplayName("Resource count")
		m.GetAnnotations().Add(fanout.AnnotationSource, "emeland_resource_count")
		Expect(srv.Model().AddMetric(m)).To(Succeed())

		q := &stubQuerier{samples: []prometheus.Sample{
			{Labels: map[string]string{"namespace": "emeland-demo", "service": "a"}, Value: "1"},
			{Labels: map[string]string{"namespace": "emeland-demo", "service": "b"}, Value: "2"},
		}}
		evaluator := fanout.New(srv.Model(), q, srv, log)
		Expect(evaluator.EvaluateOnce(context.Background())).To(Succeed())

		instances, err := srv.Model().GetMetricInstances()
		Expect(err).NotTo(HaveOccurred())
		Expect(instances).To(HaveLen(2))

		// Second poll: service "b" disappeared.
		q.samples = []prometheus.Sample{
			{Labels: map[string]string{"namespace": "emeland-demo", "service": "a"}, Value: "1"},
		}
		Expect(evaluator.EvaluateOnce(context.Background())).To(Succeed())

		Eventually(func() int {
			list, _ := srv.Model().GetMetricInstances()
			return len(list)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal(1))

		values, err := srv.Model().GetMetricValues()
		Expect(err).NotTo(HaveOccurred())
		Expect(values).To(HaveLen(1))
	})
})
