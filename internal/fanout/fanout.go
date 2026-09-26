// Package fanout turns manually-authored [observability.Metric] definitions
// into per-group [observability.MetricInstance] resources and their current
// [observability.MetricValue] readings.
//
// A Metric names an underlying Prometheus time series (source), an aggregation
// function (sum/avg/min/max/count) and a set of grouping labels (group-by,
// default namespace + service). On every poll the sensor runs an aggregated
// instant query — `<agg> by (<group-by>) (<source>)` — and fans the result out:
// one MetricInstance per returned label-set group, each with a MetricValue
// carrying that group's scalar reading.
//
// The sensor is the sole author of these derived MetricInstances and
// MetricValues. Ids are deterministic (derived from the Metric id and the
// group's label values), so repeated polls upsert in place. Groups that
// disappear from Prometheus are reconciled away with delete events.
package fanout

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"emeland.io/modelsrv-prometheus-sensor/internal/prometheus"

	"go.emeland.io/modelsrv/pkg/events"
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"
)

// Well-known annotation keys read from a Metric to drive the fan-out. See the
// modelsrv observability annotation registry.
const (
	// AnnotationSource names the Prometheus time series a Metric fans out from
	// (e.g. "emeland_resource_count"). A Metric without this key is ignored.
	AnnotationSource = "emeland.io/metric.source"
	// AnnotationAggregation names the aggregation function used to reduce each
	// group to a scalar: sum, avg, min, max or count. Defaults to sum.
	AnnotationAggregation = "emeland.io/metric.aggregation"
	// AnnotationGroupBy is a comma-separated list of label names the source
	// series is grouped by; one MetricInstance is produced per distinct
	// combination. Defaults to "namespace,job".
	AnnotationGroupBy = "emeland.io/metric.group-by"

	// AnnotationExpression records the concrete PromQL expression on a derived
	// MetricInstance (the aggregated query scoped to that instance's group).
	AnnotationExpression = "emeland.io/metric.expression"
	// AnnotationLanguage names the expression language on a derived instance.
	AnnotationLanguage = "emeland.io/metric.language"
	// LanguagePromQL is the value written to AnnotationLanguage.
	LanguagePromQL = "promql"
)

// DefaultGroupBy is used when a Metric does not set AnnotationGroupBy.
var DefaultGroupBy = []string{"namespace", "job"}

// DefaultAggregation is used when a Metric does not set AnnotationAggregation.
const DefaultAggregation = "sum"

// supportedAggregations is the set of PromQL aggregation operators this sensor
// will place in a generated query.
var supportedAggregations = map[string]bool{
	"sum":   true,
	"avg":   true,
	"min":   true,
	"max":   true,
	"count": true,
}

var (
	// instanceNamespace anchors the deterministic UUIDv5 for a derived
	// MetricInstance (from the Metric id + the group's label values).
	instanceNamespace = uuid.MustParse("a1c4f3e2-8b6d-5f01-9e2a-3c7d8f0b1a45")
	// valueNamespace anchors the deterministic UUIDv5 for a MetricValue,
	// derived from its MetricInstance id.
	valueNamespace = uuid.MustParse("6b8d0b0e-1d3a-5e2b-9c4f-2a1d7c9e5f00")
)

// Source provides the manually-authored Metric definitions to fan out. They are
// authored upstream (e.g. via git-sensor) and pushed into the sensor's local
// model; this sensor reads them, it does not create them.
type Source interface {
	GetMetrics() ([]mdlobs.Metric, error)
}

// Emitter forwards a domain event (to the local model and downstream subscribers).
type Emitter interface {
	Emit(events.Event) error
}

// Querier runs an instant PromQL query and returns each result series.
type Querier interface {
	QueryVector(ctx context.Context, query string) ([]prometheus.Sample, error)
}

// Evaluator fans each Metric out into per-group MetricInstances and MetricValues.
type Evaluator struct {
	source  Source
	querier Querier
	emitter Emitter
	log     *zap.SugaredLogger

	// instances tracks, per Metric id, the set of MetricInstance ids currently
	// derived from it, so groups that disappear can be reconciled away.
	instances map[uuid.UUID]map[uuid.UUID]struct{}
	// seenValues tracks the last emitted value per MetricValue id to choose
	// Create vs Update and skip unchanged readings.
	seenValues map[uuid.UUID]string
	// seenInstanceSig tracks the last emitted "signature" per MetricInstance id
	// (display name + expression) to skip unchanged instance re-emits.
	seenInstanceSig map[uuid.UUID]string
}

// New builds an Evaluator.
func New(source Source, querier Querier, emitter Emitter, log *zap.SugaredLogger) *Evaluator {
	if log == nil {
		log = zap.NewNop().Sugar()
	}
	return &Evaluator{
		source:          source,
		querier:         querier,
		emitter:         emitter,
		log:             log,
		instances:       make(map[uuid.UUID]map[uuid.UUID]struct{}),
		seenValues:      make(map[uuid.UUID]string),
		seenInstanceSig: make(map[uuid.UUID]string),
	}
}

// EvaluateOnce fans out every eligible Metric a single time. Individual Metric
// failures are logged so one bad Metric does not stop the rest.
func (e *Evaluator) EvaluateOnce(ctx context.Context) error {
	metrics, err := e.source.GetMetrics()
	if err != nil {
		return fmt.Errorf("list metrics: %w", err)
	}
	for _, m := range metrics {
		e.evaluateMetric(ctx, m)
	}
	return nil
}

// evaluateMetric fans a single Metric out into per-group MetricInstances and
// MetricValues, then reconciles away any groups that disappeared.
func (e *Evaluator) evaluateMetric(ctx context.Context, m mdlobs.Metric) {
	metricID := m.GetMetricId()
	ann := m.GetAnnotations()
	if ann == nil {
		return
	}

	source := strings.TrimSpace(ann.GetValue(AnnotationSource))
	if source == "" {
		// Not a fan-out Metric (abstract grouping only); nothing to do.
		return
	}

	agg := strings.TrimSpace(ann.GetValue(AnnotationAggregation))
	if agg == "" {
		agg = DefaultAggregation
	}
	if !supportedAggregations[agg] {
		e.log.Warnw("skipping metric with unsupported aggregation",
			"metricId", metricID, "aggregation", agg)
		return
	}

	groupBy := parseGroupBy(ann.GetValue(AnnotationGroupBy))

	query := buildAggregateQuery(agg, groupBy, source)
	samples, err := e.querier.QueryVector(ctx, query)
	if err != nil {
		e.log.Warnw("metric source query failed",
			"metricId", metricID, "query", query, "error", err)
		return
	}

	seen := make(map[uuid.UUID]struct{}, len(samples))
	for _, s := range samples {
		instanceID := e.upsertGroup(m, agg, source, groupBy, s)
		if instanceID != uuid.Nil {
			seen[instanceID] = struct{}{}
		}
	}

	e.reconcile(metricID, seen)
	e.instances[metricID] = seen
}

// upsertGroup emits (or updates) the MetricInstance and MetricValue for one
// group sample. It returns the derived MetricInstance id (or uuid.Nil on emit
// failure so the group is not recorded as present).
func (e *Evaluator) upsertGroup(m mdlobs.Metric, agg, source string, groupBy []string, s prometheus.Sample) uuid.UUID {
	metricID := m.GetMetricId()
	instanceID := instanceIDFor(metricID, s.Labels)
	valueID := valueIDFor(instanceID)

	expr := buildGroupQuery(agg, groupBy, source, s.Labels)
	name := instanceDisplayName(m.GetDisplayName(), s.Labels, groupBy)

	// MetricInstance: emit on first sight or when name/expression changed.
	sig := name + "\x00" + expr
	if prev, ok := e.seenInstanceSig[instanceID]; !ok || prev != sig {
		op := events.CreateOperation
		if ok {
			op = events.UpdateOperation
		}
		mi := buildMetricInstance(instanceID, metricID, name, expr, s.Labels)
		if err := e.emitter.Emit(events.Event{
			ResourceType: events.MetricInstanceResource,
			Operation:    op,
			ResourceId:   instanceID,
			Objects:      []any{mi},
		}); err != nil {
			e.log.Errorw("emit metric instance failed",
				"metricId", metricID, "metricInstanceId", instanceID, "error", err)
			return uuid.Nil
		}
		e.seenInstanceSig[instanceID] = sig
		e.log.Infow("emitted metric instance",
			"metricId", metricID, "metricInstanceId", instanceID,
			"operation", op.WireOperation(), "labels", s.Labels)
	}

	// MetricValue: emit on first sight or when the reading changed.
	if prev, ok := e.seenValues[valueID]; !ok || prev != s.Value {
		op := events.CreateOperation
		if ok {
			op = events.UpdateOperation
		}
		mv := buildMetricValue(valueID, instanceID, name, s.Value)
		if err := e.emitter.Emit(events.Event{
			ResourceType: events.MetricValueResource,
			Operation:    op,
			ResourceId:   valueID,
			Objects:      []any{mv},
		}); err != nil {
			e.log.Errorw("emit metric value failed",
				"metricInstanceId", instanceID, "metricValueId", valueID, "error", err)
			// Instance already emitted; still record it as present.
			return instanceID
		}
		e.seenValues[valueID] = s.Value
		e.log.Infow("emitted metric value",
			"metricInstanceId", instanceID, "metricValueId", valueID,
			"operation", op.WireOperation(), "value", s.Value)
	}

	return instanceID
}

// reconcile deletes MetricInstances (and their MetricValues) that were derived
// from a Metric on a previous poll but are absent from this poll's groups.
func (e *Evaluator) reconcile(metricID uuid.UUID, seen map[uuid.UUID]struct{}) {
	prev := e.instances[metricID]
	for instanceID := range prev {
		if _, ok := seen[instanceID]; ok {
			continue
		}
		valueID := valueIDFor(instanceID)

		// Delete the MetricValue first, then its MetricInstance.
		if err := e.emitter.Emit(events.Event{
			ResourceType: events.MetricValueResource,
			Operation:    events.DeleteOperation,
			ResourceId:   valueID,
		}); err != nil {
			e.log.Errorw("emit metric value delete failed",
				"metricValueId", valueID, "error", err)
			continue
		}
		delete(e.seenValues, valueID)

		if err := e.emitter.Emit(events.Event{
			ResourceType: events.MetricInstanceResource,
			Operation:    events.DeleteOperation,
			ResourceId:   instanceID,
		}); err != nil {
			e.log.Errorw("emit metric instance delete failed",
				"metricInstanceId", instanceID, "error", err)
			continue
		}
		delete(e.seenInstanceSig, instanceID)
		e.log.Infow("reconciled away disappeared group",
			"metricId", metricID, "metricInstanceId", instanceID)
	}
}

// buildMetricInstance constructs a derived MetricInstance for one group.
func buildMetricInstance(instanceID, metricID uuid.UUID, name, expr string, labels map[string]string) mdlobs.MetricInstance {
	mi := mdlobs.NewMetricInstance(instanceID)
	mi.SetMetricById(metricID)
	if name != "" {
		mi.SetDisplayName(name)
	}
	ann := mi.GetAnnotations()
	ann.Add(AnnotationExpression, expr)
	ann.Add(AnnotationLanguage, LanguagePromQL)
	// Record the group's labels so downstream consumers can identify the subject.
	for _, k := range sortedKeys(labels) {
		ann.Add("emeland.io/metric.label."+k, labels[k])
	}
	return mi
}

// buildMetricValue constructs the current reading for a group.
func buildMetricValue(valueID, instanceID uuid.UUID, name, value string) mdlobs.MetricValue {
	mv := mdlobs.NewMetricValue(valueID)
	mv.SetMetricInstanceById(instanceID)
	mv.SetValue(value)
	if name != "" {
		mv.SetDisplayName(name)
	}
	return mv
}

// instanceIDFor derives the deterministic MetricInstance id for a group from
// the Metric id and the group's sorted label key=value pairs.
func instanceIDFor(metricID uuid.UUID, labels map[string]string) uuid.UUID {
	var b strings.Builder
	b.WriteString(metricID.String())
	for _, k := range sortedKeys(labels) {
		b.WriteByte('\x00')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return uuid.NewSHA1(instanceNamespace, []byte(b.String()))
}

// valueIDFor derives the deterministic MetricValue id from its MetricInstance id.
func valueIDFor(instanceID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(valueNamespace, instanceID[:])
}

// buildAggregateQuery builds the discovery query enumerating all groups, e.g.
// `sum by (namespace, job) (emeland_resource_count)`.
func buildAggregateQuery(agg string, groupBy []string, source string) string {
	return fmt.Sprintf("%s by (%s) (%s)", agg, strings.Join(groupBy, ", "), source)
}

// buildGroupQuery builds the per-instance query scoped to one group's label
// values, e.g. `sum by (namespace, job) (emeland_resource_count{namespace="emeland-demo", job="emeland-demo-filter"})`.
// The selector is built from the group-by labels only (in declared order), so
// the `by (<group-by>)` clause and the `{<matchers>}` are drawn from the same
// set and cannot drift if a result ever carries extra labels.
func buildGroupQuery(agg string, groupBy []string, source string, labels map[string]string) string {
	matchers := make([]string, 0, len(groupBy))
	for _, k := range groupBy {
		if v, ok := labels[k]; ok {
			matchers = append(matchers, fmt.Sprintf("%s=%q", k, v))
		}
	}
	selector := source
	if len(matchers) > 0 {
		selector = fmt.Sprintf("%s{%s}", source, strings.Join(matchers, ", "))
	}
	return fmt.Sprintf("%s by (%s) (%s)", agg, strings.Join(groupBy, ", "), selector)
}

// instanceDisplayName builds a human-readable name from the Metric's name and
// the group's label values (in group-by order, then any extra labels sorted).
func instanceDisplayName(metricName string, labels map[string]string, groupBy []string) string {
	parts := make([]string, 0, len(labels))
	used := make(map[string]bool, len(groupBy))
	for _, k := range groupBy {
		if v, ok := labels[k]; ok {
			parts = append(parts, v)
			used[k] = true
		}
	}
	for _, k := range sortedKeys(labels) {
		if !used[k] {
			parts = append(parts, labels[k])
		}
	}
	base := metricName
	if base == "" {
		base = "metric"
	}
	if len(parts) == 0 {
		return base
	}
	return base + " (" + strings.Join(parts, "/") + ")"
}

// parseGroupBy splits a comma-separated group-by annotation into label names,
// falling back to DefaultGroupBy when empty.
func parseGroupBy(raw string) []string {
	fields := strings.Split(raw, ",")
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	if len(out) == 0 {
		return append([]string(nil), DefaultGroupBy...)
	}
	return out
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
