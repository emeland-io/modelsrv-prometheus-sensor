package eval

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"go.emeland.io/modelsrv/pkg/events"
	"go.emeland.io/modelsrv/pkg/model/common"
	mdlfinding "go.emeland.io/modelsrv/pkg/model/finding"
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"
)

// Observability annotation keys read from a MetricInstance and a Threshold. See
// modelsrv docs/observability-annotations.md.
const (
	// AnnotationExpression holds the PromQL query on a MetricInstance.
	AnnotationExpression = "emeland.io/metric.expression"
	// AnnotationLanguage names the expression language; only "promql" is handled.
	AnnotationLanguage = "emeland.io/metric.language"

	// LanguagePromQL is the expected value of AnnotationLanguage.
	LanguagePromQL = "promql"

	// AnnotationThresholdOperator holds the comparison operator a Threshold
	// applies to its MetricInstance's current value: gt, ge, lt, le, eq or ne.
	AnnotationThresholdOperator = "emeland.io/threshold.operator"
	// AnnotationThresholdLimit holds the numeric bound the operator compares against.
	AnnotationThresholdLimit = "emeland.io/threshold.limit"
)

// ThresholdBreachedKind is the sensor-defined finding kind emitted when a
// Threshold's condition is met by its MetricInstance's current value. Its
// FindingType UUID is derived the same way as modelsrv's built-in kinds.
const ThresholdBreachedKind mdlfinding.FindingKind = "ThresholdBreached"

var (
	// metricValueNamespace anchors the deterministic UUIDv5 derived from a
	// MetricInstance id, so repeated evaluations address the same MetricValue.
	metricValueNamespace = uuid.MustParse("6b8d0b0e-1d3a-5e2b-9c4f-2a1d7c9e5f00")

	// findingNamespace anchors the deterministic UUIDv5 derived from a
	// Threshold id + kind, matching modelsrv's finding UUID scheme intent so
	// re-emits upsert rather than duplicate.
	findingNamespace = uuid.MustParse("7a3f2c1e-4b8d-5e9f-a0b1-c2d3e4f56789")
)

// Querier runs a PromQL query and returns the scalar result as a string.
type Querier interface {
	QueryScalar(ctx context.Context, query string) (string, error)
}

// Source provides the MetricInstance and Threshold definitions to evaluate.
// Both are authored upstream (e.g. via git-sensor) or scraped; this sensor only
// reads them, it does not create them.
type Source interface {
	GetMetricInstances() ([]mdlobs.MetricInstance, error)
	GetThresholds() ([]mdlobs.Threshold, error)
}

// Emitter forwards a domain event to downstream subscribers.
type Emitter interface {
	Emit(events.Event) error
}

// Evaluator runs each MetricInstance's PromQL query, emits MetricValue events,
// and evaluates Thresholds into Finding events on breach.
type Evaluator struct {
	source  Source
	querier Querier
	emitter Emitter
	log     *zap.SugaredLogger

	// seenValues tracks the last emitted value per MetricInstance id to choose
	// Create vs Update and skip unchanged MetricValue readings.
	seenValues map[uuid.UUID]string
	// breached tracks Thresholds currently in the breached state so a Finding
	// is emitted once on breach and deleted once when it clears.
	breached map[uuid.UUID]bool
}

// New builds an Evaluator.
func New(source Source, querier Querier, emitter Emitter, log *zap.SugaredLogger) *Evaluator {
	if log == nil {
		log = zap.NewNop().Sugar()
	}
	return &Evaluator{
		source:     source,
		querier:    querier,
		emitter:    emitter,
		log:        log,
		seenValues: make(map[uuid.UUID]string),
		breached:   make(map[uuid.UUID]bool),
	}
}

// EvaluateOnce evaluates every eligible MetricInstance a single time, emitting
// MetricValues, then evaluates every Threshold against the freshly computed
// values, emitting or clearing Findings. Individual failures are logged so one
// bad MetricInstance or Threshold does not stop the rest.
func (e *Evaluator) EvaluateOnce(ctx context.Context) error {
	instances, err := e.source.GetMetricInstances()
	if err != nil {
		return fmt.Errorf("list metric instances: %w", err)
	}

	// currentValues holds this poll's numeric value per MetricInstance id, for
	// instances that evaluated successfully. Thresholds are compared against these.
	currentValues := make(map[uuid.UUID]float64, len(instances))
	for _, mi := range instances {
		if value, ok := e.evaluateMetricInstance(ctx, mi); ok {
			if f, perr := strconv.ParseFloat(value, 64); perr == nil {
				currentValues[mi.GetMetricInstanceId()] = f
			}
		}
	}

	thresholds, err := e.source.GetThresholds()
	if err != nil {
		return fmt.Errorf("list thresholds: %w", err)
	}
	for _, t := range thresholds {
		e.evaluateThreshold(t, currentValues)
	}
	return nil
}

// evaluateMetricInstance queries the MetricInstance's PromQL expression and
// emits a MetricValue when the reading changed. It returns the current value
// string and whether a value was obtained (so Thresholds can be compared even
// when the value is unchanged). MetricInstances without a PromQL expression are
// skipped (they may be populated by other means).
func (e *Evaluator) evaluateMetricInstance(ctx context.Context, mi mdlobs.MetricInstance) (string, bool) {
	instanceID := mi.GetMetricInstanceId()
	ann := mi.GetAnnotations()
	if ann == nil {
		return "", false
	}

	expr := ann.GetValue(AnnotationExpression)
	if expr == "" {
		// Not a query-backed instance; nothing for this sensor to do.
		return "", false
	}
	if lang := ann.GetValue(AnnotationLanguage); lang != "" && lang != LanguagePromQL {
		e.log.Debugw("skipping metric instance with non-promql language",
			"metricInstanceId", instanceID, "language", lang)
		return "", false
	}

	value, err := e.querier.QueryScalar(ctx, expr)
	if err != nil {
		e.log.Warnw("metric instance query failed",
			"metricInstanceId", instanceID, "expression", expr, "error", err)
		return "", false
	}

	prev, existed := e.seenValues[instanceID]
	if !existed || prev != value {
		op := events.CreateOperation
		if existed {
			op = events.UpdateOperation
		}
		mv := buildMetricValue(instanceID, mi.GetDisplayName(), value)
		ev := events.Event{
			ResourceType: events.MetricValueResource,
			Operation:    op,
			ResourceId:   mv.GetMetricValueId(),
			Objects:      []any{mv},
		}
		if err := e.emitter.Emit(ev); err != nil {
			e.log.Errorw("emit metric value failed",
				"metricInstanceId", instanceID, "metricValueId", mv.GetMetricValueId(), "error", err)
			return "", false
		}
		e.seenValues[instanceID] = value
		e.log.Infow("emitted metric value",
			"metricInstanceId", instanceID, "metricValueId", mv.GetMetricValueId(),
			"operation", op.WireOperation(), "value", value)
	}

	return value, true
}

// evaluateThreshold compares a Threshold's MetricInstance value against its
// configured operator+limit. On the transition into breach it emits a Finding;
// on the transition out of breach it deletes that Finding. Thresholds whose
// MetricInstance has no current value this poll are left in their previous state.
func (e *Evaluator) evaluateThreshold(t mdlobs.Threshold, currentValues map[uuid.UUID]float64) {
	thresholdID := t.GetThresholdId()
	instanceID := t.GetMetricInstanceId()
	if instanceID == uuid.Nil {
		e.log.Debugw("skipping threshold without a metric instance reference", "thresholdId", thresholdID)
		return
	}
	ann := t.GetAnnotations()
	if ann == nil {
		return
	}

	opRaw := ann.GetValue(AnnotationThresholdOperator)
	limitRaw := ann.GetValue(AnnotationThresholdLimit)
	if opRaw == "" || limitRaw == "" {
		e.log.Debugw("skipping threshold without operator/limit annotations",
			"thresholdId", thresholdID)
		return
	}
	limit, err := strconv.ParseFloat(limitRaw, 64)
	if err != nil {
		e.log.Warnw("threshold has non-numeric limit", "thresholdId", thresholdID, "limit", limitRaw)
		return
	}

	value, ok := currentValues[instanceID]
	if !ok {
		// No fresh value for the referenced MetricInstance this poll; leave state as is.
		e.log.Debugw("no current value for threshold's metric instance; leaving state unchanged",
			"thresholdId", thresholdID, "metricInstanceId", instanceID)
		return
	}

	isBreached, err := compare(value, opRaw, limit)
	if err != nil {
		e.log.Warnw("threshold has invalid operator", "thresholdId", thresholdID, "operator", opRaw)
		return
	}

	wasBreached := e.breached[thresholdID]
	switch {
	case isBreached && !wasBreached:
		if err := e.emitFindingEvent(events.CreateOperation, t, value, opRaw, limit); err == nil {
			e.breached[thresholdID] = true
		}
	case !isBreached && wasBreached:
		if err := e.emitFindingEvent(events.DeleteOperation, t, value, opRaw, limit); err == nil {
			delete(e.breached, thresholdID)
		}
	}
}

func (e *Evaluator) emitFindingEvent(op events.Operation, t mdlobs.Threshold, value float64, operator string, limit float64) error {
	thresholdID := t.GetThresholdId()
	instanceID := t.GetMetricInstanceId()
	findingID := findingIDFor(thresholdID)

	ev := events.Event{
		ResourceType: events.FindingResource,
		Operation:    op,
		ResourceId:   findingID,
	}
	if op != events.DeleteOperation {
		f := buildFinding(findingID, thresholdID, instanceID, t.GetDisplayName(), value, operator, limit)
		ev.Objects = []any{f}
	}
	if err := e.emitter.Emit(ev); err != nil {
		// Leave the breach state unchanged so a failed emit is retried on the
		// next poll rather than being recorded as done (which would otherwise
		// suppress a retry and later emit an orphan Delete).
		e.log.Errorw("emit threshold finding failed",
			"thresholdId", thresholdID, "findingId", findingID, "operation", op.WireOperation(), "error", err)
		return err
	}
	e.log.Infow("threshold finding",
		"thresholdId", thresholdID, "findingId", findingID,
		"operation", op.WireOperation(), "value", value, "operator", operator, "limit", limit)
	return nil
}

// compare applies a comparison operator to (value, limit).
func compare(value float64, operator string, limit float64) (bool, error) {
	switch operator {
	case "gt":
		return value > limit, nil
	case "ge":
		return value >= limit, nil
	case "lt":
		return value < limit, nil
	case "le":
		return value <= limit, nil
	case "eq":
		return value == limit, nil
	case "ne":
		return value != limit, nil
	default:
		return false, fmt.Errorf("unknown operator %q", operator)
	}
}

// buildMetricValue constructs a MetricValue for a reading, referencing the
// MetricInstance it belongs to. The MetricValue id is a deterministic UUIDv5 of
// the MetricInstance id so repeated evaluations target the same resource.
func buildMetricValue(instanceID uuid.UUID, name, value string) mdlobs.MetricValue {
	mvID := uuid.NewSHA1(metricValueNamespace, instanceID[:])
	mv := mdlobs.NewMetricValue(mvID)
	mv.SetMetricInstanceById(instanceID)
	mv.SetValue(value)
	if name != "" {
		mv.SetDisplayName(name)
	}
	return mv
}

// findingIDFor derives the deterministic Finding id for a breached Threshold so
// re-emits upsert rather than duplicate.
func findingIDFor(thresholdID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(findingNamespace, []byte("threshold:"+thresholdID.String()))
}

// buildFinding constructs the breach Finding. Its Resources list is
// subject-first (the Threshold), then the referenced MetricInstance, matching
// the modelsrv sensor contract for findings.
func buildFinding(findingID, thresholdID, instanceID uuid.UUID, thresholdName string, value float64, operator string, limit float64) mdlfinding.Finding {
	f := mdlfinding.NewFinding(findingID)
	name := thresholdName
	if name == "" {
		name = "Threshold breached"
	}
	f.SetDisplayName(name)
	f.SetDescription(fmt.Sprintf("metric value %s %s %s",
		strconv.FormatFloat(value, 'f', -1, 64), operator, strconv.FormatFloat(limit, 'f', -1, 64)))
	f.SetFindingTypeById(mdlfinding.TypeIDForKind(ThresholdBreachedKind))
	f.SetResources([]*common.ResourceRef{
		{ResourceId: thresholdID, ResourceType: events.ThresholdResource},
		{ResourceId: instanceID, ResourceType: events.MetricInstanceResource},
	})
	return f
}
