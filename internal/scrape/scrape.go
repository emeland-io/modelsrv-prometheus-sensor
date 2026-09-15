// Package scrape turns Prometheus alerting rules into emeland observability
// definitions. It fetches the alerting rules from Prometheus (the rule
// definitions, not their firing state) and decomposes each rule into a
// MetricInstance (carrying the measurement expression) plus a Threshold that
// references the instance (carrying a structured operator + limit, never an
// expression). Resources get deterministic ids derived from the rule's
// identity (group + name), so editing a rule's query, labels or threshold
// updates the same resources in place. A reconcile pass emits deletes for rules
// that have disappeared since the previous scrape (in-memory state; deletions
// that happen while the sensor is down are not detected).
package scrape

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"go.emeland.io/modelsrv/pkg/events"
	mdlobs "go.emeland.io/modelsrv/pkg/model/observability"

	"emeland.io/modelsrv-prometheus-sensor/internal/eval"
	"emeland.io/modelsrv-prometheus-sensor/internal/prometheus"
)

// Annotation keys recording the Prometheus origin of a scraped resource.
const (
	// AnnotationSource marks a resource as scraped and names the source system.
	AnnotationSource = "prometheus.emeland.io/source"
	// AnnotationRuleGroup records the Prometheus rule group the rule came from.
	AnnotationRuleGroup = "prometheus.emeland.io/rule-group"
	// AnnotationRuleName records the Prometheus alert name.
	AnnotationRuleName = "prometheus.emeland.io/rule-name"

	sourcePrometheusRules = "prometheus-rules"
)

// scrapeNamespace anchors the deterministic UUIDv5 ids derived from a rule's
// identity. It must not change after first deployment, or previously emitted
// resources would be re-created under new ids.
var scrapeNamespace = uuid.MustParse("2f7c1d90-6a4b-5e28-b13f-9c5d7e0a6b21")

// Emitter forwards a domain event to downstream subscribers.
type Emitter interface {
	Emit(events.Event) error
}

// Fetcher returns the alerting rules to decompose. It is satisfied by
// *prometheus.Client.
type Fetcher interface {
	FetchAlertingRules(ctx context.Context) ([]prometheus.AlertingRule, error)
}

// Scraper decomposes Prometheus alerting rules into MetricInstance + Threshold
// resources and reconciles deletions across scrapes.
type Scraper struct {
	fetcher Fetcher
	emitter Emitter
	log     *zap.SugaredLogger

	// seen maps a rule identity key to what was last successfully emitted for
	// it: the resource ids and a content signature (to skip unchanged rules).
	seen map[string]seenRule
}

type seenRule struct {
	metricInstanceID uuid.UUID
	thresholdID      uuid.UUID
	signature        string
}

// New builds a Scraper.
func New(fetcher Fetcher, emitter Emitter, log *zap.SugaredLogger) *Scraper {
	if log == nil {
		log = zap.NewNop().Sugar()
	}
	return &Scraper{
		fetcher: fetcher,
		emitter: emitter,
		log:     log,
		seen:    make(map[string]seenRule),
	}
}

// ScrapeOnce fetches the current alerting rules, emits a MetricInstance +
// Threshold per rule (skipping rules unchanged since the last scrape), and
// deletes resources for rules that have disappeared.
func (s *Scraper) ScrapeOnce(ctx context.Context) error {
	rules, err := s.fetcher.FetchAlertingRules(ctx)
	if err != nil {
		return fmt.Errorf("fetch alerting rules: %w", err)
	}

	current := make(map[string]struct{}, len(rules))
	for _, r := range rules {
		key := ruleIdentity(r)
		current[key] = struct{}{}
		s.emitRule(key, r)
	}

	// Reconcile: delete resources for rules no longer present. Only forget a
	// rule once both deletes succeed, so a failed delete is retried next scrape.
	for key, prev := range s.seen {
		if _, ok := current[key]; ok {
			continue
		}
		if s.emitDelete(prev) {
			delete(s.seen, key)
		}
	}
	return nil
}

func (s *Scraper) emitRule(key string, r prometheus.AlertingRule) {
	measurement, operator, limit := decompose(r.Query)
	miID := instanceIDForKey(key)
	thID := thresholdIDForKey(key)

	sig := signature(r, measurement, operator, limit)
	if prev, ok := s.seen[key]; ok && prev.signature == sig {
		return // unchanged since last scrape; nothing to re-emit
	}

	mi := buildMetricInstance(miID, r, measurement)
	if err := s.emitter.Emit(events.Event{
		ResourceType: events.MetricInstanceResource,
		Operation:    events.CreateOperation,
		ResourceId:   miID,
		Objects:      []any{mi},
	}); err != nil {
		// Leave seen unchanged so this rule is retried on the next scrape.
		s.log.Errorw("emit scraped metric instance failed", "rule", r.Name, "metricInstanceId", miID, "error", err)
		return
	}

	th := buildThreshold(thID, miID, r, operator, limit)
	if err := s.emitter.Emit(events.Event{
		ResourceType: events.ThresholdResource,
		Operation:    events.CreateOperation,
		ResourceId:   thID,
		Objects:      []any{th},
	}); err != nil {
		s.log.Errorw("emit scraped threshold failed", "rule", r.Name, "thresholdId", thID, "error", err)
		return
	}

	s.seen[key] = seenRule{metricInstanceID: miID, thresholdID: thID, signature: sig}
	s.log.Infow("scraped alerting rule", "rule", r.Name, "group", r.Group,
		"metricInstanceId", miID, "thresholdId", thID, "operator", operator, "limit", limit)
}

// emitDelete deletes the Threshold then the MetricInstance. It returns true only
// when both deletes succeed, so the caller retries on failure.
func (s *Scraper) emitDelete(prev seenRule) bool {
	if err := s.emitter.Emit(events.Event{
		ResourceType: events.ThresholdResource,
		Operation:    events.DeleteOperation,
		ResourceId:   prev.thresholdID,
	}); err != nil {
		s.log.Errorw("delete scraped threshold failed", "thresholdId", prev.thresholdID, "error", err)
		return false
	}
	if err := s.emitter.Emit(events.Event{
		ResourceType: events.MetricInstanceResource,
		Operation:    events.DeleteOperation,
		ResourceId:   prev.metricInstanceID,
	}); err != nil {
		s.log.Errorw("delete scraped metric instance failed", "metricInstanceId", prev.metricInstanceID, "error", err)
		return false
	}
	return true
}

// decompose splits a rule query into the measurement expression that the
// MetricInstance evaluates and the structured operator + limit the Threshold
// applies. A rule of the form "<expr> <op> <number>" splits cleanly. Anything
// that does not (no top-level comparison, non-numeric RHS, compound condition)
// falls back to treating the whole query as a boolean measurement (0 or 1) with
// a "gt 0" threshold, so a scraped rule always yields a usable instance + bound.
func decompose(query string) (measurement, operator, limit string) {
	if expr, op, lim, ok := splitComparison(query); ok {
		return expr, op, lim
	}
	return query, "gt", "0"
}

// signature captures everything the emitted resources depend on, so an
// unchanged rule can be skipped on the next scrape.
func signature(r prometheus.AlertingRule, measurement, operator, limit string) string {
	desc := ruleDescription(r)
	return r.Name + "\x1f" + measurement + "\x1f" + operator + "\x1f" + limit + "\x1f" + desc + "\x1f" + r.Group
}

// buildMetricInstance derives a MetricInstance carrying the measurement
// expression (the left-hand side of the rule's comparison, or the whole query
// in the boolean-fallback case).
func buildMetricInstance(id uuid.UUID, r prometheus.AlertingRule, measurement string) mdlobs.MetricInstance {
	mi := mdlobs.NewMetricInstance(id)
	if r.Name != "" {
		mi.SetDisplayName(r.Name)
	}
	ann := mi.GetAnnotations()
	ann.Add(eval.AnnotationExpression, measurement)
	ann.Add(eval.AnnotationLanguage, eval.LanguagePromQL)
	ann.Add(AnnotationSource, sourcePrometheusRules)
	ann.Add(AnnotationRuleGroup, r.Group)
	ann.Add(AnnotationRuleName, r.Name)
	if desc := ruleDescription(r); desc != "" {
		mi.SetDescription(desc)
	}
	return mi
}

// buildThreshold derives a Threshold referencing the rule's MetricInstance. The
// Threshold carries only the structured operator + limit and never an
// expression; the expression lives on the MetricInstance it references.
func buildThreshold(id, instanceID uuid.UUID, r prometheus.AlertingRule, operator, limit string) mdlobs.Threshold {
	th := mdlobs.NewThreshold(id)
	if r.Name != "" {
		th.SetDisplayName(r.Name)
	}
	th.SetMetricInstanceById(instanceID)
	ann := th.GetAnnotations()
	ann.Add(eval.AnnotationThresholdOperator, operator)
	ann.Add(eval.AnnotationThresholdLimit, limit)
	ann.Add(AnnotationSource, sourcePrometheusRules)
	if desc := ruleDescription(r); desc != "" {
		th.SetDescription(desc)
	}
	return th
}

func ruleDescription(r prometheus.AlertingRule) string {
	if desc := r.Annotations["description"]; desc != "" {
		return desc
	}
	return r.Annotations["summary"]
}

// ruleIdentity is the stable identity of a rule: its group and name. The query,
// labels and threshold are intentionally excluded so that editing any of them
// updates the same resources in place rather than orphaning them.
func ruleIdentity(r prometheus.AlertingRule) string {
	return r.Group + "\x1f" + r.Name
}

func instanceIDForKey(key string) uuid.UUID {
	return uuid.NewSHA1(scrapeNamespace, []byte("metricinstance:"+key))
}

func thresholdIDForKey(key string) uuid.UUID {
	return uuid.NewSHA1(scrapeNamespace, []byte("threshold:"+key))
}
