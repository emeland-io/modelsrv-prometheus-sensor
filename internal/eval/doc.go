// Package eval implements the evaluation loop. It reads MetricInstances from the
// model and, for each one carrying a PromQL expression annotation, runs the
// query against Prometheus and emits a MetricValue holding the current scalar
// reading of that instance. For each Threshold referencing a MetricInstance it
// compares the instance's current value against the Threshold's structured
// operator+limit annotations and emits (or clears) a ThresholdBreached Finding
// on the breach transition. The sensor never creates MetricInstances or
// Thresholds; those are authored upstream (or scraped).
package eval
