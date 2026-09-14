// Package sensor runs a modelsrv web endpoint backed by a local model. It
// registers a stable NodeType/Node, subscribes itself to an upstream emeland
// node so Metric definitions are pushed into the local model, and forwards
// emitted MetricValue events to downstream subscribers.
package sensor
