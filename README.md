# modelsrv-prometheus-sensor

An EmELand sensor that turns a small number of hand-authored **Metric**
definitions into live, per-subject readings by fanning them out against
Prometheus.

You author one **Metric** naming a Prometheus time series plus an aggregation.
The sensor discovers every namespace/job that reports that series, creates
one **MetricInstance** per group, and keeps each group's **MetricValue** updated
on every poll.

## Concepts

| Resource | Authored where | This sensor's role |
|----------|----------------|--------------------|
| **Metric** | In emeland (git-sensor, etc.), **manually** | Names the Prometheus series, the aggregation, and the group-by labels. This sensor **reads** it. |
| **MetricInstance** | — | **Produced** by this sensor: one per namespace/job group discovered in Prometheus. Carries the concrete, group-scoped PromQL. |
| **MetricValue** | — | **Produced** by this sensor: the current scalar reading of a MetricInstance's group. |

The **Metric** is the abstract measurement ("resource count", "HTTP requests").
The sensor fans it out into concrete **MetricInstances** — one per group — and
each instance gets a **MetricValue**. This mirrors System/SystemInstance and
API/ApiInstance: the abstract definition is shared; the instances are the
concrete, per-subject things — except here the sensor generates the instances
automatically rather than expecting them to be authored.

## Data flow

```
                 (Metric events, authored by hand)
   git-sensor ───────────────────────────────────▶  prometheus-sensor  ──(MetricInstance + MetricValue events)──▶  modelsrv / subscribers
   (source of truth)                                    │      ▲
                                                        │      └── aggregated query (per Metric)
                                                        ▼
                                                   Prometheus
```

On startup the sensor registers itself with the configured `upstream` node, so
the upstream pushes its Metric definitions into the sensor's local model. On
every poll the sensor:

1. reads the Metrics from its local model;
2. for each Metric carrying `emeland.io/metric.source`, runs the aggregated
   discovery query `<aggregation> by (<group-by>) (<source>)` against Prometheus;
3. for each resulting group (label set), creates or updates a MetricInstance
   (with the group-scoped query) and its MetricValue (the group's scalar);
4. reconciles: MetricInstances and MetricValues whose group has disappeared from
   Prometheus are deleted;
5. forwards every emitted event to the configured downstream `subscribers`.

The sensor is the **sole author** of the derived MetricInstances and
MetricValues. It never authors Metrics — those come from emeland.

## How the fan-out works

Take `emeland_resource_count` in the demo Prometheus. It is reported by two
services in the `emeland-demo` namespace (`emeland-demo-filter` and
`emeland-demo-server`). You author a single Metric:

```yaml
version: emeland.io/v1
kind: Metric
spec:
  metricId: 11111111-1111-1111-1111-111111111111
  displayName: Resource count
  annotations:
    emeland.io/metric.source: emeland_resource_count
    emeland.io/metric.aggregation: sum
    emeland.io/metric.group-by: namespace,job
```

Each poll the sensor runs the discovery query:

```promql
sum by (namespace, job) (emeland_resource_count)
```

That returns one series per namespace/job group. For each the sensor emits:

- a **MetricInstance** with a deterministic id derived from the Metric id and the
  group's label values, carrying the group-scoped query in
  `emeland.io/metric.expression`, e.g.
  `sum by (namespace, job) (emeland_resource_count{namespace="emeland-demo", job="emeland-demo-filter"})`,
  and the group labels under `emeland.io/metric.label.<name>` annotations;
- a **MetricValue** with a deterministic id derived from the MetricInstance id,
  carrying the current scalar reading.

Two groups → two MetricInstances → two MetricValues.

### Metric annotation keys

| Key | Required | Default | Purpose |
|-----|----------|---------|---------|
| `emeland.io/metric.source` | yes | — | The Prometheus time series to fan out. A Metric without it is ignored. |
| `emeland.io/metric.aggregation` | no | `sum` | Aggregation reducing each group to a scalar: `sum`, `avg`, `min`, `max`, `count`. |
| `emeland.io/metric.group-by` | no | `namespace,job` | Comma-separated labels to group by; one MetricInstance per distinct combination. |

The aggregation is applied **in PromQL** (`<agg> by (<group-by>) (<source>)`), so
Prometheus does the reduction and each MetricInstance's value is always a scalar.

A note on high-cardinality series: `emeland_resource_count` also carries a `type`
label (API, Component, Finding, ...). Grouping only by `namespace,job` sums
across all types into one number. If you want a separate instance per type, set
`emeland.io/metric.group-by: namespace,job,type`.

### Determinism and reconciliation

- **Deterministic ids.** A MetricInstance id is a UUIDv5 of the Metric id plus the
  group's sorted `key=value` label pairs; a MetricValue id is a UUIDv5 of its
  MetricInstance id. Repeated polls upsert the same resources rather than
  duplicating them.
- **Change-only emits.** A MetricInstance is re-emitted only when its name or
  query changes; a MetricValue only when its reading changes (Create on first
  sight, Update on change, nothing when unchanged).
- **Reconcile deletes.** When a group disappears from Prometheus, its MetricValue
  is deleted first, then its MetricInstance. State is in-memory, so a group that
  disappears while the sensor is down is re-asserted (not deleted) on restart;
  it is reconciled away on the first poll where it is absent.

## Authoring Metrics

Author these in your emeland source of truth (for example a git repo watched by
`modelsrv-git-sensor`). Full examples live in [`examples/`](examples/):

- [`examples/metric.yaml`](examples/metric.yaml) — `emeland_resource_count`
- [`examples/metric-http-requests.yaml`](examples/metric-http-requests.yaml) — `demoapp_http_requests_total`

## Configuration

Values live in [`config/sensor.yaml`](config/sensor.yaml):

- `upstream` — base API URL of the emeland node holding the Metric definitions.
- `prometheusUrl` — base URL of the Prometheus server to query.
- `pollInterval` — fan-out interval (default `30s`).
- `subscribers` — downstream model servers that receive the emitted events.

No authentication is used against Prometheus.

## Run

```bash
go run ./cmd/modelsrv-prometheus-sensor \
  --config config/sensor.yaml \
  --listen localhost:24200 \
  --poll-interval 30s
```

Flags fall back to environment variables: `SENSOR_CONFIG`,
`SENSOR_LISTEN_ADDR`, `SENSOR_POLL_INTERVAL`. The `--poll-interval` flag
overrides the config value only when it is greater than zero; otherwise the
config `pollInterval` (or its 30s default) is used.

## Subscriber management HTTP endpoints

The sensor exposes the same subscriber management endpoints as `modelsrv`
(mounted under `/api`):

- `POST /api/events/register` (body: `{"callbackUrl":"http://downstream:24000/api/"}`) → **201**
- `POST /api/events/unregister` (body: `{"callbackUrl":"http://downstream:24000/api/"}`) → **200** or **404**
- `GET /api/events/subscribers` → `["http://downstream:24000/api/", ...]`

## Dependency note: modelsrv version

`go.mod` pins `go.emeland.io/modelsrv` to a released tag exposing the Phase 6
observability resources (Metric, MetricInstance, MetricValue).

## Provenance

`internal/sensor/eventmgr.go` and `internal/sensor/replication_wire.go` are
copied verbatim from
[`modelsrv-git-sensor`](https://github.com/emeland-io/modelsrv-git-sensor).
They are generic sensor infrastructure (event forwarding and replication
wire-shaping) shared between sensors and carry the upstream Apache-2.0 header. A
follow-up will extract this into a reusable package in `modelsrv` so both
sensors import it instead of duplicating it.

## Development

```bash
make ci      # mod download, verify, lint, build, test
make test    # go test ./...
make run     # run with config/sensor.yaml
```
