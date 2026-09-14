# modelsrv-prometheus-sensor

An EmELand sensor that brings Prometheus monitoring into the landscape model. It
does **not** scrape raw time series. Instead it consumes hand-authored (or
scraped) observability definitions from emeland and turns them into live
readings and findings:

- a **MetricInstance** (carrying a PromQL query) becomes a **MetricValue** (its current reading);
- a **Threshold** (a bound on a MetricInstance) becomes a **Finding** when breached.

## Concepts

| Resource | Authored where | This sensor's role |
|----------|----------------|--------------------|
| **Metric** | In emeland (git-sensor, etc.) | Abstract, organizational grouping (name, unit, dimension). Optional. This sensor does **not** read it. |
| **MetricInstance** | In emeland (git-sensor, or scraped) | The concrete measured thing, carrying the PromQL query. This sensor **reads and evaluates** it. |
| **MetricValue** | — | **Produced** by this sensor: the current scalar reading of a MetricInstance. |
| **Threshold** | In emeland, referencing a MetricInstance | This sensor **reads** it and compares the MetricInstance's value against its bound. |
| **Finding** | — | **Produced** by this sensor: raised when a Threshold is breached, cleared when it recovers. |

The **Metric** is an abstract concept ("p99 latency"), like a `ContextType` — it
carries no query and this sensor ignores it. The **MetricInstance** is the
concrete measurement of a specific subject ("p99 latency of the payments API in
prod-eu"); it carries the actual PromQL, because the query is concrete and
differs per subject (different labels, aggregations). A MetricInstance may
optionally reference a Metric to group it under an abstract concept, but that
reference is not required — a scraped instance (e.g. from AlertManager) often
has no reliable parent Metric.

This mirrors System/SystemInstance and API/ApiInstance: the abstract definition
is shared; the instance is the concrete, technical thing.

## Data flow

```
                 (MetricInstance + Threshold events)
   git-sensor ────────────────────────────────────▶  prometheus-sensor  ──(MetricValue + Finding events)──▶  modelsrv / subscribers
   (source of truth)                                     │      ▲
                                                         │      └── PromQL query (MetricInstance)
                                                         ▼
                                                    Prometheus
```

On startup the sensor registers itself with the configured `upstream` node, so
the upstream pushes its MetricInstance and Threshold definitions into the
sensor's local model. On every poll the sensor:

1. reads the MetricInstances from its local model;
2. for each one carrying a PromQL expression, runs the query against Prometheus
   and emits a MetricValue referencing that instance;
3. reads the Thresholds, compares each one against its MetricInstance's fresh
   value, and emits or clears a Finding;
4. forwards every emitted event to the configured downstream `subscribers`.

The sensor is purely a **producer of derived resources** (MetricValues and
Findings). It never authors MetricInstances or Thresholds — those come from
emeland.

## What happens on every poll

### MetricInstances → MetricValues

A MetricInstance is evaluated when it carries a PromQL expression annotation:

- `emeland.io/metric.expression` — the PromQL query (required to evaluate; instances without it are skipped).
- `emeland.io/metric.language` — must be `promql` or unset; other languages are skipped.

The query must reduce to a scalar: a PromQL scalar, or an instant vector with a
single sample. Empty, multi-sample, matrix and string results are rejected and
logged (this sensor supports scalar metrics only). No authentication is used
against Prometheus.

The emitted MetricValue has a **deterministic id** (derived from the
MetricInstance id), so repeated polls update the same resource. Unchanged
readings are not re-emitted (Create on first value, Update on change, nothing
when unchanged).

### Thresholds → Findings

A Threshold references a MetricInstance (`metricInstanceRef`) and carries a
structured comparison in annotations — **not** its own PromQL:

- `emeland.io/threshold.operator` — one of `gt`, `ge`, `lt`, `le`, `eq`, `ne`.
- `emeland.io/threshold.limit` — the numeric bound.

Each poll, after a MetricInstance's value is computed, the sensor compares that
value against every Threshold referencing the instance: `value <operator> limit`.
No extra Prometheus query is made — the instance's value is reused.

- On the transition **into** breach, a `Finding` is emitted.
- On the transition **out of** breach, that Finding is deleted.
- A Threshold whose MetricInstance produced no value this poll is left unchanged.

#### Edge cases and runtime state

Breach state is tracked in memory per Threshold, so a Finding is emitted once on
breach and deleted once on recovery. Two consequences are intentional:

- **Unavailable value holds the state.** If a breached Threshold's MetricInstance
  stops producing a value (query failing, expression removed, instance deleted)
  or the Threshold loses its `operator`/`limit` annotations, the sensor leaves
  the breach state as-is rather than clearing the Finding. This avoids flapping
  the Finding on transient Prometheus gaps. The Finding clears only when the
  instance produces a value again and the comparison no longer holds.
- **Restart re-evaluates from scratch.** On process restart the in-memory state
  is empty. The first poll re-emits active MetricValues and re-creates
  currently-breached Findings (both use deterministic ids, so these are
  idempotent upserts, not duplicates). A breach that recovered while the process
  was down is not explicitly deleted; it is superseded on the next poll because
  the recovered value no longer breaches. For durable clearing, restart the
  sensor while Prometheus is reachable so the first poll observes current values.

The Finding:

- has a **deterministic id** derived from the Threshold id (re-emits upsert, not duplicate);
- lists its `Resources` **subject-first**: the Threshold, then the referenced
  MetricInstance (matching the modelsrv finding contract so `resolvefindings`
  can reason about it);
- is classified by the sensor-defined finding kind **`ThresholdBreached`**, whose
  FindingType UUID is derived the same way as modelsrv's built-in kinds.

To give that finding kind a human-readable name in the model, register a
`FindingType` under the same UUID — see [`examples/findingtype.yaml`](examples/findingtype.yaml).

## Authoring definitions

Author these in your emeland source of truth (for example a git repo watched by
`modelsrv-git-sensor`). Full examples live in [`examples/`](examples/).

### Metric (optional, abstract) — [`examples/metric.yaml`](examples/metric.yaml)

```yaml
version: emeland.io/v1
kind: Metric
spec:
  metricId: 11111111-1111-1111-1111-111111111111
  displayName: Up targets
  annotations:
    emeland.io/unit: targets
    emeland.io/dimension: availability
```

The Metric carries no query; it is an optional grouping. The sensor does not
read it.

### MetricInstance (the measured thing) — [`examples/metricinstance.yaml`](examples/metricinstance.yaml)

```yaml
version: emeland.io/v1
kind: MetricInstance
spec:
  metricInstanceId: 22222222-2222-2222-2222-222222222222
  displayName: Up targets (prod cluster)
  metricRef:                                   # optional grouping under a Metric
    metricId: 11111111-1111-1111-1111-111111111111
  annotations:
    emeland.io/metric.expression: "sum(up{cluster=\"prod\"})"
    emeland.io/metric.language: promql
```

The sensor runs this instance's query and emits a MetricValue with the current
value.

### Threshold — [`examples/threshold.yaml`](examples/threshold.yaml)

```yaml
version: emeland.io/v1
kind: Threshold
spec:
  thresholdId: 33333333-3333-3333-3333-333333333333
  displayName: Too few targets up
  metricInstanceRef:
    metricInstanceId: 22222222-2222-2222-2222-222222222222   # the instance above
  annotations:
    emeland.io/threshold.operator: lt
    emeland.io/threshold.limit: "3"
```

This Threshold raises a `ThresholdBreached` finding whenever the instance's value
drops below 3, and clears it when it recovers.

## Configuration

Values live in [`config/sensor.yaml`](config/sensor.yaml):

- `upstream` — base API URL of the emeland node holding the definitions.
- `prometheusUrl` — base URL of the Prometheus server to query.
- `pollInterval` — evaluation interval (default `30s`).
- `subscribers` — downstream model servers that receive the emitted events.

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

`go.mod` pins `go.emeland.io/modelsrv` to an untagged pseudo-version. The Phase 6
observability resources this sensor depends on (Metric, MetricInstance,
MetricValue, Threshold and the finding helpers) were merged after the latest
release tag, so a pseudo-version is currently the only one that exposes them.
Repin to a tagged modelsrv release once one includes those resources.

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
