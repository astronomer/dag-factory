# Metrics

DAG Factory emits operational metrics through Airflow's own stats client. They are
not sent anywhere separately: they travel over whatever metrics backend your Airflow
deployment already uses, and they honour the same `[metrics]` configuration as
Airflow's built-in metrics.

!!! note
    Metrics are silently dropped unless a backend is enabled. See
    [Airflow's metrics configuration](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)
    for how to turn on StatsD (`statsd_on`) or OpenTelemetry (`otel_on`).

## Available metrics

| Name                                | Type    | Tags     | Description                                                               |
| ----------------------------------- | ------- | -------- | ------------------------------------------------------------------------- |
| `dagfactory.telemetry.emit_failure` | counter | `reason` | A usage telemetry event could not be delivered to the telemetry endpoint. |

Airflow prepends its own namespace to every metric name, controlled by
`[metrics] statsd_prefix` or `[metrics] otel_prefix` (both default to `airflow`).
With the defaults, the counter above arrives at your backend as:

```text
airflow.dagfactory.telemetry.emit_failure
```

### `dagfactory.telemetry.emit_failure`

Incremented once per failed attempt to send a usage telemetry event. The `reason`
tag distinguishes the two failure paths:

| `reason`     | Meaning                                                                  |
| ------------ | ------------------------------------------------------------------------ |
| `exception`  | The request never completed — connection error, DNS failure, or timeout. |
| `http_error` | The request completed but the endpoint returned a non-2xx status code.   |

Successful deliveries are not counted. A non-zero rate is not an error condition for
your DAGs — telemetry failures never interrupt DAG parsing or execution — but a
sustained rate means your usage telemetry is being lost, which is worth knowing if
you rely on it or if it points at an egress restriction in your network.

The counter is only ever incremented when telemetry is enabled. If you have opted
out (see [Usage telemetry](#usage-telemetry)), no request is attempted and the
metric stays at zero.

!!! note "Tag support depends on your backend"
    The classic StatsD protocol has no concept of tags, and Airflow **drops the
    `reason` tag by default**, leaving a single undifferentiated counter. To keep the
    tag, enable a tagged wire format — `statsd_influxdb_enabled = True` (InfluxDB
    `name,key=value`) or `statsd_datadog_enabled = True` (DogStatsD `|#key:value`).
    OpenTelemetry sends tags as native attributes and needs no extra configuration.

## Filtering metrics

DAG Factory metrics are subject to Airflow's `[metrics] metrics_allow_list` and
`[metrics] metrics_block_list`. If you have set an allow list, DAG Factory metrics
are dropped unless the list matches them:

```ini
[metrics]
metrics_allow_list = dagfactory,scheduler
```

## Usage telemetry

DAG Factory reports anonymous usage telemetry to
[Scarf](https://about.scarf.sh/) when a DAG Factory-generated DAG run completes.
This helps the maintainers understand which versions and platforms are in active use.

The following fields are sent: DAG Factory version, Airflow version, Python version,
operating system and machine architecture, event type, DAG run status, task count,
and a hash of the DAG ID. No DAG contents, connection details, credentials, or
identifying information about your deployment are collected.

### Opting out

Telemetry is enabled by default. Any one of the following disables it:

| Variable                                 | Section       | Key                | Default |
| ---------------------------------------- | ------------- | ------------------ | ------- |
| `AIRFLOW__DAG_FACTORY__ENABLE_TELEMETRY` | `dag_factory` | `enable_telemetry` | `True`  |
| `DO_NOT_TRACK`                           | —             | —                  | unset   |
| `SCARF_NO_ANALYTICS`                     | —             | —                  | unset   |

```bash
# Turn off DAG Factory telemetry specifically
AIRFLOW__DAG_FACTORY__ENABLE_TELEMETRY=false

# Or honour the cross-tool opt-out conventions
DO_NOT_TRACK=true
SCARF_NO_ANALYTICS=true
```

`DO_NOT_TRACK` and `SCARF_NO_ANALYTICS` are read directly from the environment, so
they must be set in the environment of the process running your DAGs rather than in
`airflow.cfg`.
