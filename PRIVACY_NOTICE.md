# Privacy Notice

By default, telemetry is disabled for Astronomer customers running recent Astro Runtime images or Astro Private Cloud — see [Telemetry on Astronomer](#telemetry-on-astronomer) for the specific versions.

This project follows the [Privacy Policy of Astronomer](https://www.astronomer.io/privacy/).

## Telemetry on Astronomer

Since May 2025, [Astro Runtime](https://www.astronomer.io/docs/runtime/runtime-release-notes) images set the
environment variable `SCARF_NO_ANALYTICS=True`, which disables DAG Factory telemetry by default:

- Airflow 3-based images: Astro Runtime 3.0-2 and newer
- Airflow 2-based images: Astro Runtime 11.18.0, 12.9.0, 13.0.0 and newer

[Astro Private Cloud](https://www.astronomer.io/docs/astro-private-cloud/) (APC) also disables telemetry by
default, setting both `SCARF_NO_ANALYTICS=True` and `DO_NOT_TRACK=True` in all Deployments, regardless of
the Astro Runtime version.

## Collection of Data

DAG Factory integrates [Scarf](https://about.scarf.sh/) to collect basic telemetry data during operation.
This data is collected and processed by Scarf in accordance with the [Scarf Privacy Policy](https://about.scarf.sh/privacy-policy/).
It assists the project maintainers in better understanding how DAG Factory is used.
Insights gained from this telemetry are critical for prioritizing patches, minor releases, and
security fixes. Additionally, this information supports key decisions related to the development roadmap.

Deployments and individual users can opt out of analytics by setting the configuration:

```ini
[dag_factory]
enable_telemetry = false
```

or the equivalent environment variable:

```bash
AIRFLOW__DAG_FACTORY__ENABLE_TELEMETRY=false
```

As described in the [Scarf documentation](https://docs.scarf.sh/gateway/#do-not-track), it is also possible to opt out by setting one of the following environment variables (values are case-insensitive):

```bash
DO_NOT_TRACK=true
SCARF_NO_ANALYTICS=true
```

In addition to Scarf's default data collection, DAG Factory collects the following information:

- DAG Factory version
- Airflow version
- Python version
- Operating system & machine architecture
- Event type
- Number of failed DagRuns
- Number of successful DagRuns
- Total tasks associated to each DagRun
- Dag hash

Astronomer does not track user-identifiable information through DAG Factory telemetry.
For details on how Scarf handles the collected data, please refer to the
[Scarf Privacy Policy](https://about.scarf.sh/privacy-policy/).
