# Privacy Notice

This project follows the [Privacy Policy of Astronomer](https://www.astronomer.io/privacy/).

## Collection of Data

DAG Factory integrates [Scarf](https://about.scarf.sh/) to collect basic telemetry data during operation.
This data assists the project maintainers in better understanding how DAG Factory is used.
Insights gained from this telemetry are critical for prioritizing patches, minor releases, and
security fixes. Additionally, this information supports key decisions related to the development road map.

Deployments and individual users can opt-out of analytics by setting the configuration:

```
[dag_factory] enable_telemetry False
```

As described in the [official documentation](https://docs.scarf.sh/gateway/#do-not-track), it is also possible to opt out by setting one of the following environment variables:

```commandline
DO_NOT_TRACK=True
SCARF_NO_ANALYTICS=True
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

No user-identifiable information (IP included) is stored in Scarf.
