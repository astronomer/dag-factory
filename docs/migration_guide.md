# Migrate to DAG-Factory v1.0.0

## Airflow Providers Dependency

If you're using operators or sensors from apache-airflow-providers-http or apache-airflow-providers-cncf-kubernetes, you may encounter import errors, causing your DAG to fail. With the new release, DAG Factory no longer enforces the installation of Airflow providers. We recommend that you manually install the required Airflow providers in your environment.

For more details, check out this [PR](https://github.com/astronomer/dag-factory/pull/486).

## Removed clean_dags() Method from DagFactory

The `clean_dags()` method is no longer part of the `DagFactory` class and has been removed. If your DAG was using this method, you can safely remove it from your DAG file. The DAG refresh behavior is now controlled by the Airflow config setting `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL`.

For more details, check out this [PR](https://github.com/astronomer/dag-factory/pull/498).

## Removed Inconsistent DAG Factory Parameters

DAG Factory no longer accepts the parameters `dagrun_timeout_sec`, `retry_delay_sec`, `sla_secs`, `execution_delta_secs` and `execution_timeout_secs` in the YAML/YML configuration. If your DAG was using these parameters, we recommend switching to their Airflow equivalents: `dagrun_timeout`, `retry_delay`, `sla`, `execution_delta`, and `execution_timeout`.

**Example:**

```yaml
dagrun_timeout:
  __type__: datetime.timedelta
  seconds: 60
```

For more examples, check out the documentation at [Custom Python Objects in Configuration](https://astronomer.github.io/dag-factory/dev/configuration/custom_py_object/).

For further details, refer to this  [PR](https://github.com/astronomer/dag-factory/pull/512).

## Remove generate_dags

## Task as List

## KPO

## Remove schedule_interval

## !and !or

## timetable

## datasets
