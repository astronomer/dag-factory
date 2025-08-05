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
  seconds: 300
```

```yaml
retry_delay:
  __type__: datetime.timedelta
  seconds: 10
```

For more examples, check out the documentation at [Custom Python Objects in Configuration](https://astronomer.github.io/dag-factory/dev/configuration/custom_py_object/).

For further details, refer to this  [PR](https://github.com/astronomer/dag-factory/pull/512).

## List-Based Airflow DAG Tasks and Task Groups

While Airflow DAG tasks and task groups defined as dictionaries are still supported, we recommend transitioning to the list-based approach for better readability and consistency.

### Example

```yaml
basic_example_dag:
  schedule: "0 3 * * *"
  start_date:
    __type__: datetime.datetime
    year: 2025
    month: 1
    day: 1
  catchup: false
  task_groups:
    - group_name: "example_task_group"
      tooltip: "this is an example task group"
      dependencies: [task_1]
  tasks:
    - task_id: "task_1"
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo 1"
    - task_id: "task_2"
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo 2"
      dependencies: [task_1]
    - task_id: "task_3"
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo 3"
      dependencies: [task_1]
      task_group_name: "example_task_group"
```

For more examples, check out the [DAG's](https://github.com/astronomer/dag-factory/tree/main/dev/dags).

For further details, refer to this  [PR](https://github.com/astronomer/dag-factory/pull/487).

## Remove Legacy Type Casting for KubernetesPodOperator

With the latest update, the DAG-Factory `KubernetesPodOperator` now accepts a YAML dictionary only if it is compatible with the Airflow `KubernetesPodOperator`. You must use the `__type__` syntax to specify Kubernetes objects.

### Example

```yaml
kubernetes_pod_dag:
  start_date: 2025-01-01
  schedule: "@daily"
  description: "A DAG that runs a simple KubernetesPodOperator task"
  catchup: false
  tasks:
    - task_id: hello-world-pod
      operator: airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator
      config_file: "path/to/kube/config"
      image: "python:3.12-slim"
      cmds: ["python", "-c"]
      arguments: ["print('Hello from KubernetesPodOperator!')"]
      name: "example-pod-task"
      namespace: "default"
      get_logs: true
      container_resources:
        __type__: kubernetes.client.models.V1ResourceRequirements
        limits:
          cpu: "1"
          memory: "1024Mi"
        requests:
          cpu: "0.5"
          memory: "512Mi"
```

For more details, check out this [PR](https://github.com/astronomer/dag-factory/pull/523).
