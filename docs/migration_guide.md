# Migrate to DAG-Factory v1.0.0

## 1. Change `DagFactory` Class Access to Private

**Impact:**

- The `DagFactory` import path is removed.
- Class is now `_DagFactory`.
- The `generate_dags` method is now `_generate_dags`.

**Action:**

- Use the recommended `load_yaml_dags` method for DAG generation.

**Example:**

```python
from dagfactory import load_yaml_dags

# Load DAG from a specific YAML file
load_yaml_dags(globals_dict=globals(), config_filepath='/path/to/your/dag_config.yaml')
```

See [load_yaml_dags function docs](./configuration/load_yaml_dags.md)  and [PR #509](https://github.com/astronomer/dag-factory/pull/509)

## 2. Rename Parameter of `load_yaml_dags`

**Impact:**

- The parameter `default_args_config_dict` is now `defaults_config_dict`.

**Action:**

- Update your function signatures accordingly.

**Example:**

```python
from datetime import datetime
from dagfactory import load_yaml_dags

defaults_config_dict = {"default_args": {"start_date": datetime(2025, 1, 1)}}

load_yaml_dags(..., defaults_config_dict=defaults_config_dict)
```

See [PR #546](https://github.com/astronomer/dag-factory/pull/546)

## 3. Remove `schedule_interval` in Favor of `schedule`

**Impact:**

- The `schedule_interval` key in YAML DAG configuration is no longer supported.

**Action:**

- Use `schedule` rather than `schedule_interval` to define DAG schedules.

**Example:**

```yaml
schedule: @daily
```

See [PR #503](https://github.com/astronomer/dag-factory/pull/503)

## 4. Removal of Inconsistent YAML Parameters

**Impact:**

**Impact:**

- The following parameters are no longer accepted in YAML:

    - `dagrun_timeout_sec`
    - `retry_delay_sec`
    - `sla_secs`
    - `execution_delta_secs`
    - `execution_timeout_secs`

**Action:**

- Switch to Airflow’s direct equivalents:

    - `dagrun_timeout`
    - `retry_delay`
    - `sla`
    - `execution_delta`
    - `execution_timeout`

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

See [Custom Python Object Docs](./configuration/custom_py_object.md) and [PR #512](https://github.com/astronomer/dag-factory/pull/512)

## 5. Consolidate Logical Keys (`!and`, `!or`, `!join`, `and`, `or`)

**Impact:**

- The previous logical and join keys (`!and`, `!or`, `!join`, `and`, `or`) are no longer recognized in YAML DAG configurations.

**Action:**

- Update your YAML configuration files by replacing all occurrences of:

    - `!and` or `and` → `__and__`
    - `!or` or `or` → `__or__`
    - `!join` → `__join__`

See [PR #525](https://github.com/astronomer/dag-factory/pull/525)

## 6. Remove custom parsing for `timetable`

**Impact:**

- The `timetable` param no longer accept params like `callable` and `params`

**Action:**

- Use the [`__type__`](./features/custom_python_object.md) annotation to define it in your YAML.

**Example:**

```yaml
timetable:
  __type__: airflow.timetables.trigger.CronTriggerTimetable
  __args__:
    - 0 1 * * 3
```

For details: See [PR #533](https://github.com/astronomer/dag-factory/pull/533)

## 7. Airflow Providers Dependency

**Impact:**

- DAG-Factory no longer installs Airflow providers (e.g., `apache-airflow-providers-http`, `apache-airflow-providers-cncf-kubernetes`) automatically.

**Action:**

- Manually install any required Airflow providers in your environment. Missing providers will cause operator/sensor import errors and lead to DAG failures.

For details: See [PR #486](https://github.com/astronomer/dag-factory/pull/486)

## 8. Remove Legacy Type Casting for KubernetesPodOperator

**Impact:**

- With the latest update, the DAG-Factory `KubernetesPodOperator` now accepts a YAML dictionary only if it is compatible with the Airflow `KubernetesPodOperator`.

**Action:**

- You must use the `__type__` syntax to specify Python object in DAG config YAML.

**Example:**

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

See [PR #523](https://github.com/astronomer/dag-factory/pull/523)

## 9. Removal of `clean_dags()` Method

**Impact:**

- `clean_dags()` has been removed from the `DagFactory` class.

**Action:**

- If your DAG references this method, delete the invocation. Control DAG refresh behavior using the Airflow config:
`AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL`

For details: See [PR #498](https://github.com/astronomer/dag-factory/pull/498)

## 10. List-Based DAG Tasks and Task Groups

Although dictionary definitions are still allowed, transition to **list-based** configurations for better readability and maintenance.

**Example:**

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

See more examples in the [dev/dags folder](https://github.com/astronomer/dag-factory/tree/main/dev/dags) and [PR #487](https://github.com/astronomer/dag-factory/pull/487)
