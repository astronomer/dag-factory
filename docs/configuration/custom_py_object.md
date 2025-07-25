# Custom Python Object

DAG Factory supports the definition of complex Python objects directly in your YAML configuration files using a generalized object syntax. This feature, available from DAG Factory 0.23.0, allows you to represent objects such as `datetime`, `timedelta`, Airflow timetables, assets, and even Kubernetes objects in a declarative, readable way.

---

## Why Use Generalised Objects?

- **Expressiveness:** Define complex parameters and objects in YAML, not just simple values.
- **Reproducibility:** Configuration is portable and version-controlled.
- **Clarity:** YAML mirrors the Python object structure, making it easy to understand and maintain.

---

## General Format

A generalized object in YAML is defined using the `__type__` key, which specifies the full Python import path of the object/class. Arguments and attributes are then provided as additional keys.

**General YAML Format:**

```yaml
object_name:
  __type__: <python.import.path.ClassName>
  <attribute1>: <value1>
  <attribute2>: <value2>
  ...
```

- Use `__args__` for positional arguments (as a list)
- Use other keys for keyword arguments or attributes

---

## Supported Types: Python Class or Object

The generalized object feature is **extremely flexible**—it supports Python type or class that is importable in your Airflow environment. The table below shows just a few common examples, but you can use any class by specifying its full import path in `__type__`.

> **Note:** If you use a custom or third-party class, make sure it is installed and importable in your Airflow environment.

| Python Object/Class                        | YAML `__type__` Value                        | Example Use Case                |
|--------------------------------------------|----------------------------------------------|---------------------------------|
| `datetime.datetime`                        | `datetime.datetime`                          | DAG start date                  |
| `datetime.timedelta`                       | `datetime.timedelta`                         | Task timeout                    |
| `airflow.timetables.trigger.CronTriggerTimetable` | `airflow.timetables.trigger.CronTriggerTimetable` | Custom scheduling               |
| `airflow.sdk.Asset`                        | `airflow.sdk.Asset`                          | Asset definition                |
| `airflow.io.path.ObjectStoragePath`        | `airflow.io.path.ObjectStoragePath`           | Object storage path             |
| `kubernetes.client.models.V1Pod`           | `kubernetes.client.models.V1Pod`              | Kubernetes pod override         |

---

## Examples

### 1. Datetime Object

**Python:**

```python
from datetime import datetime
start_date = datetime(2025, 1, 1)
```

**YAML:**

```yaml
start_date:
  __type__: datetime.datetime
  year: 2025
  month: 1
  day: 1
```

### 2. Timedelta Object

**Python:**

```python
from datetime import timedelta
execution_timeout = timedelta(hours=1)
```

**YAML:**

```yaml
execution_timeout:
  __type__: datetime.timedelta
  hours: 1
```

### 3. Airflow Timetable

**Python:**

```python
from airflow.timetables.trigger import CronTriggerTimetable
schedule = CronTriggerTimetable(cron="* * * * *", timezone="UTC")
```

**YAML:**

```yaml
schedule:
  __type__: airflow.timetables.trigger.CronTriggerTimetable
  cron: "* * * * *"
  timezone: UTC
```

### 4. Asset Object

**Python:**

```python
from airflow.sdk import Asset
my_asset = Asset(name="sales_report", uri="s3://")
```

**YAML:**

```yaml
my_asset:
  __type__: airflow.sdk.Asset
  name: "sales_report"
  uri: "s3://"
```

### 5. ObjectStoragePath with Positional Arguments

**Python:**

```python
from airflow.io.path import ObjectStoragePath
my_obj_storage = ObjectStoragePath("file:///data/object_storage_ops.csv")
```

**YAML:**

```yaml
my_obj_storage:
  __type__: airflow.io.path.ObjectStoragePath
  __args__:
    - file:///data/object_storage_ops.csv
```

### 6. Kubernetes Object (Nested Example)

**Python:**

```python
from kubernetes.client.models import V1Pod, V1PodSpec, V1Container, V1ResourceRequirements
pod_override = V1Pod(
    spec=V1PodSpec(
        containers=[
            V1Container(
                name="base",
                resources=V1ResourceRequirements(
                    limits={"cpu": "1", "memory": "1024Mi"},
                    requests={"cpu": "0.5", "memory": "512Mi"}
                )
            )
        ]
    )
)
```

**YAML:**

```yaml
pod_override:
  __type__: kubernetes.client.models.V1Pod
  spec:
    __type__: kubernetes.client.models.V1PodSpec
    containers:
      __type__: builtins.list
      items:
        - __type__: kubernetes.client.models.V1Container
          name: "base"
          resources:
            __type__: kubernetes.client.models.V1ResourceRequirements
            limits:
              cpu: "1"
              memory: "1024Mi"
            requests:
              cpu: "0.5"
              memory: "512Mi"
```

---

## Tips & Best Practices

- Always specify the full import path in `__type__` for clarity and reliability.
- Use `__args__` for positional arguments, and other keys for keyword arguments.
- For lists, use `__type__: builtins.list` and provide an `items` key with a list of objects.
- You can nest generalized objects to any depth, mirroring complex Python object graphs.
- This feature is especially useful for Airflow 3+ and advanced use cases (e.g., custom timetables, pod overrides).

---

## Troubleshooting

- **Import Errors:** Ensure the Python class/module referenced in `__type__` is installed and importable in your Airflow environment.
- **Type Errors:** Double-check the arguments and structure match the Python class signature.
- **YAML Syntax:** Indentation and structure matter! Use a YAML linter if you encounter parsing issues.

---
