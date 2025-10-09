## TaskFlow Pipeline with `multiple_outputs=True`

DAG Factory supports Airflow’s
[Pythonic Dags with the TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html),
enabling "getitem" shorthand for TaskFlow-style decorated tasks that return mappings (multiple_outputs=True).

### What it enables

- When a TaskFlow-decorated task returns a dict (for example, using `multiple_outputs=True`), downstream task arguments can reference either the whole returned mapping or a single key from it using a concise syntax.

### Syntax

- `+task_id` (with default `multiple_outputs=False`) — reference the entire Python function return value (the pushed value from the task). This is useful when the upstream task returns a mapping and the downstream callable expects the return_value.
- `+task_id['key']` or `+task_id["key"]` — reference a single value from the mapping returned by the upstream TaskFlow task.

### Examples

Given a TaskFlow task `collect` that returns a multiple_outputs dict like:

```
@task(multiple_ouputs=True)
def collect(**context):
    return {"key1": "value1", "key2": "value2"}
```

It's representation in dag_factory yaml is like
```
- task_id: collect
  multiple_outputs: true
  decorator: airflow.decorators.task
  python_callable: sample.collect
```

Then you can use a single value from the mapping with:

```
value: "+collect['key1']"
```