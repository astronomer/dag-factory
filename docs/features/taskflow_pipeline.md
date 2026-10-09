# TaskFlow Pipeline with `multiple_outputs=True`

DAG Factory supports Airflow’s
[Pythonic Dags with the TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html),
enabling a shared output-reference shorthand for TaskFlow tasks and traditional operators.

## What it enables

- Downstream tasks can reference the whole return value or a named XCom entry from either producer type.
- The same shorthand works in TaskFlow callable arguments and traditional operators' templated arguments, including nested values and `partial` arguments.

## Syntax

- `+task_id` — reference the producer's `return_value`, including when `multiple_outputs=True`.
- `+task_id['key']` or `+task_id["key"]` — select the named XCom entry `key`. The producer must publish that entry using `multiple_outputs=True` or an explicit `xcom_push`; this does not extract a field from the dictionary stored in `return_value`.
- `task_id.output` and `task_id.output["key"]` are also supported, with the same meaning.

Airflow's restriction on mapping directly over a custom-key XCom is preserved.
See [Named XCom output references](dynamic_tasks.md#named-xcom-output-references)
for the forwarding pattern and traditional operator examples.

## Examples

Given a TaskFlow task `collect` that returns a multiple_outputs dict like:

```python
@task(multiple_outputs=True)
def collect(**context):
    return {"key1": "value1", "key2": "value2"}
```

Its representation in dag_factory YAML looks like:

```yaml
- task_id: collect
  multiple_outputs: true
  decorator: airflow.decorators.task
  python_callable: sample.collect
```

Then you can use a single value from the mapping with:

```yaml
value: "+collect['key1']"
```
