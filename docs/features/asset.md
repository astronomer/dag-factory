# Asset

DAG Factory supports Airflow's [Asset](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html), which allows you to define data assets that DAGs can emit or depend on.

To leverage `asset`, use the [`__type__`](./../configuration/custom_py_object.md) annotation to define asset metadata. You can then reference assets using `inlets` and `outlets` to emit and track asset events.

## Example DAG

First, define your DAG configuration in a YAML file

```yaml title="asset_triggered_dags.yml"
--8<-- "dev/dags/asset_triggered_dags.yml"
```

Then, you can load this YAML configuration dynamically

```python title="asset_triggered_dags.py"
--8<-- "dev/dags/asset_triggered_dags.py"
```
