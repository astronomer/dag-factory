# Custom Python Object

**DAG-Factory** supports the ability to define custom Python objects directly within your YAML configuration. This is extremely useful for advanced Airflow features, such as specifying Kubernetes pod overrides or other operator-specific configurations using native Python types—without writing Python code.

This is achieved via a special `key: __type__`, which allows recursive construction of Python objects from their fully qualified type names and YAML-defined attributes.

## Example

Below is a real-world example of how to define a DAG with Kubernetes-specific executor configuration using `__type__`.

```yaml
--8<-- "dev/dags/airflow2/example_custom_py_object_dag.yml"
```
