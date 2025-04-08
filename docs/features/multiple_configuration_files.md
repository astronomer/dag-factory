# Multiple Configuration Files

Using **DAG-Factory** if you want to split your DAG configuration into multiple files, you can do so by leveraging a suffix in the configuration file name.

```python
    from dagfactory import load_yaml_dags  # load relevant YAML files as airflow DAGs

    load_yaml_dags(globals_dict=globals(), suffix=['dag.yaml'])
```
