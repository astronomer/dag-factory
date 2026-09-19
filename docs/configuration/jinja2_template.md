# Jinja2 Template

This page shows how to use [Apache Airflow®](https://airflow.apache.org/) Jinja2 templating within a YAML-based DAG definition using DAG-Factory.

## Built-in Variables

Airflow exposes a set of built-in Jinja2 variables (e.g. `ds`, `run_id`, `task`) that are available in all task templates without any configuration.

### Example DAG

```yaml
--8<-- "dev/dags/airflow2/example_jinja2_template_dag.yml"
```

## User-Defined Macros

Airflow allows you to extend the Jinja2 templating environment with custom macros via the `user_defined_macros` DAG parameter. These macros are callable functions or values made available in all task templates of the DAG.

In DAG-Factory, `user_defined_macros` values are specified as dotted import paths (e.g. `module.function`) and are imported at DAG build time. Nested dicts are supported, allowing you to group related macros under a namespace.

### Example DAG

```yaml
--8<-- "dev/dags/airflow3/example_user_defined_macros.yml"
```

### Accessing macros in templates

- **Top-level callable**: `{{ add_days(ds, -1) }}`
- **Nested callable**: `{{ date_utils.shift(ds, -1) }}`
