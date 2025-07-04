# Params

**Params** in Airflow are a way to pass dynamic runtime configuration to tasks within a DAG. They allow tasks to be more flexible by enabling templated values to be injected during execution.

## Key Features

- Available in DAG and task definitions.
- Can be templated using Jinja.
- Useful for customizing task behavior without modifying code.

## Example DAG

```yaml
--8<-- "dev/dags/example_params.yml"
```

## When to Use

- You want to reuse the same DAG for different input values.
- You want to change a task’s behavior at runtime.
