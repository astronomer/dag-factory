# Defaults

DAG Factory allows you to define Airflow
[default_args](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#default-arguments) and
additional DAG-level arguments in a `default` block. This block enables you to share common settings across all DAGs in
your YAML configuration, with the arguments automatically applied to each DAG defined in the file.

## Benefits of using the default block

- Consistency: Ensures uniform configurations across all tasks and DAGs.
- Maintainability: Reduces duplication by centralizing common properties.
- Simplicity: Makes configurations easier to read and manage.

### Example usage of default block

```title="Usage of default block in YAML"
--8<-- "dev/dags/example_task_group.yml"
```

The arguments specified in the `default` block, such as `default_args`, `default_view`, `max_active_runs`,
`schedule_interval`, and any others defined, will be applied to all the DAGs in the YAML configuration.

## Multiple ways for specifying Airflow default_args

DAG Factory offers flexibility in defining Airflow’s `default_args`. These can be specified in several ways, depending on your requirements.

1. Specifying `default_args` in the `default` block

    As seen in the previous example, you can define shared `default_args` for all DAGs in the configuration YAML under
the `default` block. These arguments are automatically inherited by every DAG defined in the file.

2. Specifying `default_args` directly in a DAG configuration

    You can override or define specific `default_args` at the individual DAG level. This allows you to customize arguments
for each DAG without affecting others.

    Example:

    ```title="DAG level default_args"
    --8<-- "dev/dags/example_dag_factory.yml"
    ```

3. Specifying `default_args` in a shared `defaults.yml`

    Starting DAG Factory 0.22.0, you can also keep the `default_args` in the `defaults.yml` file. The configuration
from `defaults.yml` is applied to all DAG Factory generated DAGs.

    ```title="defaults.yml"
    --8<-- "dev/dags/defaults.yml"
    ```

Given the various ways to specify `default_args`, the following precedence order is applied when arguments are
duplicated:

1. In the DAG configuration
2. In the `default` block within the workflow's YAML file
3. In the `defaults.yml`
