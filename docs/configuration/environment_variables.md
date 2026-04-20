# Environment variables

Starting release `0.20.0`, DAG Factory introduces support for referencing environment variables directly within YAML
configuration files. This enhancement enables dynamic configuration paths and enhances workflow portability by
resolving environment variables during DAG parsing.

With this feature, DAG Factory removes the reliance on hard-coded paths, allowing for more flexible and adaptable
configurations that work seamlessly across various environments.

## Example YAML Configuration with Environment Variables

```title="Reference environment variable in YAML"
--8<-- "dev/dags/airflow2/example_dag_factory_multiple_config.yml:environment_variable_example"
```

In the above example, `$CONFIG_ROOT_DIR` is used to reference an environment variable that points to the root
directory of your DAG configurations. During DAG parsing, it will be resolved to the value specified for the
`CONFIG_ROOT_DIR` environment variable.

## `strict_mode`

| Setting | Default |
|---------|---------|
| `[dag_factory] strict_mode` | `False` |
| `AIRFLOW__DAG_FACTORY__STRICT_MODE` | `false` |

By default, DAGs that are misformatted never raise an error, which can mislead users into thinking that the lack of
an error means all DAGs have been rendered. When `AIRFLOW__DAG_FACTORY__STRICT_MODE=true`, DAG Factory raises an error for all failed DAGs,
causing Airflow to record a file-level import error and making the problem visible in the Airflow UI. This is the recommended mode for
CI/CD pipelines where silent failures are not acceptable.

