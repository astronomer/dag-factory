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

## Strict Mode

By default DAG Factory logs the error and skips DAGs that fail to build so other DAGs in the same YAML file
or folder can still be loaded. Setting strict mode raises an exception for every problematic DAG while still
registering all DAGs that built successfully.

| Variable | Section | Key | Default |
|---|---|---|---|
| `AIRFLOW__DAG_FACTORY__STRICT_MODE` | `dag_factory` | `strict_mode` | `False` |

```bash
AIRFLOW__DAG_FACTORY__STRICT_MODE=true
```

When strict mode is enabled:

- Each DAG in a YAML file is attempted independently; successfully built DAGs are registered before any
  exception is raised.
- If one or more DAGs fail to build, a `DagFactoryConfigException` is raised after registration so Airflow
  surfaces the error as a broken-DAG import error for that file.
- In folder-scan mode, each YAML file is still processed independently — a broken file does not prevent other
  files from loading.
