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

## DAG Factory runtime settings

DAG Factory exposes Airflow-style configuration options under the `[dag_factory]` section of `airflow.cfg`.
Each option can also be set via the standard Airflow environment variable convention
`AIRFLOW__DAG_FACTORY__<KEY>`.

### `strict_mode`

| Setting | Default |
|---------|---------|
| `[dag_factory] strict_mode` | `False` |
| `AIRFLOW__DAG_FACTORY__STRICT_MODE` | `false` |

Controls how DAG Factory handles errors that occur while building individual DAGs from a YAML config file.

**Default behaviour (`strict_mode = False`):**
When a single DAG definition fails to build (e.g. invalid operator, missing required field), DAG Factory
logs the error with a full traceback and continues building the remaining DAGs in the same file.
Healthy DAGs are still registered and visible in Airflow. Only the broken DAG is skipped.

**Strict mode (`strict_mode = True`):**
DAG Factory still attempts to build every DAG in the file (so all failures are collected and logged),
but after the loop it raises a `DagFactoryConfigException` listing every DAG that failed.
This causes Airflow to record a file-level import error, making the problem visible in the Airflow UI
and preventing any partially-loaded state from going unnoticed.

Strict mode is recommended for CI/CD pipelines and environments where silent failures are not acceptable.

**Enable via environment variable:**

```bash
export AIRFLOW__DAG_FACTORY__STRICT_MODE=true
```

**Enable via `airflow.cfg`:**

```ini
[dag_factory]
strict_mode = True
```
