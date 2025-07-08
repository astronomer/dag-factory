# Defaults

DAG Factory allows you to define Airflow
[default_args](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#default-arguments) and
additional DAG-level arguments in a `default` block. This block enables you to share common settings and configurations
across all DAGs in your YAML configuration, with the arguments automatically applied to each DAG defined in the file.
This is one of DAG Factory's most powerful features; using defaults allows for the dynamic generation of more than a
single DAG.

## Benefits of using the default block

- Consistency: Ensures uniform configurations across all tasks and DAGs.
- Maintainability: Reduces duplication by centralizing common properties.
- Simplicity: Makes configurations easier to read and manage.
- Dynamic Generation: Use a single default block to easily generate more than a single DAG.

### Example usage of a default block for `default_args`

#### Specifying `default_args` in the `default` block

   Using a `default` block in a YAML file allows for those key-value pairs to be applied to each DAG that is defined in
   that same file. One of the most common examples is using a `default` block to specify `default_args` for each DAG
   defined in that file. These arguments are automatically inherited by every DAG defined in the file. Below is an example of this.

   ```yaml title="Usage of default block for default_args in YAML"
   --8<-- "dev/dags/example_dag_factory_default_args.yml"
   ```

#### Specifying `default_args` directly in a DAG configuration

   You can override or define specific `default_args` at the individual DAG level. This allows you to customize
   arguments for each DAG without affecting others. Not only can existing `default_args` be overridden directly in a DAG
   configuration, but new arguments can be added.

   ```yaml
   etl:
     default_args:
       start_date: '2024-12-31'
       retries: 1  # A new default_arg was added
  ...
   ```

#### Specifying `default_args` in a shared `defaults.yml`

   Starting DAG Factory 0.22.0, you can also keep the `default_args` in the `defaults.yml` file. The configuration
   from `defaults.yml` will be applied to all DAG Factory generated DAGs. **Be careful, these will be applied to all
   generated DAGs.**

   ```yaml title="defaults.yml"
   --8<-- "dev/dags/defaults.yml"
   ```

   Given the various ways to specify `default_args`, the following precedence order is applied when arguments are
   duplicated:

   1. In the DAG configuration
   2. In the `default` block within the workflow's YAML file
   3. In the `defaults.yml`

### Example using of default block for dynamic DAG generation

   Not only can the `default` block in a YAML file be used to define `default_args` for one or more DAGs; it can also be
   used to create the skeleton of "templated" DAGs. In the example below, the `default` block is used to define not only
   the `default_args` of a DAG, but also default Tasks. These Tasks provide a "template" for the DAGs defined in this
   file. Each DAG (`machine_learning`, `data_science`, `artificial_intelligence`) will be defined using the values from
   the `default` block, and like with `default_args`, can override these values. **This is a powerful way to use DAG
   Factory to dynamically create DAGs using a single configuration.**


   ```yaml title="Usage of default block in YAML"
   --8<-- "dev/dags/example_dag_factory_default_config.yml"
   ```

### Specifying `default_args` in a `.py` file

   In the `.py` used to instantiate a DAG defined using YAML, default arguments in the form of a Python dictionary can
   be set using the `default_args_config_dict` parameter in the `DAGFactory` class. This mirrors the functionality of
   manually specifying a `default_args_config_ypath` in the `DAGFactory` class.

   ```python title="Usage of default_args_config_dict in .py file"
   --8<-- "dev/dags/example_dag_factory_default_config_dict.py:13:19"
   ```
