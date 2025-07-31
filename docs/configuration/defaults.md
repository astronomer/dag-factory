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
the `default_args` of a DAG, but also default Tasks. These Tasks provide a "template" for the DAGs defined in this file.
Each DAG (`machine_learning`, `data_science`, `artificial_intelligence`) will be defined using the values from the
`default` block, and like with `default_args`, can override these values. **This is a powerful way to use DAG Factory
to dynamically create DAGs using a single configuration.**


```yaml title="Usage of default block in YAML"
--8<-- "dev/dags/example_dag_factory_default_config.yml"
```

## Configuration Inheritance with `__extends__`

Starting with DAG Factory 0.23.0, you can create modular, reusable configuration files using the `__extends__` feature. This allows you to build configuration hierarchies by extending other YAML configuration files, promoting better organization and reusability of common settings.

### Benefits of using `__extends__`

- **Modularity**: Split configurations into logical, reusable components
- **Maintainability**: Centralize common configurations and reduce duplication
- **Flexibility**: Support multiple inheritance levels and chaining
- **Organization**: Create clear configuration hierarchies (base → environment → team → DAG)

### How `__extends__` works

When a YAML file contains an `__extends__` key, DAG Factory will:

1. Load the specified configuration files in order
2. Merge their `default` sections with the current configuration
3. Apply the same precedence rules as other default configurations
4. Support chaining: extended files can also have their own `__extends__` keys

### Example usage of `__extends__`

#### Basic extension

**File: `extends_base.yml`**
```yaml
default:
  default_args:
    owner: "data_team"
    retries: 2
    retry_delay_sec: 300
  schedule_interval: "@daily"
  tags: ["dag-factory"]
```

**File: `my_dags.yml`**
```yaml
__extends__:
  - "extends_base.yml"

default:
  default_args:
    owner: "analytics_team"  # Overrides "data_team"
    start_date: "2024-01-01"  # Added to inherited config

my_analytics_dag:
  description: "Analytics pipeline"
  tasks:
    extract_data:
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo 'Extracting data...'"
```

#### Chained extension

You can create inheritance chains where configurations extend other configurations:

```yaml
# production_dags.yml extends environment_config.yml which extends base_config.yml
__extends__:
  - "environment_config.yml"

default:
  default_args:
    owner: "prod_team"  # Final override
```

### Configuration precedence with `__extends__`

When using `__extends__` along with other default configuration methods, the following precedence order applies:

1. In the DAG configuration (highest priority)
2. In the `default` block within the workflow's YAML file (including extended configuration files)
3. In the `defaults.yml` (lowest priority)

Note: Extended configuration files are merged into the `default` block during config loading, so they share the same priority level as the main configuration's `default` section.

Currently, only `default_args` can be specified using the `defaults.yml` file.
