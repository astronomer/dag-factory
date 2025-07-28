# Defaults

DAG Factory allows you to define default values for DAG-level arguments and Airflow
[default_args](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#default-arguments). There are several ways to accomplish this:

- Define the `default_args` within each DAG definition in the DAGs YAML file;
- Declare a `default` block within the toplevel of the DAGs YAML file;
- Define the `default_args_config_dict` argument when instantiating the `DAGFactory` class;
- Create one or multiple `defaults.yml` and declare the `default_args_config_path` argument in the `DAGFactory` class. This approach includes support for combining multiple `defaults.yml` files.

Although you cannot use the last two configurations together, you can use a combination of the first two configurations with either the third or the last.

Below, we detail how to use each of these approaches and also how to combine them.

## Specifying `default_args` directly in the DAG YAML specification

This configuration affects only the DAG where the `default_args` are defined.

You can override or define specific `default_args` at the individual DAG level. This strategy allows you to customize arguments for each DAG without affecting others. Not only can existing `default_args` be overridden directly in a DAG configuration, but also adding new arguments.

```yaml
etl:
   default_args:
      start_date: '2024-12-31'
      retries: 1  # A new default_arg was added
...
```

## YAML top-level `default`

This configuration affects all the DAGs defined in the YAML file where the `default` block is declared.

The `default` top-level block enables you to share standard settings and configurations across all DAGs in your YAML configuration, with the arguments automatically applied to each DAG defined in the file.
It is one of DAG Factory's most powerful features; using defaults allows for the dynamic generation of multiple DAGs.

### Benefits of using the `default` block

- Consistency: Ensures uniform configurations across all tasks and DAGs.
- Maintainability: Reduces duplication by centralizing common properties.
- Simplicity: Makes configurations easier to read and manage.
- Dynamic Generation: Use a single default block to generate more than a single DAG easily.

### Example usage of the `default` block

#### Specifying `default_args` in the `default` block

Using a `default` block in a YAML file allows for those key-value pairs to apply to each DAG defined in that same file. One of the most common examples is using a `default` block to specify `default_args` for each DAG defined in that file. Every DAG defined in the file inherits these arguments. Below is an example of this.

   ```yaml title="Usage of default block for default_args in YAML"
   --8<-- "dev/dags/airflow2/example_dag_factory_default_args.yml"
   ```

#### Example using of default block for dynamic DAG generation

 Not only can the `default` block in a YAML file be used to define `default_args` for one or more DAGs, you can also use it to create the skeleton of "templated" DAGs. In the example below, the `default` block defines both the `default_args` of a DAG, and also default Tasks. These Tasks provide a "template" for the DAGs defined in this file. Each DAG (`machine_learning`, `data_science`, `artificial_intelligence`) is defined using the values from the `default` block, and, like with `default_args`, can override these values. **This is a powerful way to use DAG Factory to dynamically create DAGs using a single configuration.**

 ```yaml title="Usage of default block in YAML"
   --8<-- "dev/dags/airflow2/example_dag_factory_default_config.yml"
 ```

## Specifying `default` arguments via a Python dictionary

This configuration affects DAGs created using the `DagFactory` class with the `default_args_config_dict` argument.

It allows you to define DAG-level arguments, including the `default_args`, using Python dictionaries.

### Example of using a Python-defined default configuration

```yaml title="Usage of default block in YAML"
--8<-- "dev/dags/airflow2/example_dag_factory_default_config.yml"
```

 In this example, users create a YAML DAG by using the `DagFactory` class and declare default arguments in the form of a Python dictionary by setting the `default_args_config_dict` parameter in the `DAGFactory` class. This feature mirrors the functionality of
 manually specifying a `default_args_config_path` in the `DAGFactory` class, described in the next section.

```python title="Usage of default_args_config_dict in .py file"
--8<-- "dev/dags/example_dag_factory_default_config_dict.py:13:19"
```

## Declaring default values using the `defaults.yml` file

This configuration affects DAGs created using the `DagFactory` class without the `default_args_config_dict` argument.

If a `defaults.yml` file is present in the same directory as the YAML file representing the DAG, DagFactory will use it to build the DAG.

If the `defaults.yml` is added to a separate directory, users can share it using `DagFactory`'s argument `default_args_config_path`.

### Example of declaring the `default_args` using `defaults.yml`

 Starting DAG Factory 0.22.0, you can also keep the `default_args` in the `defaults.yml` file. The configuration
 from `defaults.yml` will be applied to all DAG Factory-generated DAGs. **Be careful, DagFactory will apply these to all
 generated DAGs.**

 ```yaml title="defaults.yml"
   --8<-- "dev/dags/defaults.yml"
 ```

### Example usage of DAG-level configurations in `defaults.yml`

   In Airflow, not all DAG-level arguments are supported under `default_args`, because they are DAG-specific and not
   used by any operator classes, such as `schedule` and `catchup`. To set default values for those arguments, they need
   to be added at the root level of `defaults.yml` as follows:

   ```yaml
   schedule: 0 1 * * *   # set DAG-specific arguments at the root level
   catchup: False

   default_args:
      start_date: '2024-12-31'
  ...
   ```

### Combining multiple `defaults.yml` files

It is possible to combine and merge the content of multiple `defaults.yml` files.

To accomplish this, you should declare in the `default_args_config_path` a folder that is a parent folder of a DAG-defined `YAML` file. In this case, DAG Factory will merge all the `defaults.yml` configurations, following the directories' hierarchy, and give precedence to the arguments declared in the `defaults.yml` file closest to the DAG YAML file.

As an example, let's say there are DagFactory DAGs defined inside the `a/b/c/some_dags.yml` file following this directory tree:

```shell
sample_project
└── a
    ├── b
    │   ├── c
    │   │   ├── defaults.yml
    │   │   └── some_dags.yml
    │   └── defaults.yml
    └── defaults.yml
```

Assuming you instantiate the DAG by using:

```python
DagFactory(
   "a/b/c/some_dags.yml",
   default_args_config_path="a"
)`

The DAG will be using the default configuration defined in all the following files:

- `a/b/c/some_dags.yml`
- `a/b/c/defaults.yml`
- `a/b/defaults.yml`
- `a/defaults.yml`

Following this precedence order. Illustrating, if the DAG `owner` is declared both in `a/b/c/defaults.yml` and in `a/defaults.yml`, the one that takes precedence is the `a/b/c/defaults.yml`, since it is closer to the DAG YAML file.

## Combining multiple methods of defining default values

Given the various ways to specify top-level DAG arguments, including `default_args`, the following precedence order is applied if multiple places define the same argument:

1. In the DAG configuration
2. In the `default` block within the workflow's YAML file
3. The arguments defined in `default_args_config_dict`
4. If (3) is not declared, the `defaults.yml` hierarchy.
