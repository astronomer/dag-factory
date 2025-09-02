# Configuring Your Workflows

DAG Factory allows you to define workflows in a structured, configuration-driven way using YAML files.
You can define multiple workflows within a single YAML file based on your requirements.

## Key Elements of Workflow Configuration

- **dag_id**: Unique identifier for your DAG.
- **default_args**: Common arguments for all tasks.
- **schedule**: Specifies the execution schedule.
- **tasks**: Defines the [Airflow tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html) in your workflow.
- **task_groups**: Defines [Airflow task groups](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups) to organize and group related tasks.

## Example DAG Configuration

### Task and Task Group Configuration Formats

DAG Factory supports two formats for defining `tasks` and `task_groups`:

#### List Format (Recommended)

The **list format** is the recommended and more readable approach. In this format, tasks are defined as a list where each task includes a `task_id` field, and task groups are also defined as a list where each group includes a `group_name` field:

!!! info "Version Support"
    List format support was introduced in version 1.0.0.

```title="example_dag_factory.yml"
--8<-- "dev/dags/example_dag_factory.yml:example_dag_yaml_configuration"
```

#### Dictionary Format (Legacy)

The **dictionary format** is also supported for backward compatibility. In this format, tasks are defined as a dictionary where the key is the task ID, and task groups are also defined as a dictionary where the key is the group name:

```title="example_dag_factory_tasks_taskgroups_as_dict_format.yml"
--8<-- "dev/dags/example_dag_factory_tasks_taskgroups_as_dict_format.yml:example_dag_dict_configuration"
```

!!! note "Format Recommendation"
    While both formats are supported, **we recommend using the list format** as it is more readable and easier to maintain.

## Reserved Keys

The DAG Factory designates certain YAML keys for internal processing. While these keys appear in your YAML files, they are reserved exclusively for specific internal functions and should not be redefined or used for other purposes:

- `__type__`
- `__args__`
- `__join__`
- `__and__`
- `__or__`

Using these keys outside their intended internal roles may lead to unexpected behavior.

### Check out more configuration params

- [Environment variables](environment_variables.md)
- [Defaults](defaults.md)
