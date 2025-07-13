# Configuring Your Workflows

DAG Factory allows you to define workflows in a structured, configuration-driven way using YAML files.
You can define multiple workflows within a single YAML file based on your requirements.

## Key Elements of Workflow Configuration

- **dag_id**: Unique identifier for your DAG.
- **default_args**: Common arguments for all tasks.
- **schedule**/**schedule_interval**: Specifies the execution schedule.
- **tasks**: Defines the [Airflow tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html) in your workflow.

### Example DAG Configuration

```title="example_dag_factory.yml"
--8<-- "dev/dags/airflow2/example_dag_factory.yml:example_dag_yaml_configuration"
```

### Check out more configuration params

- [Environment variables](environment_variables.md)
- [Defaults](defaults.md)
