# Environment variables

Starting release `0.20.0`, DAG Factory introduces support for referencing environment variables directly within YAML
configuration files. This enhancement enables dynamic configuration paths and enhances workflow portability by
resolving environment variables during DAG parsing.

With this feature, DAG Factory removes the reliance on hard-coded paths, allowing for more flexible and adaptable
configurations that work seamlessly across various environments.

## Example YAML Configuration with Environment Variables

```title="Reference environment variable in YAML"
--8<-- "dev/dags/example_dag_factory_multiple_config.yml:environment_variable_example"
```

In the above example, `$CONFIG_ROOT_DIR` is used to reference an environment variable that points to the root
directory of your DAG configurations. During DAG parsing, it will be resolved to the value specified for the
`CONFIG_ROOT_DIR` environment variable.
