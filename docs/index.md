# DAG Factory documentation

Everything you need to know about how to build Apache Airflow® workflows using YAML files.

## Getting started

Are you new to DAG Factory? This is the place to start!

- DAG Factory at a glance
    - [Quickstart with Airflow standalone](getting-started/quick-start-airflow-standalone.md)
    - [Quickstart with Astro CLI](getting-started/quick-start-astro-cli.md)

- [Using YAML instead of Python](comparison/index.md)
    - [Traditional Airflow Operators](comparison/traditional_operators.md)
    - [TaskFlow API](comparison/taskflow_api.md)

## Command Line

- [Using DAG Factory CLI features](features/cli.md)

## Configuration

- [Configuring your workflows](configuration/configuring_workflows.md)
- [load_yml_dags Function](configuration/load_yaml_dags.md)
- [Define Python Object](configuration/custom_py_object.md)
- [Defaults](configuration/defaults.md)
- [Environment variables](configuration/environment_variables.md)
- [Schedule](configuration/schedule.md)

## Features

- [Dynamic tasks](features/dynamic_tasks.md)
- [Callbacks](features/callbacks.md)
- [Custom operators](features/custom_operators.md)
- [Multiple configuration files](features/multiple_configuration_files.md)
- [Datasets scheduling](features/datasets.md)
- [Assets Scheduling](features/asset.md)
- [KubernetesPodOperator](features/kpo.md)
- [HttpSensor](features/http_task.md)

## 📢 Dag-Factory 1.0 Released

Version **1.0** introduces important improvements and breaking changes to support modern Airflow usage.

👉 See the [Migration Guide](./migration_guide.md) to upgrade from earlier versions.

## 🚀 Dag-Factory Supports Airflow 3

DAG-Factory is compatible with **Apache Airflow 3** and supports modern scheduling, and updated import paths.

## Getting help

Having trouble? We'd like to help!

- Report bugs, questions and feature requests in our [ticket tracker](https://github.com/astronomer/dag-factory/issues).

## Contributing

DAG Factory is an Open-Source project. Learn about its development process and about how you can contribute:

- [Contributing to DAG Factory](contributing/howto.md)
- [Github repository](https://github.com/astronomer/dag-factory/)

## License

To learn more about the terms and conditions for use, reproduction and distribution, read the [Apache License 2.0](https://github.com/astronomer/dag-factory/blob/main/LICENSE).

## Privacy Notice

This project follows [Astronomer's Privacy Policy](https://www.astronomer.io/privacy/).

For further information, [read this](https://github.com/astronomer/dag-factory/blob/main/PRIVACY_NOTICE.md)

## Security Policy

Check the project's [Security Policy](https://github.com/astronomer/dag-factory/blob/main/SECURITY.md) to learn
how to report security vulnerabilities in DAG Factory and how security issues reported to the DAG Factory
security team are handled.

<img alt=analytics referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=2bb92a5b-beb3-48cc-a722-79dda1089eda" />
