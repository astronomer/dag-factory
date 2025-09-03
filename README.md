# dag-factory

[![Github Actions](https://github.com/astronomer/dag-factory/actions/workflows/cicd.yaml/badge.svg?branch=main&event=push)](https://github.com/astronomer/dag-factory/actions?workflow=build)
[![Coverage](https://codecov.io/github/astronomer/dag-factory/coverage.svg?branch=master)](https://codecov.io/github/astronomer/dag-factory?branch=master)
[![PyPi](https://img.shields.io/pypi/v/dag-factory.svg)](https://pypi.org/project/dag-factory/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Downloads](https://img.shields.io/pypi/dm/dag-factory.svg)](https://img.shields.io/pypi/dm/dag-factory)

<img alt=analytics referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=2bb92a5b-beb3-48cc-a722-79dda1089eda" />

Welcome to *dag-factory*! *dag-factory* is a library for [Apache Airflow®](https://airflow.apache.org) to construct DAGs
declaratively via configuration files.

The minimum requirements for **dag-factory** are:

- Python 3.9.0+
- [Apache Airflow®](https://airflow.apache.org) 2.4+

For a gentle introduction, please take a look at our [Quickstart Guide](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-airflow-standalone/). For more examples, please see the
[examples](/examples) folder.

- Quickstart
    - [Astro CLI](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-astro-cli/)
    - [Airflow Standalone](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-airflow-standalone/)
- [Benefits](#benefits)
- Features
    - [load_yml_dags Function](https://astronomer.github.io/dag-factory/latest/configuration/load_yaml_dags/)
    - [Schedule](https://astronomer.github.io/dag-factory/latest/configuration/schedule/)
    - [Dynamically Mapped Tasks](https://astronomer.github.io/dag-factory/latest/features/dynamic_tasks/)
    - [Define Python Object](https://astronomer.github.io/dag-factory/latest/configuration/custom_py_object/)
    - [Custom Operators](https://astronomer.github.io/dag-factory/latest/features/custom_operators/)
    - [Defaults](https://astronomer.github.io/dag-factory/latest/configuration/defaults/)
    - [Callbacks](https://astronomer.github.io/dag-factory/latest/features/callbacks/)
    - [KubernetesPodOperator](https://astronomer.github.io/dag-factory/latest/features/kpo/)
- [Contributing](https://astronomer.github.io/dag-factory/latest/contributing/howto/)

## Benefits

- Construct DAGs without knowing Python
- Construct DAGs without learning Airflow primitives
- Avoid duplicative code
- Everyone loves YAML! ;)

## 📢 Dag-Factory 1.0 Released

Version **1.0** introduces important improvements and breaking changes to support modern Airflow usage.

👉 See the [Migration Guide](https://astronomer.github.io/dag-factory/latest/migration_guide/) to upgrade from earlier versions.

## 🚀 Dag-Factory Supports Airflow 3

DAG-Factory is compatible with **Apache Airflow 3** and supports modern scheduling, and updated import paths.

## License

To learn more about the terms and conditions for use, reproduction and distribution, read the [Apache License 2.0](https://github.com/astronomer/dag-factory/blob/main/LICENSE).

## Privacy Notice

This project follows [Astronomer's Privacy Policy](https://www.astronomer.io/privacy/).

For further information, [read this](https://github.com/astronomer/dag-factory/blob/main/PRIVACY_NOTICE.md)

## Security Policy

Check the project's [Security Policy](https://github.com/astronomer/dag-factory/blob/main/SECURITY.md) to learn
how to report security vulnerabilities in DAG Factory and how security issues reported to the DAG Factory
security team are handled.
