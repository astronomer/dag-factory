# dag-factory

[![Github Actions](https://github.com/astronomer/dag-factory/actions/workflows/cicd.yaml/badge.svg?branch=main&event=push)](https://github.com/astronomer/dag-factory/actions?workflow=build)
[![Coverage](https://codecov.io/github/astronomer/dag-factory/coverage.svg?branch=master)](https://codecov.io/github/astronomer/dag-factory?branch=master)
[![PyPi](https://img.shields.io/pypi/v/dag-factory.svg)](https://pypi.org/project/dag-factory/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Downloads](https://img.shields.io/pypi/dm/dag-factory.svg)](https://img.shields.io/pypi/dm/dag-factory)

<img alt=analytics referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=2bb92a5b-beb3-48cc-a722-79dda1089eda" />

Welcome to *dag-factory*! *dag-factory* is a library for [Apache Airflow®](https://airflow.apache.org) that builds DAGs
from declarative YAML files. The logic inside each task stays in Python. The workflow around it, meaning the schedule,
the tasks, and their dependencies, lives in a small, fixed format that is easy to review, check, and update across many
DAGs, whether people or AI agents write them.

The minimum requirements for **dag-factory** are:

- Python 3.11.0+
- [Apache Airflow®](https://airflow.apache.org) 2.11+

For a gentle introduction, please take a look at our [Quickstart Guide](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-airflow-standalone/). For more examples, please see the
[examples](/examples/dags/) folder.

- Quickstart
    - [Astro CLI](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-astro-cli/)
    - [Airflow Standalone](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-airflow-standalone/)
- [Why dag-factory?](#why-dag-factory)
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

## Why dag-factory?

With AI coding agents, writing an Airflow DAG in Python is cheap. For a team with many DAGs and many authors, the
expensive part is reviewing, checking, and changing those DAGs later. dag-factory is built for that:

- Constrained: a workflow is a definition, not a program. The schedule, tasks, and dependencies always sit in the same
  places, so every DAG has the same shape no matter who or what wrote it.
- Verifiable: YAML has a fixed grammar, so a config can be checked without running anything, and policy checks can run
  before a DAG reaches Airflow.
- Maintainable: `dagfactory convert` migrates a whole fleet of YAML DAGs from Airflow 2 to Airflow 3 in one
  deterministic step.
- Less repetition: `defaults.yml` files and `default` blocks set shared values once for many DAGs.

For a team with a few DAGs, AI-written Python is fine. See [Why DAG Factory?](docs/why-dag-factory.md) for the full
reasoning and when Python fits better.

## 📢 Dag-Factory 1.0 Released

Version **1.0** introduces important improvements and breaking changes to support modern Airflow usage.

👉 See the [Migration Guide](https://astronomer.github.io/dag-factory/latest/migration_guide/) to upgrade from earlier versions.

## 🚀 Dag-Factory Supports Airflow 3

DAG-Factory is compatible with **Apache Airflow 3** and supports modern scheduling, and updated import paths.

## Community

Join us on the Apache Airflow Slack Workspace at [#yaml-dags](https://apache-airflow.slack.com/archives/C099751BPHT)

## License

To learn more about the terms and conditions for use, reproduction and distribution, read the [Apache License 2.0](https://github.com/astronomer/dag-factory/blob/main/LICENSE).

## Privacy Notice

This project follows [Astronomer's Privacy Policy](https://www.astronomer.io/privacy/).

For further information, [read this](https://github.com/astronomer/dag-factory/blob/main/PRIVACY_NOTICE.md)

## Security Policy

Check the project's [Security Policy](https://github.com/astronomer/dag-factory/blob/main/SECURITY.md) to learn
how to report security vulnerabilities in DAG Factory and how security issues reported to the DAG Factory
security team are handled.
