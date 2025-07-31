# Contributing Guide

All contributions, bug reports, bug fixes, documentation improvements, and enhancements are welcome.

All contributors and maintainers to this project should abide by the [Contributor Code of Conduct](code_of_conduct.md).

Learn more about the contributors' roles in [the Roles page](roles.md).

This document describes how to contribute to DAG Factory, covering:

- Overview of how to contribute
- How to set up the local development environment
- Running tests
- Pre-commit and linting
- Authoring the documentation
- Releasing

## Overview of how to contribute

To contribute to the DAG Factory project:

1. Please create a [GitHub Issue](https://github.com/astronomer/dag-factory/issues) describing a bug, enhancement, or feature request.
2. [Fork the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo) and clone your fork locally.
3. Open a feature branch off of the main branch in your fork
4. Make your changes, push the branch to your fork, and open a Pull Request from your feature branch into the ``main`` branch of the upstream repository.
5. Link your issue to the pull request.
6. After you complete development on your feature branch, request a review. A maintainer will merge your PR after all reviewers approve it.

## Set up a local development environment

### Requirements

- [Git](https://git-scm.com/)
- [Python](https://www.python.org/) <= 3.12 (due to dependencies, such as `google-re2` not supporting Python 3.13 yet)
- [uv](https://docs.astral.sh/uv/) (for fast package management)
- [Hatch](https://hatch.pypa.io/latest/) (installed automatically via uv)

Clone the **DAG Factory** repository and change the current working directory to the repo's root directory:

```bash
git clone https://github.com/astronomer/dag-factory.git
cd dag-factory/
```

After cloning the project, there are two options for setting up the local development environment:

- Use a Python virtual environment, or
- Use Docker

### Using a Python virtual environment for local development

DAG Factory uses [uv](https://docs.astral.sh/uv/) for fast and reliable package management. The setup process is significantly faster than traditional pip-based installations.

#### 1. Install the project dependencies

Recommended: uv setup (fast, reproducible builds)

```bash
uv sync --dev
```

Alternative: Traditional setup

```bash
make setup
```

Both commands install all dependencies, including test dependencies. The uv option is significantly faster and uses the lockfile for reproducible builds.

#### 2. Activate the local python environment

For uv setup:

```bash
source .venv/bin/activate
```

For traditional setup:

```bash
source venv/bin/activate
```

### Additional uv Commands

Once you're set up with uv, you can use these helpful commands:

- `uv sync` - Sync dependencies from the lockfile
- `uv lock --upgrade` - Update the lockfile with latest dependency versions
- `uv add <package>` - Add a new dependency
- `uv remove <package>` - Remove a dependency

#### 3. Set [Apache Airflow®](https://airflow.apache.org/) home to the `dev/`, so you can see DAG Factory example DAGs

   Disable loading Airflow standard example DAGs:

```bash
export AIRFLOW_HOME=$(pwd)/dev/
export AIRFLOW__CORE__LOAD_EXAMPLES=false
```

   Set CONFIG_ROOT_DIR for locating DAG config files:

```bash
export CONFIG_ROOT_DIR=$AIRFLOW_HOME/dags
```

Then, run Airflow in standalone mode; the command below will create a new user (if it does not exist) and run the necessary Airflow component (webserver, scheduler and triggered):

> Note: By default, Airflow will use sqlite as a database; you can override this by setting the variable `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` to the SQL connection string.

```bash
airflow standalone
```

After Airflow is running, you can access the Airflow UI at `http://localhost:8080`.

> Note: whenever you want to start the development server, you need to activate the `virtualenv` and set the `environment variables`

### Use Docker for local development

It is also possible to build the development environment using [Docker](https://www.docker.com/products/docker-desktop/):

```bash
make docker-run
```

After the sandbox is running, you can access the Airflow UI at `http://localhost:8080`.

This approach builds a DAG Factory wheel, so if there are code changes, you must stop and restart the containers:

```bash
make docker-stop
```

## Testing application with hatch

The tests are developed using PyTest and run using hatch.

The [pyproject. toml](https://github.com/astronomer/dag-factory/blob/main/pyproject.toml) file currently defines a matrix of supported versions of Python and Airflow against which a user can run the tests.

### Run unit tests

!!! note
    - These tests create local Python virtual environments in a hatch-managed directory.

If you have YAMLs written for Airflow 2 and would like them to be run for Airflow 3 tests, set the following environment variable for DAG Factory to convert and make them compatible with Airflow 3:

```bash
export AUTO_CONVERT_TO_AF3=true
```

To run unit tests using Python 3.10 and Airflow 2.5, use the following:

```bash
hatch run tests.py3.10-2.5:test-cov
```

It is also possible to run the tests using all the matrix combinations, by using:

```bash
hatch run tests:test-cov
```

### Run integration tests

!!! note
    - These tests create local Python virtual environments within a `hatch`-managed directory.
    - They also use the user-defined `AIRFLOW_HOME`, overriding any pre-existing `airflow.cfg` and `airflow.db` files.
    - The `AUTO_CONVERT_TO_AF3` environment variable is required to run tests in the Airflow 3 environment.

First, set the following environment variables:

```bash
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=90
export AIRFLOW_HOME=$(pwd)/dev/
export CONFIG_ROOT_DIR=$(pwd)/dev/dags
export PYTHONPATH=$(pwd)/dev/dags:$PYTHONPATH
export AUTO_CONVERT_TO_AF3=true
```

To run the integration tests using Python 3.9 and Airflow 2.9, use

```bash
hatch run tests.py3.9-2.9:test-integration-setup
hatch run tests.py3.9-2.9:test-integration
```

## Pre-Commit and linting

We use pre-commit to run several checks on the code before committing. To install pre-commit hooks, run:

```bash
pre-commit install
```

To run the checks manually, run the following:

```bash
pre-commit run --all-files
```

Pre-commit runs several static checks, including Black and Ruff. It is also possible to run them using `hatch`:

```bash
hatch run tests.py3.9-2.9:static-check
```

## Write docs

We use Markdown to author DAG Factory documentation.

Similar to running tests, we also use hatch to manage the documentation.

To build and serve the documentation locally:

```bash
hatch run docs:dev
```

To release the documentation with the current project version and set it to the latest:

```bash
hatch run docs:gh-release
```

## Releasing

We currently use [hatch](https://github.com/pypa/hatch) for building and distributing `dag-factory`.

We use GitHub actions to create and deploy new releases. To create a new release, update the latest release version.

It is possible to update the version either by using hatch:

> Note: You can update the version in several different ways. To learn more, check out the [hatch docs](https://hatch.pypa.io/latest/version/#updating).

```bash
hatch version minor
```

Or by manually updating the value of `__version__` in `dagfactory/__init__.py`.

Make sure the [CHANGELOG file](https://github.com/astronomer/dag-factory/blob/main/CHANGELOG.md) is up-to-date.

Create a release using the [GitHub UI](https://github.com/astronomer/dag-factory/releases/new). GitHub will update the package directly to [PyPI](https://pypi.org/project/dag-factory/).

If you're a [project maintainer in PyPI](https://pypi.org/project/dag-factory/), it is also possible to create a release manually,
by authenticating to PyPI and running the commands:

```bash
uv build --wheel --sdist
uv publish --token <your-pypi-token>
```
