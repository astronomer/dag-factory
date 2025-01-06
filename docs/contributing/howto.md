# Contributing Guide

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

As contributors and maintainers to this project, you are expected to abide by the
[Contributor Code of Conduct](code_of_conduct.md).

Learn more about the contributors roles in [this page](roles.md).

This document describes how to contribute to DAG Factory, covering:

1. Overview in how to contribute
2. How to set up the local development environment
3. Running tests
4. Pre-commit and linting
5. Authoring the documentation
6. Releasing

## 1. Overview in how to contribute

To contribute to the DAG Factory project:

1. Please create a `GitHub Issue <https://github.com/astronomer/dag-factory/issues>`_ describing a bug, enhancement or feature request
2. Open a branch off of the ``main`` branch and create a Pull Request into the ``main`` branch from your feature branch
3. Link your issue to the pull request
4. Once the development is complete on your feature branch, request a review, and it will be merged once approved.

## 2. Setup a local development environment

### Requirements

* [Git](https://git-scm.com/)
* [Python](https://www.python.org/) <= 3.12 (due to dependencies, such as ``google-re2`` not supporting Python 3.13 yet)

Clone the **DAG Factory** repository and change the current working directory to the repo's root directory:

```bash
git clone https://github.com/astronomer/dag-factory.git
cd dag-factory/
```

Once the project is cloned, there are two options for development:

* Use a Python virtual environment, or
* Use Docker

### a) Using a Python virtual environment for local development

Install the project dependencies:

```bash
make setup
```

Activate the local python environment:

```bash
source venv/bin/activate
```

Set airflow home to the ``dev/``, so you can see DAG Factory example DAGs.
Disabled loading Airflow standard example DAGs:

```bash
export AIRFLOW_HOME=$(pwd)/dev/
export AIRFLOW__CORE__LOAD_EXAMPLES=false
```

Then, run Airflow in standalone mode, command below will create a new user (if not exist) and run necessary airflow component (webserver, scheduler and triggerer):

> Note: By default, Airflow will use sqlite as database, you can overwrite this by set variable ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`` to the sql connection string.

```bash
airflow standalone
```

Once Airflow is up, you can access the Airflow UI at ``http://localhost:8080``.

> Note: whenever you want to start the development server, you need to activate the ``virtualenv`` and set the ``environment variables``

### b) Using Docker for local development

It is also possible to build the development environment using [Docker](https://www.docker.com/products/docker-desktop/):

```bash
make docker-run
```

Once the sandbox is up, you can access the Airflow UI at ``http://localhost:8080``.

This approach builds a DAG Factory wheel, so if there are code changes, you'll have to stop and restart the containers:

```bash
make docker-stop
```

## 3. Testing application with hatch

We currently use `hatch <https://github.com/pypa/hatch>`_ for building and distributing ``dag-factory``.

The tool can also be used for local development. The `pyproject.toml <https://github.com/astronomer/dag-factory/blob/main/pyproject.toml>`_ file currently defines a matrix of supported versions of Python and Airflow for which a user can run the tests against.

For instance, to run the tests using Python 3.10 and `Apache Airflow® <https://airflow.apache.org/>`_ 2.5, use the following:

### a) Running unit tests

```bash
hatch run tests.py3.10-2.5:test-cov
```

It is also possible to run the tests using all the matrix combinations, by using:

```bash
hatch run tests:test-cov
```

### b) Running integration tests

> Note: these tests will create local Python virtual environments in a hatch managed directory
> They will also use the user-defined `AIRFLOW_HOME`, overriding any pre-existing `airflow.cfg` and `airflow.db` files

First, set the following environment variables:

```bash
export AIRFLOW_HOME=$(pwd)/dev/
export CONFIG_ROOT_DIR=`pwd`"/dev/dags"
export PYTHONPATH=dev/dags:$PYTHONPATH
```

To run the integration tests using Python 3.9 and Airflow 2.9, use

```bash
hatch run tests.py3.9-2.9:test-integration-setup
hatch run tests.py3.9-2.9:test-integration
```

## 4. Pre-Commit and linting

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run:

```bash
pre-commit install
```

To run the checks manually, run:

```bash
pre-commit run --all-files
```

Pre-commit runs several static checks, including Black amd Ruff. They can also be run using ``hatch``:

```bash
hatch run tests.py3.9-2.9:static-check
```

## 5. Authoring the documentation

We use Markdown to author DAG Factory documentation.

Similar to running tests, we also use hatch to manage the documentation.

To build the documentation locally:

```bash
hatch run docs:build
```

To serve the documentation locally in `localhost:8080`:

```bash
hatch run docs:serve
```

To release the documentation with the current project version, and set it to the latest:

```bash
hatch run docs:release
```

## 6. Releasing

We use GitHub actions to create and deploy new releases. To create a new release, update the new release version.

This can be done either by using hatch:

> Note: You can update the version in a few different ways. Check out the `hatch docs <https://hatch.pypa.io/latest/version/#updating>`_ to learn more.

```bash
hatch version minor
```

Or by manually updating the value of `__version__` in `dagfactory/__init__.py`.

Make sure the [CHANGELOG file](https://github.com/astronomer/dag-factory/blob/main/CHANGELOG.md) is up-to-date.

Create a release using the [GitHub UI](https://github.com/astronomer/dag-factory/releases/new). The release will be automatically deployed to PyPI.

If you're a [project maintainer in PyPI](https://pypi.org/project/dag-factory/), it is also possible to create a release manually,
by authenticating to PyPI and running the commands:

```bash
hatch build
hatch publish
```
