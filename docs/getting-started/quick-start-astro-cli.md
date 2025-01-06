# DAG Factory Quick Start Guide

**DAG Factory** is a Python library [Apache Airflow®](https://airflow.apache.org) that simplifies DAG creation using declarative YAML configuration files instead of Python.

## Prerequisites

The minimum requirements for **dag-factory** are:

- Python 3.8.0+
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/)

## Step 1: Initialize Airflow Project

Create a new directory and initialize your Astro CLI project:

```commandline
mkdir dag-factory-quick-start && cd dag-factory-quick-start

astro dev init
```

This will set up the necessary Airflow files and directories.

## Step 2: Install DAG Factory

Install DAG Factory in your Airflow environment:

1. Add dag-factory as a dependency to the `requirements.txt` file created during the project initialization.


## Step 3: Define a DAG in YAML

**DAG Factory** uses YAML files to define DAG configurations. Create a file named `example_dag_factory.yml` in the `$AIRFLOW_HOME/dags` folder with the following content:

```title="example_dag_factory.yml"
--8<-- "dev/dags/example_dag_factory.yml"
```

## Step 4: Generate the DAG from YAML

Create a Python script named `example_dag_factory.py` in the `$AIRFLOW_HOME/dags` folder. This script will generate the DAG from the YAML configuration

```title="example_dag_factory.py"
--8<-- "dev/dags/example_dag_factory.py"
```

## Step 5: Start Airflow Project

Once you've set up your YAML configuration and Python script, start the Airflow environment with the following command:

```commandline
astro dev start
```

This will take a few minutes to set up. Once completed, you can access the Airflow UI and the generated DAG at http://localhost:8080 🚀.

## View Your Generated DAG

Once Airflow is up and running, you can login with the username `admin` and the password `admin`. You should be able to see your generated DAG in the Airflow UI.

**Generated DAG**

![Airflow DAG](../static/images/airflow-home.png)

**Graph View**

![Airflow Home](../static/images/airflow-dag.png)

Checkout [examples](https://github.com/astronomer/dag-factory/tree/main/dev/dags) for generating more advanced DAGs.
