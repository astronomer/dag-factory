# DAG Factory: Quick Start Guide With Airflow

**DAG Factory** is a Python library [Apache Airflow®](https://airflow.apache.org) that simplifies DAG creation using declarative YAML configuration files instead of Python.

## Prerequisites

The minimum requirements for **dag-factory** are:

- Python 3.9.0+
- [Apache Airflow®](https://airflow.apache.org) 2.4+

## Step 1: Create a Python Virtual Environment

Create and activate a virtual environment:

```commandline
python3 -m venv dagfactory_env
source dagfactory_env/bin/activate
```

## Step 2: Install Apache Airflow

Install [Apache Airflow®](https://airflow.apache.org):

1. Create a directory for your project and navigate to it:

   ```commandline
   mkdir dag-factory-quick-start && cd dag-factory-quick-start
   ```

2. Set the `AIRFLOW_HOME` environment variable:

   ```commandline
   export AIRFLOW_HOME=$(pwd)
   export AIRFLOW__CORE__LOAD_EXAMPLES=False
   ```

3. Install Apache Airflow:

   ```commandline
   pip install apache-airflow
   ```

## Step 3: Install DAG Factory

Install the DAG Factory library in your virtual environment:

```commandline
pip install dag-factory
```

## Step 4: Set Up the DAGS Folder

Create a DAGs folder inside the $AIRFLOW_HOME directory, which is where your DAGs will be stored:

```commandline
mkdir dags
```

## Step 5: Define a DAG in YAML

**DAG Factory** uses YAML files to define DAG configurations. Create a file named `example_dag_factory.yml` in the `$AIRFLOW_HOME/dags` folder with the following content:

```title="example_dag_factory.yml"
--8<-- "dev/dags/example_dag_factory.yml"
```

## Step 6: Generate the DAG from YAML

Create a Python script named `example_dag_factory.py` in the `$AIRFLOW_HOME/dags` folder. This script will generate the DAG from the YAML configuration

```title="example_dag_factory.py"
--8<-- "dev/dags/example_dag_factory.py"
```

## Step 7: Start Airflow

To start the Airflow environment with your DAG Factory setup, run the following command:

```commandline
airflow standalone
```

This will take a few minutes to set up. Once completed, you can access the Airflow UI and the generated DAG at `http://localhost:8080` 🚀.

## View Your Generated DAG

Once Airflow is up and running, you can login with the username `admin` and the password in `$AIRFLOW_HOME/standalone_admin_password.txt`. You should be able to see your generated DAG in the Airflow UI.

## Generated DAG

![Airflow DAG](../static/images/airflow-home.png)

## Graph View

![Airflow Home](../static/images/airflow-dag.png)

Checkout [examples](https://github.com/astronomer/dag-factory/tree/main/dev/dags) for generating more advanced DAGs.
