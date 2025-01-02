# DAG Factory Quick Start Guide

**DAG Factory** is a Python library [Apache Airflow®](https://airflow.apache.org) that simplifies DAG creation using declarative YAML configuration files.

# Prerequisites

The minimum requirements for **dag-factory** are:

- Python 3.8.0+
- [Apache Airflow®](https://airflow.apache.org) 2.0+

# Step 1: Create a Virtual Environment

First, create and activate a virtual environment:

```commandline
python3 -m venv dagfactory_env
source dagfactory_env/bin/activate
```

# Step 2: Install Apache Airflow

Next, you'll need to install [Apache Airflow®](https://airflow.apache.org). Follow these steps:

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

# Step 3: Install DAG Factory

Now, install the DAG Factory library in your virtual environment:

```commandline
pip install dag-factory
```

# Step 4: Set Up the DAGS Folder

Create a dags folder inside the $AIRFLOW_HOME directory, which is where your DAGs will be stored:

```commandline
mkdir dags
```

# Step 5: Define a DAG in YAML

**DAG Factory** uses YAML files to define DAG configurations. Create a file named `example_dag_factory.yml` in the `$AIRFLOW_HOME/dags` folder with the following content:

```yaml
default:
  default_args:
    catchup: false,
    start_date: 2024-11-11

example_dag:
  default_args:
    owner: "custom_owner"
  description: "this is an example dag"
  schedule_interval: "0 3 * * *"
  render_template_as_native_obj: True
  tasks:
    task_1:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: "echo 1"
    task_2:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: "echo 2"
      dependencies: [task_1]
    task_3:
      operator: airflow.operators.bash_operator.BashOperator
      bash_command: "echo 2"
      dependencies: [task_1]
```

# Step 6: Generate the DAG from YAML

Create a Python script named `example_dag_factory_dag.py` in the `$AIRFLOW_HOME/dags` folder. This script will generate the DAG from the YAML configuration

```python
import os
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

CONFIG_ROOT_DIR = Path(os.getenv("AIRFLOW_HOME", ""))

config_file = str(CONFIG_ROOT_DIR / "dags/example_dag_factory.yml")

example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
```

# Step 7: Start Airflow

To start the Airflow environment with your DAG Factory setup, run the following command:

```commandline
airflow standalone
```

This will take a few minutes to set up. Once completed, you can access the Airflow UI and the generated DAG at http://localhost:8080 🚀.

# View Your Generated DAG

Once Airflow is up and running, you can login with the username `admin` and the password in `$AIRFLOW_HOME/standalone_admin_password.txt`. You should be able to see your generated DAG in the Airflow UI.

**Generated DAG**

![Airflow DAG](./static/images/airflow-home.png)

**Graph View**

![Airflow Home](./static/images/airflow-dag.png)


Checkout [examples](https://github.com/astronomer/dag-factory/tree/main/dev/dags) for generating more advanced DAGs.
