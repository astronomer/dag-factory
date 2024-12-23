**DAG Factory** is a library for [Apache Airflow®](https://airflow.apache.org) to construct DAGs declaratively via configuration files.

# Prerequisites

The minimum requirements for **dag-factory** are:

- Python 3.8.0+
- [Apache Airflow®](https://airflow.apache.org) 2.0+

# Install

Install Dag-Factory in your Airflow environment using pip:

```bash
pip install dag-factory
```

# Example DAG

**Dag-Factory** uses YAML files to define DAG configurations. Create a ``.yml`` file that contains the DAG configuration. Below is a basic example of a YAML configuration for a DAG

```yaml
default:
  default_args:
    start_date: 2024-11-11
    retries: 1
    retry_delay_sec: 30
  concurrency: 1
  max_active_runs: 1
  dagrun_timeout_sec: 600
  schedule_interval: "0 1 * * *"

example_dag:
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
```

Next, You will need a Python script to generate the DAG from the YAML configuration file. Here's an example:

```python
import os
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"
CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))

config_file = str(CONFIG_ROOT_DIR / "example_dag_factory.yml")

example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
```

[More Examples](https://github.com/astronomer/dag-factory/tree/main/dev/dags)
