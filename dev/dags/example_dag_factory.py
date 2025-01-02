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
