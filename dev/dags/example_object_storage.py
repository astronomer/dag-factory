import os
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"
CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))

config_file = str(CONFIG_ROOT_DIR / "example_object_storage.yml")
example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.generate_dags(globals())
