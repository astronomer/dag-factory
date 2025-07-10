import os
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"
CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))

config_file = str(CONFIG_ROOT_DIR / "example_dag_factory_default_config_dict.yml")

example_dag_factory = dagfactory.DagFactory(
    config_file, default_args_config_dict={"default_args": {"start_date": "2025-01-01", "owner": "global_owner"}}
)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
