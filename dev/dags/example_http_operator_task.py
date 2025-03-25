import os
from pathlib import Path

try:
    from airflow.providers.http.operators.http import HttpOperator
    HTTP_OPERATOR_AVAILABLE = True
except ImportError:
    HTTP_OPERATOR_AVAILABLE = False

# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"

CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))
if HTTP_OPERATOR_AVAILABLE:
    config_file = str(CONFIG_ROOT_DIR / "example_http_operator_task.yml")
else:
    config_file = str(CONFIG_ROOT_DIR / "example_simple_http_operator_task.yml")

example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.clean_dags(globals())
example_dag_factory.generate_dags(globals())
