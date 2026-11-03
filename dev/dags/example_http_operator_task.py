from pathlib import Path

try:
    from airflow.providers.http.operators.http import HttpOperator  # noqa: F401
except ImportError:
    raise ImportError("Package apache-airflow-providers-http is not installed.")

# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent

config_file = str(CONFIG_ROOT_DIR / "example_http_operator_task.yml")


load_yaml_dags(
    globals_dict=globals(),
    config_filepath=config_file,
)
