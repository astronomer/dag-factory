import os
from pathlib import Path

try:
    from airflow.providers.http.operators.http import HttpOperator  # noqa: F401

    HTTP_OPERATOR_AVAILABLE = True
    SIMPLE_HTTP_OPERATOR_AVAILABLE = False
except ImportError:
    try:
        from airflow.providers.http.operators.http import SimpleHttpOperator  # noqa: F401

        SIMPLE_HTTP_OPERATOR_AVAILABLE = True
        HTTP_OPERATOR_AVAILABLE = False
    except ImportError:
        HTTP_OPERATOR_AVAILABLE = False
        SIMPLE_HTTP_OPERATOR_AVAILABLE = False
        raise ImportError("Package apache-airflow-providers-http is not installed.")

# The following import is here so Airflow parses this file
# from airflow import DAG
import dagfactory

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"

CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))
if HTTP_OPERATOR_AVAILABLE:
    config_file = str(CONFIG_ROOT_DIR / "example_http_operator_task.yml")
elif SIMPLE_HTTP_OPERATOR_AVAILABLE:
    config_file = str(CONFIG_ROOT_DIR / "example_simple_http_operator_task.yml")
else:
    raise ImportError("Package apache-airflow-providers-http is not installed.")

example_dag_factory = dagfactory.DagFactory(config_file)

# Creating task dependencies
example_dag_factory.generate_dags(globals())
