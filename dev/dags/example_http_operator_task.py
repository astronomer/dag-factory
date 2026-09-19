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
from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent

if HTTP_OPERATOR_AVAILABLE:
    config_file = str(CONFIG_ROOT_DIR / "example_http_operator_task.yml")
elif SIMPLE_HTTP_OPERATOR_AVAILABLE:
    config_file = str(CONFIG_ROOT_DIR / "example_simple_http_operator_task.yml")
else:
    raise ImportError("Package apache-airflow-providers-http is not installed.")


load_yaml_dags(
    globals_dict=globals(),
    config_filepath=config_file,
)
