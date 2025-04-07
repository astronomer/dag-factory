from __future__ import annotations

from pathlib import Path

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import airflow
import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from packaging.version import Version

from . import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
AIRFLOW_VERSION = Version(airflow.__version__)
IGNORED_DAG_FILES = ["example_callbacks.py"]

MIN_VER_DAG_FILE_VER: dict[str, list[str]] = {
    # TaskFlow examples unrelated to dynamic task mapping work in earlier versions
    "2.3": ["example_dynamic_task_mapping.py", "example_taskflow.py"],
    "2.5": [
        "example_pypi_stats_dagfactory",
        "example_hackernews_dagfactory",
        "example_hackernews_plain_airflow",
        "example_pypi_stats_plain_airflow",
    ],
    "2.7": ["example_map_index_template.py"],
    "2.4": ["example_external_sensor_dag.py"],
}

# Add HTTP operator DAG to ignored files for providers-http versions without HttpOperator
try:
    from airflow.providers.http.operators.http import HttpOperator
    HTTP_OPERATOR_AVAILABLE = True
except ImportError:
    HTTP_OPERATOR_AVAILABLE = False


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


@cache
def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""

    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for min_version, files in MIN_VER_DAG_FILE_VER.items():
            if AIRFLOW_VERSION < Version(min_version):
                print(f"Adding {files} to .airflowignore")
                file.writelines([f"{file}\n" for file in files])

        for dagfile in IGNORED_DAG_FILES:
            print(f"Adding {dagfile} to .airflowignore")
            file.writelines([f"{dagfile}\n"])

    # Print the contents of the .airflowignore file, and build the DagBag
    print(".airflowignore contents: ")
    print(AIRFLOW_IGNORE_FILE.read_text())
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)

    assert db.dags
    assert not db.import_errors
    return db


def get_dag_ids() -> list[str]:
    dag_bag = get_dag_bag()
    return dag_bag.dag_ids


@pytest.mark.integration
@pytest.mark.parametrize("dag_id", get_dag_ids())
def test_example_dag(session, dag_id: str):
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag(dag_id)

    # Skip http_operator_example_dag in older Airflow versions without HttpOperator
    if dag_id == "http_operator_example_dag" and not HTTP_OPERATOR_AVAILABLE:
        pytest.skip(f"Skipping {dag_id} because HttpOperator is not available")
    
    # Skip http_operator_example_dag in older Airflow versions 
    # since it has compatibility issues with our connection handling
    if dag_id == "http_operator_example_dag" and AIRFLOW_VERSION < Version("2.7.0"):
        pytest.skip(f"Skipping {dag_id} on Airflow version {AIRFLOW_VERSION}")
    
    # This feature is available since Airflow 2.5:
    # https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#airflow-2-5-0-2022-12-02
    if AIRFLOW_VERSION >= Version("2.5"):
        dag.test()
    else:
        test_utils.run_dag(dag)
