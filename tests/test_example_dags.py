from __future__ import annotations

from functools import cache
from pathlib import Path

import airflow
import pytest
from airflow.models.dagbag import DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from packaging.version import Version

from . import utils as test_utils

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
AIRFLOW_VERSION = Version(airflow.__version__)
# TODO: Enable asset_triggered_dags.py once https://github.com/apache/airflow/issues/51644 is solved
IGNORED_DAG_FILES = ["example_callbacks.py", "example_http_operator_task.py", "asset_triggered_dags.py", "kpo.py"]

MIN_VER_DAG_FILE_VER: dict[str, list[str]] = {
    "2.5": [
        "example_pypi_stats_dagfactory",
        "example_hackernews_dagfactory",
        "example_hackernews_plain_airflow",
        "example_pypi_stats_plain_airflow",
    ],
    "2.9": ["example_map_index_template.py", "example_object_storage.py"],
}


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

        if AIRFLOW_VERSION < Version("3.0.0"):
            file.writelines(["example_load_airflow3_dags.py\n"])

        if AIRFLOW_VERSION >= Version("3.0.0"):
            # TODO: https://github.com/astronomer/dag-factory/issues/437
            file.writelines(["example_dag_datasets.py\n"])
            file.writelines(["example_load_airflow2_dags.py\n"])

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

    # This feature is available since Airflow 2.5:
    # https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#airflow-2-5-0-2022-12-02
    dag_run = None
    if AIRFLOW_VERSION >= Version("2.5"):
        dag_run = dag.test()
    else:
        dag_run = test_utils.run_dag(dag)

    if dag_run is not None:
        assert dag_run.state == DagRunState.SUCCESS
