from __future__ import annotations
from pathlib import Path

import airflow
from airflow.models.dagbag import DagBag
from packaging.version import Version


EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "examples"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
AIRFLOW_VERSION = Version(airflow.__version__)


MIN_VER_DAG_FILE_VER: dict[str, list[str]] = {
    "2.3": ["example_dynamic_task_mapping.py"],
}


def test_no_import_errors():
    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for min_version, files in MIN_VER_DAG_FILE_VER.items():
            if AIRFLOW_VERSION < Version(min_version):
                print(f"Adding {files} to .airflowignore")
                file.writelines([f"{file}\n" for file in files])

    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)
    assert db.dags
    assert not db.import_errors
