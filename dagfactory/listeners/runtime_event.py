from __future__ import annotations

from airflow.listeners import hookimpl

try:
    from airflow.sdk.definitions.dag import DAG
except ImportError:
    from airflow.models.dag import DAG
import hashlib

from airflow.models.dagrun import DagRun
from airflow.version import version as AIRFLOW_VERSION
from packaging import version

from dagfactory import telemetry

INSTALLED_AIRFLOW_VERSION = version.parse(AIRFLOW_VERSION)


class EventStatus:
    SUCCESS = "success"
    FAILED = "failed"


DAG_RUN = "dag_run"


def is_dagfactory_dag(dag: DAG | None = None):
    if "dagfactory" in dag.tags:
        return True
    return False


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    dag = dag_run.get_dag()
    if not is_dagfactory_dag(dag):
        return

    if INSTALLED_AIRFLOW_VERSION < version.Version("3.0.0"):
        dag_hash = dag_run.dag_hash
    else:
        dag_id_str = str(dag_run.dag_id)
        dag_hash = hashlib.md5(dag_id_str.encode("utf-8")).hexdigest()

    additional_telemetry_metrics = {
        "dag_hash": dag_hash,
        "status": EventStatus.SUCCESS,
        "task_count": len(dag.task_ids),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    dag = dag_run.get_dag()
    if not is_dagfactory_dag(dag):
        return

    if INSTALLED_AIRFLOW_VERSION < version.Version("3.0.0"):
        dag_hash = dag_run.dag_hash
    else:
        dag_id_str = str(dag_run.dag_id)
        dag_hash = hashlib.md5(dag_id_str.encode("utf-8")).hexdigest()

    additional_telemetry_metrics = {
        "dag_hash": dag_hash,
        "status": EventStatus.FAILED,
        "task_count": len(dag.task_ids),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)
