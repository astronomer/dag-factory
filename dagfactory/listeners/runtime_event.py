from __future__ import annotations

from airflow.listeners import hookimpl
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun

from dagfactory import telemetry


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
    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.SUCCESS,
        "task_count": len(dag.task_ids),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    dag = dag_run.get_dag()
    if not is_dagfactory_dag(dag):
        return
    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.FAILED,
        "task_count": len(dag.task_ids),
    }

    telemetry.emit_usage_metrics_if_enabled(DAG_RUN, additional_telemetry_metrics)
