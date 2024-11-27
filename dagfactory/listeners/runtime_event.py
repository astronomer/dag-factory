from __future__ import annotations

from airflow.listeners import hookimpl
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState
from packaging import version

from dagfactory import telemetry

try:
    from airflow.version import version as AIRFLOW_VERSION
except ImportError:
    from airflow import __version__ as AIRFLOW_VERSION

log = LoggingMixin().log


class EventStatus:
    SUCCESS = "success"
    FAILED = "failed"


class EventType:
    DAG_RUN = "dag_run"
    TASK_INSTANCE = "task_instance"


def is_dagfactory_dag(dag: DAG | None = None):
    if "dagfactory" in dag.tags:
        return True
    return False


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    if not is_dagfactory_dag(dag_run.get_dag()):
        return
    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.SUCCESS,
    }

    telemetry.emit_usage_metrics_if_enabled(EventType.DAG_RUN, additional_telemetry_metrics)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    if not is_dagfactory_dag(dag_run.get_dag()):
        return
    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.FAILED,
    }

    telemetry.emit_usage_metrics_if_enabled(EventType.DAG_RUN, additional_telemetry_metrics)


@hookimpl
def on_task_instance_success(previous_state: TaskInstanceState, task_instance: TaskInstance, session):
    if not is_dagfactory_dag(task_instance.dag_run.get_dag()):
        return
    additional_telemetry_metrics = {
        "dag_hash": task_instance.dag_run.dag_hash,
        "status": EventStatus.SUCCESS,
    }

    telemetry.emit_usage_metrics_if_enabled(EventType.TASK_INSTANCE, additional_telemetry_metrics)


if version.parse(AIRFLOW_VERSION) >= version.parse("2.10.0"):

    @hookimpl
    def on_task_instance_failed(
        previous_state: TaskInstanceState, task_instance: TaskInstance, error: None | str | BaseException, session
    ):
        if not is_dagfactory_dag(task_instance.dag_run.get_dag()):
            return
        additional_telemetry_metrics = {
            "dag_hash": task_instance.dag_run.dag_hash,
            "status": EventStatus.FAILED,
        }

        telemetry.emit_usage_metrics_if_enabled(EventType.TASK_INSTANCE, additional_telemetry_metrics)

else:

    @hookimpl
    def on_task_instance_failed(previous_state: TaskInstanceState, task_instance: TaskInstance, session):
        if not is_dagfactory_dag(task_instance.dag_run.get_dag()):
            return
        additional_telemetry_metrics = {
            "dag_hash": task_instance.dag_run.dag_hash,
            "status": EventStatus.FAILED,
        }

        telemetry.emit_usage_metrics_if_enabled(EventType.TASK_INSTANCE, additional_telemetry_metrics)
