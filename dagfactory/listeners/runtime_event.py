from __future__ import annotations

from airflow.listeners import hookimpl
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

from dagfactory import telemetry

log = LoggingMixin().log


class EventStatus:
    SUCCESS = "success"
    FAILED = "failed"


class EventType:
    DAG_RUN = "dag_run"
    TASK_INSTANCE = "task_instance"


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.SUCCESS,
    }

    telemetry.emit_usage_metrics_if_enabled(EventType.DAG_RUN, additional_telemetry_metrics)


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    additional_telemetry_metrics = {
        "dag_hash": dag_run.dag_hash,
        "status": EventStatus.FAILED,
    }

    telemetry.emit_usage_metrics_if_enabled(EventType.DAG_RUN, additional_telemetry_metrics)


@hookimpl
def on_task_instance_success(previous_state: TaskInstanceState, task_instance: TaskInstance, session):
    additional_telemetry_metrics = {
        "dag_hash": task_instance.dag_run.dag_hash,
        "status": EventStatus.SUCCESS,
        "operator": task_instance.operator,
    }

    telemetry.emit_usage_metrics_if_enabled(EventType.TASK_INSTANCE, additional_telemetry_metrics)


@hookimpl
def on_task_instance_failed(
    previous_state: TaskInstanceState, task_instance: TaskInstance, error: None | str | BaseException, session
):
    additional_telemetry_metrics = {
        "dag_hash": task_instance.dag_run.dag_hash,
        "status": EventStatus.FAILED,
        "operator": task_instance.operator,
    }

    telemetry.emit_usage_metrics_if_enabled(EventType.TASK_INSTANCE, additional_telemetry_metrics)
