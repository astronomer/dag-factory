from datetime import datetime, timedelta
import os
import pendulum
from typing import Optional, Iterable

from airflow import AirflowException
from airflow.models import TaskInstance, DagRun, DagModel, DagBag
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.session import provide_session
from airflow.utils.state import State

import pendulum

utc = pendulum.timezone("UTC")
from sqlalchemy import func


class PelotonExternalTaskSensor(BaseSensorOperator):
    """
    This sensor waits for a different dag or a task in a different DAG to complete.
    Unlike the airflow.sensors.external_task.ExternalTaskSensor, which uses the execution date to match the state of the
    dependent task, this operator simply checks the latest state for the task.

    :param external_dag_id:
    :param
    """

    @apply_defaults
    def __init__(
        self,
        *,
        external_dag_id: str,
        external_task_id: Optional[str],
        executed_hours_ago=24,
        executed_same_day=True,
        check_existence: bool = False,
        allowed_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        super(PelotonExternalTaskSensor, self).__init__(**kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.allowed_states = (
            list(allowed_states) if allowed_states is not None else [State.SUCCESS]
        )
        self.failed_states = (
            list(failed_states) if failed_states is not None else [State.FAILED, State.UP_FOR_RETRY]
        )
        self.check_existence = check_existence
        self._has_checked_existence = False
        self.executed_hours_ago = executed_hours_ago
        self.executed_same_day = executed_same_day

    def _check_for_existence(self, session) -> None:
        dag_to_wait = (
            session.query(DagModel)
            .filter(DagModel.dag_id == self.external_dag_id)
            .first()
        )

        if not dag_to_wait:
            raise AirflowException(
                f"The external DAG {self.external_dag_id} does not exist."
            )

        if not os.path.exists(dag_to_wait.fileloc):
            raise AirflowException(
                f"The external DAG {self.external_dag_id} was deleted."
            )

        if self.external_task_id:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(
                self.external_dag_id
            )
            if not refreshed_dag_info.has_task(self.external_task_id):
                raise AirflowException(
                    f"The external task {self.external_task_id} in "
                    f"DAG {self.external_dag_id} does not exist."
                )
        self._has_checked_existence = True

    @provide_session
    def poke(self, context, session=None):
        if self.check_existence and not self._has_checked_existence:
            self._check_for_existence(session)
        latest_state = self._get_latest_state(session)
        if latest_state in self.failed_states:
            if self.external_task_id:
                raise AirflowException(
                    f"The external task {self.external_task_id} in DAG {self.external_dag_id} failed."
                )
            else:
                raise AirflowException(
                    f"The external DAG {self.external_dag_id} failed."
                )
        return latest_state in self.allowed_states

    def _get_latest_state(self, session):
        time_limit = (
            datetime.now()
            - timedelta(hours=self.executed_hours_ago)
        ).astimezone(utc)

        today_midnight = time_limit
        if self.executed_same_day:
            today_midnight = datetime.now().replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            ).astimezone(utc)

        if self.external_task_id:
            latest_execution = (
                session.query(func.max(TaskInstance.start_date).label("maxdate"))
                .filter(
                    TaskInstance.dag_id == self.external_dag_id,
                    TaskInstance.task_id == self.external_task_id,
                )
                .subquery()
            )

            state = (
                session.query(TaskInstance.state)
                .filter(
                    TaskInstance.dag_id == self.external_dag_id,
                    TaskInstance.task_id == self.external_task_id,
                    TaskInstance.start_date > time_limit,
                    TaskInstance.start_date > today_midnight,
                    TaskInstance.start_date == latest_execution.c.maxdate,
                )
                .first()
            )
            self.log.info(
                f"Task state for task {self.external_task_id} on dag {self.external_dag_id} is {state}"
            )
        else:
            latest_execution = (
                session.query(func.max(DagRun.start_date).label("maxdate"))
                .filter(
                    DagRun.dag_id == self.external_dag_id,
                )
                .subquery()
            )
            state = (
                session.query(DagRun.state)
                .filter(
                    DagRun.dag_id == self.external_dag_id,
                    DagRun.start_date > time_limit,
                    DagRun.start_date > today_midnight,
                    DagRun.start_date == latest_execution.c.maxdate,
                )
                .first()
            )
            self.log.info(f"DAG state for dag {self.external_dag_id} is {state}")
        if state:
            return state[0]