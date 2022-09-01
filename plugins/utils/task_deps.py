from typing import List, Optional, Union

from airflow.utils.state import State
from plugins.sensors.external_task import PelotonExternalTaskSensor
from functools import partial


def peloton_task_dependency(
    task_id: str,
    external_dag_id: Optional[str] = None,
    external_task_id: Optional[str] = None,
    upstream_task=None,
    poke_interval: float = 60,
    timeout: float = 60 * 60,
    executed_hours_ago=24,
    executed_same_day=True,
    allowed_states: Optional[List[Union[State, str]]] = None,
    failed_states: Optional[List[Union[State, str]]] = None,
    **kwargs
):
    sensor_kwargs = kwargs or dict()
    sensor_kwargs["poke_interval"] = poke_interval
    sensor_kwargs["timeout"] = timeout
    allowed_states = allowed_states if allowed_states is not None else [State.SUCCESS]
    failed_states = failed_states if failed_states is not None else [State.FAILED]
    assert any([external_dag_id, upstream_task]), "Either external_dag_id or upstream_task needs to be provided"

    if upstream_task:
        ext_dag_id = upstream_task.dag_id
        ext_task_id = upstream_task.task_id
    else:
        ext_dag_id = external_dag_id
        ext_task_id = external_task_id

    return PelotonExternalTaskSensor(
        task_id=task_id,
        external_dag_id=ext_dag_id,
        external_task_id=ext_task_id,
        executed_hours_ago=executed_hours_ago,
        executed_same_day=executed_same_day,
        allowed_states=allowed_states,
        failed_states=failed_states,
        **sensor_kwargs
    )


depends_on_task = partial(
    peloton_task_dependency,
    allowed_states=[State.SUCCESS],
    failed_states=[
        State.FAILED,
        State.UPSTREAM_FAILED,
        State.SKIPPED,
        State.REMOVED
    ],
)

wait_on_task = partial(
    peloton_task_dependency,
    allowed_states=[
        State.SUCCESS,
        State.FAILED,
        State.NONE,
        State.UPSTREAM_FAILED,
        State.SKIPPED,
        State.REMOVED
    ],
    failed_states=[],
)
