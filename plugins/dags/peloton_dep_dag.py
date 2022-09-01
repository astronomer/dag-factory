import logging
from typing import List, Optional, Union

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import BaseOperator

from plugins.utils.task_deps import wait_on_task, depends_on_task
from plugins.utils.slack_callbacks import dag_start_slack_alert, dag_success_slack_alert

logger = logging.getLogger(__file__)


def assert_datacron_job_names(name):
    assert name.endswith('.sh'), f"The job name in datacron has to ends with .sh, but got {name}"


def assert_datacron_dep_struct(dep_dict):
    assert 'upstream_job' in dep_dict, "Must provide upstream_job if datacron dependencies are defined"
    assert_datacron_job_names(dep_dict['upstream_job'])
    dep_dict['poke_interval'] = dep_dict.get('poke_interval', 300)
    dep_dict['timeout'] = dep_dict.get('timeout', 300 * 60)


def assert_dag_dep_struct(dep_dict):
    assert 'external_dag_id' in dep_dict, "Must provide external_dag_id if depend on external dag"
    dep_dict['poke_interval'] = dep_dict.get('poke_interval', 300)
    dep_dict['timeout'] = dep_dict.get('timeout', 300 * 60)


def assert_task_dep_struct(dep_dict):
    upstream_task = dep_dict.get('upstream_task', None)
    external_dag_id = dep_dict.get('external_dag_id', None)
    external_task_id = dep_dict.get('external_task_id', None)
    assert any([
        upstream_task,
        external_dag_id and external_task_id
    ]), "Must provide either upstream_task or (external dag id and task id) if depend on external task"
    if upstream_task:
        assert isinstance(upstream_task, BaseOperator), "upstream_task must be of class BaseOperator"
    dep_dict['poke_interval'] = dep_dict.get('poke_interval', 300)
    dep_dict['timeout'] = dep_dict.get('timeout', 300 * 60)


class PelotonDepDag(DAG):
    """
    subclass of DAG to facilitate dependency needs.
    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII)
    :type dag_id: str
    :param depend_on_dags: a list of dictionary defining the upstream dags within airflow. eg. [{'external_dag_id':'dag1', 'poke_interval': 60, 'timeout': 360}]
    :type depend_on_dags: list[dict]
    :param wait_on_dags: a list of dictionary defining the upstream dags that cannot run simultaneously with this dag within airflow. eg. [{'external_dag_id':'dag2', 'poke_interval': 60, 'timeout': 360}]
    :type wait_on_dags: list[dict]
    :param depend_on_tasks: a list of dictionary defining the upstream tasks within airflow. eg. [{'upstream_task':sleep, 'poke_interval': 60, 'timeout': 360}] (need to import the task sleep from corresponding dag)
    :type depend_on_tasks: list[dict]
    :param wait_on_tasks: a list of dictionary defining the upstream tasks that cannot run simultaneously with this dag within airflow. eg. [{'upstream_task':sleep, 'poke_interval': 60, 'timeout': 360}] (need to import the task sleep from corresponding dag)
    :type wait_on_tasks: list[dict]
    :param alert_on_start: whether slack alert is sent on execution of the dag, default is false
    :type alert_on_start: boolean
    :param alert_on_finish: whether slack alert is sent on completion of the dag, default is false
    :type alert_on_finish: boolean
    all other parameters inherited from DAG would also apply
    """

    def __init__(
            self,
            dag_id: str,
            depend_on_dags: Optional[List[dict]] = None,
            wait_on_dags: Optional[List[dict]] = None,
            depend_on_tasks: Optional[List[dict]] = None,
            wait_on_tasks: Optional[List[dict]] = None,
            alert_on_start: bool = False,
            alert_on_finish: bool = False,
            *args,
            **kwargs
    ):
        super(PelotonDepDag, self).__init__(dag_id, *args, **kwargs)

        if depend_on_dags:
            for depend_on_d in depend_on_dags:
                assert_dag_dep_struct(depend_on_d)

        if wait_on_dags:
            for wait_on_d in wait_on_dags:
                assert_dag_dep_struct(wait_on_d)

        if depend_on_tasks:
            for depend_on_t in depend_on_tasks:
                assert_task_dep_struct(depend_on_t)

        if wait_on_tasks:
            for wait_on_t in wait_on_tasks:
                assert_task_dep_struct(wait_on_t)

        self.depend_on_dags = depend_on_dags
        self.wait_on_dags = wait_on_dags
        self.depend_on_tasks = depend_on_tasks
        self.wait_on_tasks = wait_on_tasks
        self.alert_on_start = alert_on_start
        self.alert_on_finish = alert_on_finish

    def __exit__(self, _type, _value, _tb):
        roots = self.roots
        leaves = self.leaves

        if any([
            self.depend_on_dags,
            self.wait_on_dags,
            self.depend_on_tasks,
            self.wait_on_tasks,
            self.alert_on_start
        ]):
            job_start = DummyOperator(
                task_id="airflow_job_start",
                on_execute_callback=dag_start_slack_alert() if self.alert_on_start else None
            )
            for rt in roots:
                rt.set_upstream(job_start)

            if self.depend_on_dags:
                for depend_on_d in self.depend_on_dags:
                    external_dag_id = depend_on_d['external_dag_id']
                    job_start.set_upstream(
                        depends_on_task(
                            task_id=f"depends_on_{external_dag_id}",
                            **depend_on_d
                        )
                    )

            if self.wait_on_dags:
                for wait_on_d in self.wait_on_dags:
                    external_dag_id = wait_on_d['external_dag_id']
                    job_start.set_upstream(
                        wait_on_task(
                            task_id=f"wait_on_{external_dag_id}",
                            **wait_on_d
                        )
                    )

            if self.depend_on_tasks:
                for depend_on_t in self.depend_on_tasks:
                    if 'upstream_task' in depend_on_t:
                        external_dag_id = depend_on_t['upstream_task'].dag_id
                        external_task_id = depend_on_t['upstream_task'].task_id
                    else:
                        external_dag_id = depend_on_t['external_dag_id']
                        external_task_id = depend_on_t['external_task_id']
                    job_start.set_upstream(
                        depends_on_task(
                            task_id=f"depends_on_{external_dag_id}.{external_task_id}",
                            **depend_on_t
                        )
                    )

            if self.wait_on_tasks:
                for wait_on_t in self.wait_on_tasks:
                    if 'upstream_task' in wait_on_t:
                        external_dag_id = wait_on_t['upstream_task'].dag_id
                        external_task_id = wait_on_t['upstream_task'].task_id
                    else:
                        external_dag_id = wait_on_t['external_dag_id']
                        external_task_id = wait_on_t['external_task_id']
                    job_start.set_upstream(
                        wait_on_task(
                            task_id=f"wait_on_{external_dag_id}.{external_task_id}",
                            **wait_on_t
                        )
                    )

        if self.alert_on_finish:
            job_end = DummyOperator(
                task_id="airflow_job_end",
                on_success_callback=dag_success_slack_alert()
            )
            for lv in leaves:
                lv.set_downstream(job_end)
        super().__exit__(_type, _value, _tb)