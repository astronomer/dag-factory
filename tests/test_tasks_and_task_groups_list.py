import datetime

import pytest

try:
    from airflow.sdk.definitions.dag import DAG
except ImportError:  # pragma: no cover
    from airflow.models import DAG

from dagfactory.dagbuilder import DagBuilder, DagFactoryConfigException
from tests.utils import get_bash_operator_path, get_schedule_key

DEFAULT_CONFIG_MINIMAL = {
    "default_args": {
        "start_date": datetime.date(2024, 1, 1),
    }
}


# ---------------------------------------------------------------------------
# Tasks defined as a LIST ------------------------------------------------
# ---------------------------------------------------------------------------


def _make_tasks_list_config():
    """Return a DAG config where ``tasks`` is a list instead of a mapping."""
    return {
        "default_args": {
            "owner": "custom_owner",
            "start_date": datetime.date(2024, 1, 1),
        },
        get_schedule_key(): "0 3 * * *",
        "tasks": [
            {
                "task_id": "task_1",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 1",
            },
            {
                "task_id": "task_2",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 2",
                "dependencies": ["task_1"],
            },
            {
                "task_id": "task_3",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 3",
                "dependencies": ["task_1"],
            },
        ],
    }


def test_build_with_tasks_list():
    """DAG builds and dependencies are wired correctly when tasks is a list."""
    dag_config = _make_tasks_list_config()
    td = DagBuilder("dag_tasks_list", dag_config, DEFAULT_CONFIG_MINIMAL)
    result = td.build()

    assert result["dag_id"] == "dag_tasks_list"
    dag: DAG = result["dag"]
    assert isinstance(dag, DAG)
    assert len(dag.tasks) == 3
    assert dag.task_dict["task_1"].downstream_task_ids == {"task_2", "task_3"}


def test_build_with_duplicate_task_ids_raises_exception():
    dag_config = _make_tasks_list_config()
    # Introduce a duplicate entry for task_1
    dag_config["tasks"].append(
        {
            "task_id": "task_1",
            "operator": get_bash_operator_path(),
            "bash_command": "echo again",
        }
    )

    td = DagBuilder("dag_tasks_list_dup", dag_config, DEFAULT_CONFIG_MINIMAL)
    with pytest.raises(DagFactoryConfigException, match="Duplicate task_id"):
        td.build()


# ---------------------------------------------------------------------------
# task_groups defined as a LIST ----------------------------------------
# ---------------------------------------------------------------------------


def _make_task_groups_list_config():
    """Return a DAG config where both tasks and task_groups are lists."""
    return {
        "default_args": {
            "owner": "custom_owner",
            "start_date": datetime.date(2024, 1, 1),
        },
        get_schedule_key(): "0 3 * * *",
        "task_groups": [
            {
                "group_name": "group_1",
                "tooltip": "this is a task group",
                "dependencies": ["task_1"],
            },
            {
                "group_name": "group_2",
                "dependencies": ["group_1"],
            },
            {
                "group_name": "group_3",
            },
        ],
        "tasks": [
            {
                "task_id": "task_1",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 1",
            },
            {
                "task_id": "task_2",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 2",
                "task_group_name": "group_1",
            },
            {
                "task_id": "task_3",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 3",
                "task_group_name": "group_1",
                "dependencies": ["task_2"],
            },
            {
                "task_id": "task_4",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 4",
                "dependencies": ["group_1"],
            },
            {
                "task_id": "task_5",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 5",
                "task_group_name": "group_2",
            },
            {
                "task_id": "task_6",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 6",
                "task_group_name": "group_2",
                "dependencies": ["task_5"],
            },
        ],
    }


def test_build_with_task_groups_list():
    dag_config = _make_task_groups_list_config()
    td = DagBuilder("dag_task_groups_list", dag_config, DEFAULT_CONFIG_MINIMAL)
    result = td.build()

    dag: DAG = result["dag"]
    assert len(dag.tasks) == 6

    # Verify dependencies were wired correctly, including group prefixes.
    assert dag.task_dict["task_1"].downstream_task_ids == {"group_1.task_2"}
    assert dag.task_dict["group_1.task_2"].downstream_task_ids == {"group_1.task_3"}
    assert dag.task_dict["group_1.task_3"].downstream_task_ids == {
        "task_4",
        "group_2.task_5",
    }


def test_build_with_duplicate_group_name_raises_exception():
    dag_config = _make_task_groups_list_config()
    dag_config["task_groups"].append(
        {
            "group_name": "group_1",  # duplicate
        }
    )

    td = DagBuilder("dag_task_groups_list_dup", dag_config, DEFAULT_CONFIG_MINIMAL)
    with pytest.raises(DagFactoryConfigException, match="Duplicate group_name"):
        td.build()
