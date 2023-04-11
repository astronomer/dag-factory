import os
import datetime
from unittest.mock import patch

import pendulum
import pytest
from airflow import DAG

from packaging import version

try:
    from airflow.providers.http.sensors.http import HttpSensor
except ImportError:
    from airflow.sensors.http_sensor import HttpSensor

try:
    from airflow.sensors.sql_sensor import SqlSensor
except ImportError:
    from airflow.providers.common.sql.sensors.sql import SqlSensor

try:
    from airflow.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash_operator import BashOperator

try:
    from airflow.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

try:
    from airflow.version import version as AIRFLOW_VERSION
except ImportError:
    from airflow import __version__ as AIRFLOW_VERSION

from dagfactory import dagbuilder

if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
    from airflow.timetables.interval import CronDataIntervalTimetable
else:
    Timetable = None
# pylint: disable=ungrouped-imports,invalid-name

if version.parse(AIRFLOW_VERSION) >= version.parse("2.3.0"):
    from airflow.models import MappedOperator
else:
    MappedOperator = None
# pylint: disable=ungrouped-imports,invalid-name

here = os.path.dirname(__file__)

DEFAULT_CONFIG = {
    "default_args": {
        "owner": "default_owner",
        "start_date": datetime.date(2018, 3, 1),
        "end_date": datetime.date(2018, 3, 5),
        "retries": 1,
        "retry_delay_sec": 300,
    },
    "concurrency": 1,
    "max_active_runs": 1,
    "dagrun_timeout_sec": 600,
    "schedule_interval": "0 1 * * *",
}
DAG_CONFIG = {
    "doc_md": "##here is a doc md string",
    "default_args": {"owner": "custom_owner"},
    "description": "this is an example dag",
    "schedule_interval": "0 3 * * *",
    "tags": ["tag1", "tag2"],
    "render_template_as_native_obj": True,
    "tasks": {
        "task_1": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 1",
            "execution_timeout_secs": 5,
        },
        "task_2": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 2",
            "dependencies": ["task_1"],
        },
        "task_3": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 3",
            "dependencies": ["task_1"],
        },
    },
}
DAG_CONFIG_TASK_GROUP = {
    "default_args": {"owner": "custom_owner"},
    "schedule_interval": "0 3 * * *",
    "task_groups": {
        "task_group_1": {
            "tooltip": "this is a task group",
            "dependencies": ["task_1"],
        },
        "task_group_2": {
            "dependencies": ["task_group_1"],
        },
        "task_group_3": {},
    },
    "tasks": {
        "task_1": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 1",
        },
        "task_2": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 2",
            "task_group_name": "task_group_1",
        },
        "task_3": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 3",
            "task_group_name": "task_group_1",
            "dependencies": ["task_2"],
        },
        "task_4": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 4",
            "dependencies": ["task_group_1"],
        },
        "task_5": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 5",
            "task_group_name": "task_group_2",
        },
        "task_6": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 6",
            "task_group_name": "task_group_2",
            "dependencies": ["task_5"],
        },
    },
}
DAG_CONFIG_DYNAMIC_TASK_MAPPING = {
    "default_args": {"owner": "custom_owner"},
    "description": "This is an example dag with dynamic task mapping",
    "schedule_interval": "0 4 * * *",
    "tasks": {
        "request": {
            "operator": "airflow.operators.python_operator.PythonOperator",
            "python_callable_name": "example_task_mapping",
            "python_callable_file": os.path.realpath(__file__)
        },
        "process_1": {
            "operator": "airflow.operators.python_operator.PythonOperator",
            "python_callable_name": "expand_task",
            "python_callable_file": os.path.realpath(__file__),
            "partial": {
                "op_kwargs": {
                    "test_id": "test"
                }
            },
            "expand": {
                "op_args": "request.output"
            }
        },
    },
}


DAG_CONFIG_CALLBACK = {
    "doc_md": "##here is a doc md string",
    "default_args": {
        "owner": "custom_owner",
        "on_failure_callback": f"{__name__}.print_context_callback",
        "on_success_callback": f"{__name__}.print_context_callback",
        "on_execute_callback": f"{__name__}.print_context_callback",
        "on_retry_callback": f"{__name__}.print_context_callback",
    },
    "description": "this is an example dag",
    "schedule_interval": "0 3 * * *",
    "tags": ["tag1", "tag2"],
    "on_failure_callback": f"{__name__}.print_context_callback",
    "on_success_callback": f"{__name__}.print_context_callback",
    "sla_miss_callback": f"{__name__}.print_context_callback",
    "tasks": {
        "task_1": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 1",
            "execution_timeout_secs": 5,
            "on_failure_callback": f"{__name__}.print_context_callback",
            "on_success_callback": f"{__name__}.print_context_callback",
            "on_execute_callback": f"{__name__}.print_context_callback",
            "on_retry_callback": f"{__name__}.print_context_callback",
        },
        "task_2": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 2",
            "dependencies": ["task_1"],
            "on_failure_callback": f"{__name__}.print_context_callback",
            "on_success_callback": f"{__name__}.print_context_callback",
            "on_execute_callback": f"{__name__}.print_context_callback",
            "on_retry_callback": f"{__name__}.print_context_callback",
        },
        "task_3": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 3",
            "dependencies": ["task_1"],
            "on_failure_callback": f"{__name__}.print_context_callback",
            "on_success_callback": f"{__name__}.print_context_callback",
            "on_execute_callback": f"{__name__}.print_context_callback",
            "on_retry_callback": f"{__name__}.print_context_callback",
        },
    },
}
UTC = pendulum.timezone("UTC")


class MockTaskGroup:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def test_get_dag_params():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    expected = {
        "doc_md": "##here is a doc md string",
        "dag_id": "test_dag",
        "default_args": {
            "owner": "custom_owner",
            "start_date": datetime.datetime(2018, 3, 1, 0, 0, tzinfo=UTC),
            "end_date": datetime.datetime(2018, 3, 5, 0, 0, tzinfo=UTC),
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=300),
        },
        "description": "this is an example dag",
        "schedule_interval": "0 3 * * *",
        "concurrency": 1,
        "max_active_runs": 1,
        "dagrun_timeout": datetime.timedelta(seconds=600),
        "render_template_as_native_obj": True,
        "tags": ["tag1", "tag2"],
        "tasks": {
            "task_1": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 1",
                "execution_timeout_secs": 5,
            },
            "task_2": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 2",
                "dependencies": ["task_1"],
            },
            "task_3": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 3",
                "dependencies": ["task_1"],
            },
        },
    }
    actual = td.get_dag_params()
    assert actual == expected


def test_get_dag_params_no_start_date():
    td = dagbuilder.DagBuilder("test_dag", {}, {})
    with pytest.raises(Exception):
        td.get_dag_params()


def test_make_task_valid():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.bash_operator.BashOperator"
    task_params = {
        "task_id": "test_task",
        "bash_command": "echo 1",
        "execution_timeout_secs": 5,
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert actual.bash_command == "echo 1"
    assert isinstance(actual, BashOperator)


def test_make_task_bad_operator():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "not_real"
    task_params = {"task_id": "test_task", "bash_command": "echo 1"}
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def test_make_task_missing_required_param():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.bash_operator.BashOperator"
    task_params = {"task_id": "test_task"}
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def print_test():
    print("test")


def expand_task(x, test_id):
    print(test_id)
    print(x)
    return [x]


def example_task_mapping():
    return [[1], [2], [3]]


def test_make_python_operator():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.python_operator.PythonOperator"
    task_params = {
        "task_id": "test_task",
        "python_callable_name": "print_test",
        "python_callable_file": os.path.realpath(__file__),
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert callable(actual.python_callable)
    assert isinstance(actual, PythonOperator)


def test_make_python_operator_missing_param():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.python_operator.PythonOperator"
    task_params = {"task_id": "test_task", "python_callable_name": "print_test"}
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def test_make_python_operator_missing_params():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.python_operator.PythonOperator"
    task_params = {"task_id": "test_task"}
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def test_make_http_sensor():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.sensors.http_sensor.HttpSensor"
    task_params = {
        "task_id": "test_task",
        "http_conn_id": "test-http",
        "method": "GET",
        "endpoint": "",
        "response_check_name": "print_test",
        "response_check_file": os.path.realpath(__file__),
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert callable(actual.response_check)
    assert isinstance(actual, HttpSensor)


def test_make_http_sensor_lambda():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.sensors.http_sensor.HttpSensor"
    task_params = {
        "task_id": "test_task",
        "http_conn_id": "test-http",
        "method": "GET",
        "endpoint": "",
        "response_check_lambda": 'lambda response: "ok" in response.text',
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert callable(actual.response_check)
    assert isinstance(actual, HttpSensor)


def test_make_sql_sensor_success():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.sensors.sql_sensor.SqlSensor"
    task_params = {
        "task_id": "test_task",
        "conn_id": "test-sql",
        "sql": "SELECT 1 AS status;",
        "success_check_name": "print_test",
        "success_check_file": os.path.realpath(__file__),
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert callable(actual.success)
    assert isinstance(actual, SqlSensor)


def test_make_sql_sensor_success_lambda():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.sensors.sql_sensor.SqlSensor"
    task_params = {
        "task_id": "test_task",
        "conn_id": "test-sql",
        "sql": "SELECT 1 AS status;",
        "success_check_lambda": "lambda res: res > 0",
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert callable(actual.success)
    assert isinstance(actual, SqlSensor)


def test_make_sql_sensor_failure():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.sensors.sql_sensor.SqlSensor"
    task_params = {
        "task_id": "test_task",
        "conn_id": "test-sql",
        "sql": "SELECT 1 AS status;",
        "failure_check_name": "print_test",
        "failure_check_file": os.path.realpath(__file__),
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert not callable(actual.success)
    assert callable(actual.failure)
    assert isinstance(actual, SqlSensor)


def test_make_sql_sensor_failure_lambda():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.sensors.sql_sensor.SqlSensor"
    task_params = {
        "task_id": "test_task",
        "conn_id": "test-sql",
        "sql": "SELECT 1 AS status;",
        "failure_check_lambda": "lambda res: res > 0",
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert not callable(actual.success)
    assert callable(actual.failure)
    assert isinstance(actual, SqlSensor)


def test_make_http_sensor_missing_param():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.sensors.http_sensor.HttpSensor"
    task_params = {
        "task_id": "test_task",
        "http_conn_id": "test-http",
        "method": "GET",
        "endpoint": "",
        "response_check_name": "print_test",
    }
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def test_build():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    actual = td.build()
    assert actual["dag_id"] == "test_dag"
    assert isinstance(actual["dag"], DAG)
    assert len(actual["dag"].tasks) == 3
    assert actual["dag"].task_dict["task_1"].downstream_task_ids == {"task_2", "task_3"}
    if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.8"):
        assert actual["dag"].tags == ["tag1", "tag2"]


def test_get_dag_params():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP, DEFAULT_CONFIG)
    expected = {
        "default_args": {
            "owner": "custom_owner",
            "start_date": datetime.datetime(2018, 3, 1, 0, 0, tzinfo=UTC),
            "end_date": datetime.datetime(2018, 3, 5, 0, 0, tzinfo=UTC),
            "retries": 1,
            "retry_delay": datetime.timedelta(seconds=300),
        },
        "schedule_interval": "0 3 * * *",
        "task_groups": {
            "task_group_1": {
                "tooltip": "this is a task group",
                "dependencies": ["task_1"],
            },
            "task_group_2": {"dependencies": ["task_group_1"]},
            "task_group_3": {},
        },
        "tasks": {
            "task_1": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 1",
            },
            "task_2": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 2",
                "task_group_name": "task_group_1",
            },
            "task_3": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 3",
                "task_group_name": "task_group_1",
                "dependencies": ["task_2"],
            },
            "task_4": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 4",
                "dependencies": ["task_group_1"],
            },
            "task_5": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 5",
                "task_group_name": "task_group_2",
            },
            "task_6": {
                "operator": "airflow.operators.bash_operator.BashOperator",
                "bash_command": "echo 6",
                "task_group_name": "task_group_2",
                "dependencies": ["task_5"],
            },
        },
        "concurrency": 1,
        "max_active_runs": 1,
        "dag_id": "test_dag",
        "dagrun_timeout": datetime.timedelta(seconds=600),
    }
    if version.parse(AIRFLOW_VERSION) < version.parse("2.0.0"):
        error_message = "`task_groups` key can only be used with Airflow 2.x.x"
        with pytest.raises(Exception, match=error_message):
            td.get_dag_params()
    else:
        assert td.get_dag_params() == expected


def test_build_task_groups():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP, DEFAULT_CONFIG)
    if version.parse(AIRFLOW_VERSION) < version.parse("2.0.0"):
        error_message = "`task_groups` key can only be used with Airflow 2.x.x"
        with pytest.raises(Exception, match=error_message):
            td.build()
    else:
        actual = td.build()
        task_group_1 = {
            t for t in actual["dag"].task_dict if t.startswith("task_group_1")
        }
        task_group_2 = {
            t for t in actual["dag"].task_dict if t.startswith("task_group_2")
        }
        assert actual["dag_id"] == "test_dag"
        assert isinstance(actual["dag"], DAG)
        assert len(actual["dag"].tasks) == 6
        assert actual["dag"].task_dict["task_1"].downstream_task_ids == {
            "task_group_1.task_2"
        }
        assert actual["dag"].task_dict["task_group_1.task_2"].downstream_task_ids == {
            "task_group_1.task_3"
        }
        assert actual["dag"].task_dict["task_group_1.task_3"].downstream_task_ids == {
            "task_4",
            "task_group_2.task_5",
        }
        assert actual["dag"].task_dict["task_group_2.task_5"].downstream_task_ids == {
            "task_group_2.task_6",
        }
        assert {"task_group_1.task_2", "task_group_1.task_3"} == task_group_1
        assert {"task_group_2.task_5", "task_group_2.task_6"} == task_group_2


@patch("dagfactory.dagbuilder.TaskGroup", new=MockTaskGroup)
def test_make_task_groups():
    task_group_dict = {
        "task_group": {
            "tooltip": "this is a task group",
        },
    }
    dag = "dag"
    task_groups = dagbuilder.DagBuilder.make_task_groups(task_group_dict, dag)
    expected = MockTaskGroup(
        tooltip="this is a task group", group_id="task_group", dag=dag
    )
    if version.parse(AIRFLOW_VERSION) < version.parse("2.0.0"):
        assert task_groups == {}
    else:
        assert task_groups["task_group"].__dict__ == expected.__dict__


def test_make_task_groups_empty():
    task_groups = dagbuilder.DagBuilder.make_task_groups({}, None)
    assert task_groups == {}


def print_context_callback(context, **kwargs):
    print(context)


def test_make_task_with_callback():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.python_operator.PythonOperator"
    task_params = {
        "task_id": "test_task",
        "python_callable_name": "print_test",
        "python_callable_file": os.path.realpath(__file__),
        "on_failure_callback": f"{__name__}.print_context_callback",
        "on_success_callback": f"{__name__}.print_context_callback",
        "on_execute_callback": f"{__name__}.print_context_callback",
        "on_retry_callback": f"{__name__}.print_context_callback",
    }
    actual = td.make_task(operator, task_params)
    assert actual.task_id == "test_task"
    assert callable(actual.python_callable)
    assert isinstance(actual, PythonOperator)
    assert callable(actual.on_failure_callback)
    assert callable(actual.on_success_callback)
    if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
        assert callable(actual.on_execute_callback)
    assert callable(actual.on_retry_callback)


def test_make_timetable():
    if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
        td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
        timetable = "airflow.timetables.interval.CronDataIntervalTimetable"
        timetable_params = {
            "cron": "0 8,16 * * 1-5",
            "timezone": "UTC"
        }
        actual = td.make_timetable(timetable, timetable_params)
        assert actual.periodic
        assert actual.can_run


def test_make_dag_with_callback():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_CALLBACK, DEFAULT_CONFIG)
    td.build()


def test_get_dag_params_with_template_searchpath():
    from dagfactory import utils
    td = dagbuilder.DagBuilder("test_dag", {"template_searchpath": ["./sql"]}, DEFAULT_CONFIG)
    error_message = "template_searchpath must be absolute paths"
    with pytest.raises(Exception, match=error_message):
        td.get_dag_params()

    td = dagbuilder.DagBuilder("test_dag", {"template_searchpath": ["/sql"]}, DEFAULT_CONFIG)
    error_message = "template_searchpath must be existing paths"
    with pytest.raises(Exception, match=error_message):
        td.get_dag_params()
        
    td = dagbuilder.DagBuilder("test_dag", {"template_searchpath": "./sql"}, DEFAULT_CONFIG)
    error_message = "template_searchpath must be absolute paths"
    with pytest.raises(Exception, match=error_message):
        td.get_dag_params()

    td = dagbuilder.DagBuilder("test_dag", {"template_searchpath": "/sql"}, DEFAULT_CONFIG)
    error_message = "template_searchpath must be existing paths"
    with pytest.raises(Exception, match=error_message):
        td.get_dag_params()

    assert utils.check_template_searchpath(123) == False
    assert utils.check_template_searchpath("/home/runner/work") == True
    assert utils.check_template_searchpath(["/home/runner/work"]) == True


def test_get_dag_params_with_render_template_as_native_obj():
    td = dagbuilder.DagBuilder("test_dag", {"render_template_as_native_obj": "true"}, DEFAULT_CONFIG)
    error_message = "render_template_as_native_obj should be bool type!"
    with pytest.raises(Exception, match=error_message):
        td.get_dag_params()

    false = lambda x: print(x)
    td = dagbuilder.DagBuilder("test_dag", {"render_template_as_native_obj": false}, DEFAULT_CONFIG)
    error_message = "render_template_as_native_obj should be bool type!"
    with pytest.raises(Exception, match=error_message):
        td.get_dag_params()


def test_make_task_with_duplicated_partial_kwargs():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_DYNAMIC_TASK_MAPPING, DEFAULT_CONFIG)
    operator = "airflow.operators.bash_operator.BashOperator"
    task_params = {"task_id": "task_bash",
                   "bash_command": "echo 2",
                   "partial": {"bash_command": "echo 4"}
                   }
    with pytest.raises(Exception):
        td.make_task(operator, task_params)


def test_dynamic_task_mapping():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_DYNAMIC_TASK_MAPPING, DEFAULT_CONFIG)
    if version.parse(AIRFLOW_VERSION) < version.parse("2.3.0"):
        error_message = "Dynamic task mapping available only in Airflow >= 2.3.0"
        with pytest.raises(Exception, match=error_message):
            td.build()
    else:
        operator = "airflow.operators.python_operator.PythonOperator"
        task_params = {
            "task_id": "process",
            "python_callable_name": "expand_task",
            "python_callable_file": os.path.realpath(__file__),
            "partial": {
                "op_kwargs": {
                    "test_id": "test"
                }
            },
            "expand": {
                "op_args": "request.output"
            }
        }
        actual = td.make_task(operator, task_params)
        assert isinstance(actual, MappedOperator)


