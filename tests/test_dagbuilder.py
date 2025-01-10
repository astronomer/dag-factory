import datetime
import functools
import os
from pathlib import Path
from unittest.mock import mock_open, patch

import pendulum
import pytest
from airflow import DAG
from packaging import version

from dagfactory.dagbuilder import DagBuilder, DagFactoryConfigException, Dataset
from tests.utils import one_hour_ago

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

if version.parse(AIRFLOW_VERSION) >= version.parse("2.3.0"):
    from airflow.models import MappedOperator
else:
    MappedOperator = None
# pylint: disable=ungrouped-imports,invalid-name

here = Path(__file__).parent

PROJECT_ROOT_PATH = str(here.parent)
UTC = pendulum.timezone("UTC")

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
    "dag_display_name": "Pretty example dag",
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
            "python_callable_file": os.path.realpath(__file__),
        },
        "process_1": {
            "operator": "airflow.operators.python_operator.PythonOperator",
            "python_callable_name": "expand_task",
            "python_callable_file": os.path.realpath(__file__),
            "partial": {"op_kwargs": {"test_id": "test"}},
            "expand": {"op_args": {"request_output": "request.output"}},
        },
    },
}

DAG_CONFIG_CALLBACKS = {
    "doc_md": "##here is a doc md string",
    "default_args": {
        "owner": "custom_owner",
        "on_execute_callback": f"{__name__}.print_context_callback",
        "on_success_callback": f"{__name__}.print_context_callback",
        # "on_failure_callback": f"{__name__}.print_context_callback",  # Passing this in at the Task-level
        "on_retry_callback": f"{__name__}.print_context_callback",
        "on_skipped_callback": f"{__name__}.print_context_callback",
    },
    "description": "this is an example dag",
    "schedule_interval": "0 3 * * *",
    "tags": ["tag1", "tag2"],
    # This includes each of the four options (str function, str function with params, file and name, provider)
    "on_execute_callback": f"{__name__}.print_context_callback",
    "on_success_callback": {
        "callback": f"{__name__}.empty_callback_with_params",
        "param_1": "value_1",
        "param_2": "value_2",
    },
    "on_failure_callback_name": "print_context_callback",
    "on_failure_callback_file": __file__,
    "tasks": {
        "task_1": {  # Make sure that default_args are applied to this Task
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 1",
            "execution_timeout_secs": 5,
            "on_failure_callback_name": "print_context_callback",
            "on_failure_callback_file": __file__,
        }
    },
}

DAG_CONFIG_TASK_GROUP_WITH_CALLBACKS = {
    "default_args": {
        "owner": "custom_owner",
        "on_failure_callback": {  # Include this to assert that these are overridden by TaskGroup callbacks
            "callback": f"{__name__}.empty_callback_with_params",
            "param_1": "value_1",
            "param_2": "value_2",
        },
    },
    "schedule_interval": "0 3 * * *",
    "task_groups": {
        "task_group_1": {
            "tooltip": "this is a task group",
            "default_args": {
                "on_execute_callback": f"{__name__}.print_context_callback",
                "on_success_callback": f"{__name__}.print_context_callback",
                "on_failure_callback": f"{__name__}.print_context_callback",
                "on_retry_callback": f"{__name__}.print_context_callback",
                "on_skip_callback": f"{__name__}.print_context_callback",  # Throwing this in for good measure
            },
        },
    },
    "tasks": {
        "task_1": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 1",
            "task_group_name": "task_group_1",
        },
        "task_2": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 2",
            "task_group_name": "task_group_1",
            "on_failure_callback": {
                "callback": f"{__name__}.empty_callback_with_params",
                "param_1": "value_1",
                "param_2": "value_2",
            },
        },
        "task_3": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 3",
            "task_group_name": "task_group_1",
            "dependencies": ["task_2"],
        },
        # This is not part of the TaskGroup, we'll add additional callbacks here. These are going to include:
        # - String with no parameters
        # - String with parameters
        # - File name and path
        "task_4": {
            "operator": "airflow.operators.bash_operator.BashOperator",
            "bash_command": "echo 4",
            "dependencies": ["task_group_1"],
            "on_execute_callback": f"{__name__}.print_context_callback",
            "on_success_callback": {
                "callback": f"{__name__}.empty_callback_with_params",
                "param_1": "value_1",
                "param_2": "value_2",
            },
            "on_failure_callback_name": "print_context_callback",
            "on_failure_callback_file": __file__,
        },
    },
}


class MockTaskGroup:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockPythonOperator(MockTaskGroup):
    """
    Mock PythonOperator
    """


def test_get_dag_params():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    expected = {
        "doc_md": "##here is a doc md string",
        "dag_display_name": "Pretty example dag",
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


def test_adjust_general_task_params_external_sensor_arguments():
    task_params = {"execution_date_fn": "tests.utils.one_hour_ago"}
    DagBuilder.adjust_general_task_params(task_params)
    assert task_params["execution_date_fn"] == one_hour_ago

    task_params = {"execution_delta": "1 days"}
    DagBuilder.adjust_general_task_params(task_params)
    assert task_params["execution_delta"] == datetime.timedelta(days=1)


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


def test_make_python_operator_with_callable_str():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    operator = "airflow.operators.python_operator.PythonOperator"
    task_params = {
        "task_id": "test_task",
        "python_callable": "builtins.print",
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
    if version.parse(AIRFLOW_VERSION) >= version.parse("2.9.0"):
        assert actual["dag"].dag_display_name == "Pretty example dag"
    if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.8"):
        assert actual["dag"].tags == ["tag1", "tag2", "dagfactory"]


def test_get_dag_params_dag_with_task_group():
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

    assert td.get_dag_params() == expected


def test_build_task_groups():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP, DEFAULT_CONFIG)

    actual = td.build()
    task_group_1 = {t for t in actual["dag"].task_dict if t.startswith("task_group_1")}
    task_group_2 = {t for t in actual["dag"].task_dict if t.startswith("task_group_2")}
    assert actual["dag_id"] == "test_dag"
    assert isinstance(actual["dag"], DAG)
    assert len(actual["dag"].tasks) == 6
    assert actual["dag"].task_dict["task_1"].downstream_task_ids == {"task_group_1.task_2"}
    assert actual["dag"].task_dict["task_group_1.task_2"].downstream_task_ids == {"task_group_1.task_3"}
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
    expected = MockTaskGroup(tooltip="this is a task group", group_id="task_group", dag=dag)

    assert task_groups["task_group"].__dict__ == expected.__dict__


def test_make_task_groups_empty():
    task_groups = dagbuilder.DagBuilder.make_task_groups({}, None)
    assert task_groups == {}


def print_context_callback(context, **kwargs):
    print(context)


def empty_callback_with_params(context, param_1, param_2, **kwargs):
    # Context is the first parameter passed into the callback
    print(param_1)
    print(param_2)


# Test the set_callback() static method
@pytest.mark.callbacks
def test_set_callback_exceptions():
    """
    test_set_callback_exceptions

    Validate that exceptions are being throw for an incompatible version of Airflow, as well as for an invalid type
    passed to the parameter config.
    """
    # Test a versioning exception
    if version.parse(AIRFLOW_VERSION) < version.parse("2.0.0"):
        error_message = "Cannot parse callbacks with an Airflow version less than 2.0.0"
        with pytest.raises(DagFactoryConfigException, match=error_message):
            DagBuilder.set_callback(
                parameters={"dummy_key": "dummy_value"},
                callback_type="on_execute_callback",
            )

    # Now, test an exception parsing the parameters dictionary
    invalid_type_passed_message = "Invalid type passed to on_execute_callback"
    with pytest.raises(DagFactoryConfigException, match=invalid_type_passed_message):
        DagBuilder.set_callback(
            parameters={"on_execute_callback": ["callback_1", "callback_2", "callback_3"]},
            callback_type="on_execute_callback",
        )


@pytest.mark.callbacks
def test_make_dag_with_callbacks():
    """
    test_make_dag_with_callbacks

    Validate that the DAG builds. Then, check callbacks configured at the DAG-level.
    """
    # Build the DAG if the Airflow version check is met
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_CALLBACKS, DEFAULT_CONFIG)
    dag = td.build()["dag"]  # Pull the actual DAG object, which can be used in

    # Validate the .set_callback() method works as expected when importing a string (for on_execute_callback)
    assert "on_execute_callback" in td.dag_config
    assert callable(td.dag_config["on_execute_callback"])
    assert td.dag_config["on_execute_callback"].__name__ == "print_context_callback"

    # Check on_success_callback, which is a function that is configured with two key-value pairs
    assert "on_success_callback" in td.dag_config
    assert isinstance(dag.on_success_callback, functools.partial)
    assert callable(dag.on_success_callback)
    assert dag.on_success_callback.func.__name__ == "empty_callback_with_params"
    assert "param_1" in dag.on_success_callback.keywords  # Check the parameters
    assert dag.on_success_callback.keywords["param_1"] == "value_1"
    assert "param_2" in dag.on_success_callback.keywords
    assert dag.on_success_callback.keywords["param_2"] == "value_2"

    # Verify that the callbacks have been set up properly per DAG after specifying:
    # - 'on_failure_callback_file' & 'on_failure_callback_name' for 'on_failure_callback'
    assert "on_failure_callback" in td.dag_config
    assert callable(dag.on_failure_callback)
    assert dag.on_failure_callback.__name__ == "print_context_callback"

    if version.parse(AIRFLOW_VERSION) >= version.parse("2.6.0"):
        from airflow.providers.slack.notifications.slack import send_slack_notification

        dag_config_callbacks__with_provider = dict(DAG_CONFIG_CALLBACKS)
        dag_config_callbacks__with_provider["sla_miss_callback"] = {
            "callback": "airflow.providers.slack.notifications.slack.send_slack_notification",
            "slack_conn_id": "slack_conn_id",
            "text": f"""Sample callback text.""",
            "channel": "#channel",
            "username": "username",
        }

        with_provider_td = dagbuilder.DagBuilder("test_dag", dag_config_callbacks__with_provider, DEFAULT_CONFIG)
        with_provider_td.build()

        # Assert that sla_miss_callback is part of the dag_config. If it is, pull the callback and validate the config
        # of the Slack notifier
        assert "sla_miss_callback" in with_provider_td.dag_config
        sla_miss_callback = with_provider_td.dag_config["sla_miss_callback"]

        assert isinstance(sla_miss_callback, send_slack_notification)
        assert callable(sla_miss_callback)
        assert sla_miss_callback.slack_conn_id == "slack_conn_id"
        assert sla_miss_callback.channel == "#channel"
        assert sla_miss_callback.username == "username"


def test_make_timetable():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    timetable = "airflow.timetables.interval.CronDataIntervalTimetable"
    timetable_params = {"cron": "0 8,16 * * 1-5", "timezone": "UTC"}
    actual = td.make_timetable(timetable, timetable_params)
    assert actual.periodic
    try:
        assert actual.can_run
    except AttributeError:
        # can_run attribute was removed and replaced with can_be_scheduled in later versions of Airflow.
        assert actual.can_be_scheduled


@pytest.mark.callbacks
def test_make_dag_with_callbacks_default_args():
    """
    test_make_dag_with_callbacks_default_args

    Check callbacks configured in default args.
    """
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_CALLBACKS, DEFAULT_CONFIG)
    dag = td.build()["dag"]
    default_args = td.dag_config["default_args"]  # Pull the default_args

    # Validate at the Task-level
    assert "task_1" in dag.task_dict
    task_1 = dag.task_dict["task_1"]

    # Validate the .set_callback() method works as expected when importing a string
    for callback_type in (
        "on_execute_callback",
        "on_success_callback",
        # "on_failure_callback",  # Set at the Task-level, tested below
        "on_retry_callback",
        "on_skipped_callback",
    ):
        # on_skipped_callback could only be added to default_args starting in Airflow version 2.7.0
        # TODO: Address this, this should be 2.7.0
        if not (version.parse(AIRFLOW_VERSION) < version.parse("2.9.0") and callback_type == "on_skipped_callback"):
            assert callback_type in default_args
            assert callable(default_args.get(callback_type))
            assert default_args.get(callback_type).__name__ == "print_context_callback"

            # Assert that these callbacks have been applied at the Task-level
            assert callback_type in task_1.__dict__
            assert callable(task_1.__dict__[callback_type])
            assert task_1.__dict__[callback_type].__name__ == "print_context_callback"

        # Assert that these callbacks have been applied at the Task-level
        assert "on_failure_callback" in task_1.__dict__
        assert callable(task_1.__dict__["on_failure_callback"])
        assert task_1.__dict__["on_failure_callback"].__name__ == "print_context_callback"


@pytest.mark.callbacks
def test_make_dag_with_task_group_callbacks():
    """
    test_dag_with_task_group_callbacks

    Test the DAG with callbacks configured at both the Task and the TaskGroup level. Note that callbacks configured in
    the default_args of a TaskGroup are applied to each of those Tasks. To do this, we'll use the config set in the
    DAG_CONFIG_TASK_GROUP_WITH_CALLBACKS variable. We'll want to test three things:

    1) The DAG is successfully built
    2) There appropriate number of Tasks that make up this DAG
    3) There is a TaskGroup configured as part of the DAG, which has Tasks assigned to that group
    """

    # Import the DAG using the callback config that was build above
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP_WITH_CALLBACKS, DEFAULT_CONFIG)

    # This will be done only once; validate the exception that is raised if trying to use an invalid version of Airflow
    # when building TaskGroups
    if version.parse(AIRFLOW_VERSION) < version.parse("2.2.0"):
        error_message = "`task_groups` key can only be used with Airflow 2.x.x"
        with pytest.raises(Exception, match=error_message):
            td.build()
    else:
        dag = td.build()["dag"]  # Also, pull the dag

        # Basic checks to ensure the DAG was built as expected
        assert dag.task_count == 4
        assert len([task for task in dag.task_dict.keys() if task.startswith("task_group_1")]) == 3
        assert (
            "task_group_1.task_1" in dag.task_dict
            and "task_group_1.task_2" in dag.task_dict
            and "task_group_1.task_3" in dag.task_dict
        )


@pytest.mark.callbacks
def test_make_dag_with_task_group_callbacks_default_args():
    """
    test_dag_with_task_group_callbacks_default_args

    Once the "build-ability" of the DAG configured in DAG_CONFIG_TASK_GROUP_WITH_CALLBACKS has been tested, we'll test
    the callbacks configured for this DAG. The following assertions will be made:
        - There are callbacks present in the default_args of the TaskGroup
        - These callbacks are "callable", and have the name print_context_callback
        - task_group_1.task_2 has overridden the on_failure_callback
        - task_2 uses the empty_callback_with_params function, which takes two arguments
    """
    # Import the DAG using the callback config that was build above. Previously, we matched the error message thrown
    # if the version was not met. Here, we'll pass testing
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP_WITH_CALLBACKS, DEFAULT_CONFIG)

    # TODO: This should be 2.2.0
    if version.parse(AIRFLOW_VERSION) >= version.parse("2.3.0"):  # This is a work-around for now
        dag = td.build()["dag"]  # Also, pull the dag

        # Now, loop through each of the callback types and validate
        assert "task_group_1" in td.dag_config["task_groups"]
        task_group_default_args = td.dag_config["task_groups"]["task_group_1"]["default_args"]

        # Test that the on_execute_callback configured in the default_args of the TaskGroup are passed down to the Tasks
        # grouped into task_group_1
        assert "on_execute_callback" in task_group_default_args and "on_failure_callback" in task_group_default_args
        assert callable(dag.task_dict["task_group_1.task_1"].on_execute_callback)
        assert dag.task_dict["task_group_1.task_1"].on_execute_callback.__name__ == "print_context_callback"

        # task_2 overrides the on_failure_callback configured in the default_args of task_group_1. Below, this is
        # validated but checking the type, "callab-ility", name, and parameters configured with it
        assert isinstance(dag.task_dict["task_group_1.task_2"].on_failure_callback, functools.partial)
        assert callable(dag.task_dict["task_group_1.task_2"].on_failure_callback)
        assert dag.task_dict["task_group_1.task_2"].on_failure_callback.func.__name__ == "empty_callback_with_params"
        assert "param_1" in dag.task_dict["task_group_1.task_2"].on_failure_callback.keywords
        assert dag.task_dict["task_group_1.task_2"].on_failure_callback.keywords.get("param_1") == "value_1"


@pytest.mark.callbacks
def test_make_dag_with_task_group_callbacks_tasks():
    """
    test_dag_with_task_group_callbacks_tasks

    Here, we're testing callbacks applied at the Task-level.
    """
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_TASK_GROUP_WITH_CALLBACKS, DEFAULT_CONFIG)
    dag = td.build()["dag"]

    task_4 = dag.task_dict["task_4"]
    assert callable(task_4.on_execute_callback)
    assert task_4.on_execute_callback.__name__ == "print_context_callback"

    assert isinstance(task_4.on_success_callback, functools.partial)
    assert callable(task_4.on_success_callback)
    assert task_4.on_success_callback.func.__name__ == "empty_callback_with_params"
    assert "param_2" in task_4.on_success_callback.keywords
    assert task_4.on_success_callback.keywords["param_2"] == "value_2"

    assert callable(task_4.on_failure_callback)
    assert task_4.on_failure_callback.__name__ == "print_context_callback"

    # Can do something similar here as was done with the make_dag test (for Slack)


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
    assert utils.check_template_searchpath(PROJECT_ROOT_PATH) == True
    assert utils.check_template_searchpath([PROJECT_ROOT_PATH]) == True


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
    task_params = {
        "task_id": "task_bash",
        "bash_command": "echo 2",
        "partial": {"bash_command": "echo 4"},
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
            "partial": {"op_kwargs": {"test_id": "test"}},
            "expand": {"op_args": {"request_output": "request.output"}},
        }
        actual = td.make_task(operator, task_params)
        assert isinstance(actual, MappedOperator)


def test_replace_expand_string_with_xcom():
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG_DYNAMIC_TASK_MAPPING, DEFAULT_CONFIG)
    if version.parse(AIRFLOW_VERSION) < version.parse("2.3.0"):
        with pytest.raises(Exception):
            td.build()
    else:
        from airflow.models.xcom_arg import XComArg

        task_conf_output = {"expand": {"key_1": "task_1.output"}}
        task_conf_xcomarg = {"expand": {"key_1": "XcomArg(task_1)"}}

        task1 = PythonOperator(
            task_id="task1",
            python_callable=lambda: print("hello"),
        )

        tasks_dict = {"task_1": task1}
        updated_task_conf_output = dagbuilder.DagBuilder.replace_expand_values(task_conf_output, tasks_dict)
        updated_task_conf_xcomarg = dagbuilder.DagBuilder.replace_expand_values(task_conf_xcomarg, tasks_dict)
        assert updated_task_conf_output["expand"]["key_1"] == XComArg(tasks_dict["task_1"])
        assert updated_task_conf_xcomarg["expand"]["key_1"] == XComArg(tasks_dict["task_1"])


@pytest.mark.skipif(
    version.parse(AIRFLOW_VERSION) <= version.parse("2.4.0"), reason="Requires Airflow version greater than 2.4.0"
)
@pytest.mark.parametrize(
    "outlets,output",
    [
        (
            {"datasets": "s3://test/test.txt", "file": "file://path/to/my_file.txt"},
            ["s3://test/test.txt", "file://path/to/my_file.txt"],
        ),
        (["s3://test/test.txt"], ["s3://test/test.txt"]),
    ],
)
@patch("dagfactory.dagbuilder.utils.get_datasets_uri_yaml_file", new_callable=mock_open)
def test_make_task_outlets(mock_read_file, outlets, output):
    td = dagbuilder.DagBuilder("test_dag", DAG_CONFIG, DEFAULT_CONFIG)
    task_params = {
        "task_id": "process",
        "python_callable_name": "expand_task",
        "python_callable_file": os.path.realpath(__file__),
        "outlets": outlets,
    }
    mock_read_file.return_value = output
    operator = "airflow.operators.python_operator.PythonOperator"
    actual = td.make_task(operator, task_params)
    assert actual.outlets == [Dataset(uri) for uri in output]


@patch("dagfactory.dagbuilder.TaskGroup", new=MockTaskGroup)
def test_make_nested_task_groups():
    task_group_dict = {
        "task_group": {
            "tooltip": "this is a task group",
        },
        "sub_task_group": {"tooltip": "this is a sub task group", "parent_group_name": "task_group"},
    }
    dag = "dag"
    task_groups = dagbuilder.DagBuilder.make_task_groups(task_group_dict, dag)
    expected = {
        "task_group": MockTaskGroup(tooltip="this is a task group", group_id="task_group", dag=dag),
        "sub_task_group": MockTaskGroup(tooltip="this is a sub task group", group_id="sub_task_group", dag=dag),
    }

    sub_task_group = task_groups["sub_task_group"].__dict__
    assert sub_task_group["parent_group"]
    del sub_task_group["parent_group"]
    assert task_groups["task_group"].__dict__ == expected["task_group"].__dict__
    assert sub_task_group == expected["sub_task_group"].__dict__


class TestTopologicalSortTasks:

    def test_basic_topological_sort(self):
        tasks_configs = {
            "task1": {"dependencies": []},
            "task2": {"dependencies": ["task1"]},
            "task3": {"dependencies": ["task2"]},
        }
        result = dagbuilder.DagBuilder.topological_sort_tasks(tasks_configs)
        expected = [
            ("task1", {"dependencies": []}),
            ("task2", {"dependencies": ["task1"]}),
            ("task3", {"dependencies": ["task2"]}),
        ]
        assert result == expected

    def test_no_dependencies(self):
        tasks_configs = {
            "task1": {"dependencies": []},
            "task2": {"dependencies": []},
            "task3": {"dependencies": []},
        }
        result = dagbuilder.DagBuilder.topological_sort_tasks(tasks_configs)
        # Order doesn't matter as there are no dependencies
        expected = [
            ("task1", {"dependencies": []}),
            ("task2", {"dependencies": []}),
            ("task3", {"dependencies": []}),
        ]
        assert result == expected

    def test_empty_input(self):
        tasks_configs = {}
        result = dagbuilder.DagBuilder.topological_sort_tasks(tasks_configs)
        assert result == []

    def test_cyclic_dependencies(self):
        tasks_configs = {
            "task1": {"dependencies": ["task3"]},
            "task2": {"dependencies": ["task1"]},
            "task3": {"dependencies": ["task2"]},
        }
        with pytest.raises(ValueError) as exc_info:
            dagbuilder.DagBuilder.topological_sort_tasks(tasks_configs)
        assert "Cycle detected" in str(exc_info.value)

    def test_multiple_dependencies(self):
        tasks_configs = {
            "task1": {"dependencies": []},
            "task2": {"dependencies": ["task1"]},
            "task3": {"dependencies": ["task1"]},
            "task4": {"dependencies": ["task2", "task3"]},
        }
        result = dagbuilder.DagBuilder.topological_sort_tasks(tasks_configs)
        # Verify ordering with dependencies
        task_names = [task[0] for task in result]
        assert task_names.index("task1") < task_names.index("task2")
        assert task_names.index("task1") < task_names.index("task3")
        assert task_names.index("task2") < task_names.index("task4")
        assert task_names.index("task3") < task_names.index("task4")
