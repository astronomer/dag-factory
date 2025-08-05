import datetime
import logging
import os
import shutil
import tempfile
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from airflow.version import version as AIRFLOW_VERSION

try:
    from airflow.sdk.definitions.variable import Variable  # noqa: F401
except ImportError:
    from airflow.models.variable import Variable  # noqa: F401

from packaging import version
from pendulum.datetime import DateTime, Timezone

from tests.utils import get_bash_operator_path

here = os.path.dirname(__file__)

from dagfactory import DagFactory, dagfactory, exceptions, load_yaml_dags


TEST_DAG_FACTORY = os.path.join(here, "fixtures/dag_factory.yml")
DAG_FACTORY_NO_OR_NONE_STRING_SCHEDULE = os.path.join(here, "fixtures/dag_factory_no_or_none_string_schedule.yml")
DAG_FACTORY_SCHEDULE_INTERVAL = os.path.join(here, "fixtures/dag_factory_schedule_interval.yml")
INVALID_YAML = os.path.join(here, "fixtures/invalid_yaml.yml")
INVALID_DAG_FACTORY = os.path.join(here, "fixtures_without_default_yaml/invalid_dag_factory.yml")
DEFAULT_ARGS_CONFIG_ROOT = os.path.join(here, "fixtures/")
DAG_FACTORY_KUBERNETES_POD_OPERATOR = os.path.join(here, "fixtures/dag_factory_kubernetes_pod_operator.yml")
DAG_FACTORY_KUBERNETES_POD_OPERATOR_LT_2_7 = os.path.join(
    here, "fixtures/dag_factory_kubernetes_pod_operator_lt_2_7.yml"
)
DAG_FACTORY_VARIABLES_AS_ARGUMENTS = os.path.join(here, "fixtures/dag_factory_variables_as_arguments.yml")

DOC_MD_FIXTURE_FILE = os.path.join(here, "fixtures/mydocfile.md")
DOC_MD_PYTHON_CALLABLE_FILE = os.path.join(here, "fixtures/doc_md_builder.py")

DAG_FACTORY_CONFIG = {
    "default": {
        "default_args": {
            "owner": "airflow",
            "start_date": "2020-01-01",
            "end_date": "2020-01-01",
        },
        "default_view": "graph",
        "schedule": "@daily",
    },
    "example_dag": {
        "tasks": [
            {
                "task_id": "task_1",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 1",
            },
        ],
    },
}

DAG_FACTORY_CALLBACK_CONFIG = {
    "example_dag": {
        "doc_md": "##here is a doc md string",
        "default_args": {
            "owner": "custom_owner",
            "start_date": "2020-01-01",
            "on_failure_callback": f"{__name__}.print_context_callback",
            "on_success_callback": f"{__name__}.print_context_callback",
            "on_execute_callback": f"{__name__}.print_context_callback",
            "on_retry_callback": f"{__name__}.print_context_callback",
        },
        "description": "this is an example dag",
        "schedule": "0 3 * * *",
        "tags": ["tag1", "tag2"],
        "on_failure_callback": f"{__name__}.print_context_callback",
        "on_success_callback": f"{__name__}.print_context_callback",
        "sla_miss_callback": f"{__name__}.print_context_callback",
        "tasks": [
            {
                "task_id": "task_1",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 1",
                "execution_timeout": timedelta(seconds=5),
                "on_failure_callback": f"{__name__}.print_context_callback",
                "on_success_callback": f"{__name__}.print_context_callback",
                "on_execute_callback": f"{__name__}.print_context_callback",
                "on_retry_callback": f"{__name__}.print_context_callback",
            },
            {
                "task_id": "task_2",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 2",
                "dependencies": ["task_1"],
                "on_failure_callback": f"{__name__}.print_context_callback",
                "on_success_callback": f"{__name__}.print_context_callback",
                "on_execute_callback": f"{__name__}.print_context_callback",
                "on_retry_callback": f"{__name__}.print_context_callback",
            },
            {
                "task_id": "task_3",
                "operator": get_bash_operator_path(),
                "bash_command": "echo 3",
                "dependencies": ["task_1"],
                "on_failure_callback": f"{__name__}.print_context_callback",
                "on_success_callback": f"{__name__}.print_context_callback",
                "on_execute_callback": f"{__name__}.print_context_callback",
                "on_retry_callback": f"{__name__}.print_context_callback",
            },
        ],
    }
}


def test_validate_config_filepath_valid():
    dagfactory.DagFactory._validate_config_filepath(TEST_DAG_FACTORY)


def test_validate_config_filepath_invalid():
    with pytest.raises(Exception):
        dagfactory.DagFactory._validate_config_filepath("config.yml")


def test_load_dag_config_valid(monkeypatch):
    expected = {
        "default": {
            "default_args": {
                "owner": "default_owner",
                "start_date": datetime.date(2018, 3, 1),
                "end_date": datetime.date(2018, 3, 5),
                "retries": 1,
                "retry_delay": timedelta(seconds=300),
            },
            "concurrency": 1,
            "max_active_runs": 1,
            "dagrun_timeout": timedelta(seconds=600),
            "default_view": "tree",
            "orientation": "LR",
            "schedule": "0 1 * * *",
        },
        "example_dag": {
            "doc_md": "##here is a doc md string",
            "default_args": {"owner": "custom_owner", "start_date": "2 days"},
            "description": "this is an example dag",
            "schedule": "0 3 * * *",
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
        },
        "example_dag2": {
            "doc_md_file_path": DOC_MD_FIXTURE_FILE,
            "schedule": "None",
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
        },
        "example_dag3": {
            "doc_md_python_callable_name": "mydocmdbuilder",
            "doc_md_python_callable_file": DOC_MD_PYTHON_CALLABLE_FILE,
            "doc_md_python_arguments": {"arg1": "arg1", "arg2": "arg2"},
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
            ],
        },
        "example_dag4": {
            "vars": {"arg1": "hello", "arg2": "hello world"},
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo hello world",
                },
            ],
        },
    }
    td = dagfactory.DagFactory(DAG_FACTORY_VARIABLES_AS_ARGUMENTS)
    actual = td._load_dag_config(TEST_DAG_FACTORY)
    actual["example_dag2"]["doc_md_file_path"] = DOC_MD_FIXTURE_FILE
    actual["example_dag3"]["doc_md_python_callable_file"] = DOC_MD_PYTHON_CALLABLE_FILE
    assert sorted(actual) == sorted(expected)


def test_load_dag_config_invalid():
    td = dagfactory.DagFactory(DAG_FACTORY_VARIABLES_AS_ARGUMENTS)
    with pytest.raises(Exception):
        td._load_dag_config(INVALID_YAML)


def test_get_dag_configs(monkeypatch):
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    expected = {
        "example_dag": {
            "doc_md": "##here is a doc md string",
            "default_args": {"owner": "custom_owner", "start_date": "2 days"},
            "description": "this is an example dag",
            "schedule": "0 3 * * *",
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
        },
        "example_dag2": {
            "doc_md_file_path": DOC_MD_FIXTURE_FILE,
            "schedule": "None",
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
        },
        "example_dag3": {
            "doc_md_python_callable_name": "mydocmdbuilder",
            "doc_md_python_callable_file": DOC_MD_PYTHON_CALLABLE_FILE,
            "doc_md_python_arguments": {"arg1": "arg1", "arg2": "arg2"},
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
            ],
        },
        "example_dag4": {
            "vars": {"arg1": "hello", "arg2": "hello world"},
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo hello world",
                },
            ],
        },
    }
    actual = td.get_dag_configs()
    actual["example_dag2"]["doc_md_file_path"] = DOC_MD_FIXTURE_FILE
    actual["example_dag3"]["doc_md_python_callable_file"] = DOC_MD_PYTHON_CALLABLE_FILE
    assert sorted(actual) == sorted(expected)


def test_get_default_config(monkeypatch):
    monkeypatch.setenv("AUTO_CONVERT_TO_AF3", "true")
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    expected = {
        "default_args": {
            "owner": "default_owner",
            "start_date": datetime.date(2018, 3, 1),
            "end_date": datetime.date(2018, 3, 5),
            "retries": 1,
            "retry_delay": timedelta(seconds=300),
        },
        "concurrency": 1,
        "max_active_runs": 1,
        "dagrun_timeout": timedelta(seconds=600),
        "default_view": "tree",
        "orientation": "LR",
        "schedule": "0 1 * * *",
    }
    actual = td.get_default_config()
    assert actual == expected


def test_generate_dags_valid():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    td.generate_dags(globals())
    assert "example_dag" in globals()
    assert "example_dag2" in globals()
    assert "fake_example_dag" not in globals()


def test_generate_dags_with_removal_valid():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    td.generate_dags(globals())
    assert "example_dag" in globals()
    assert "example_dag2" in globals()
    assert "fake_example_dag" not in globals()

    del td.config["example_dag"]
    del td.config["example_dag2"]


def test_generate_dags_invalid():
    td = dagfactory.DagFactory(INVALID_DAG_FACTORY)
    with pytest.raises(Exception):
        td.generate_dags(globals())


@pytest.mark.skipif(version.parse(AIRFLOW_VERSION) < version.parse("2.7.0"), reason="Requires Airflow >= 2.7.0")
def test_kubernetes_pod_operator_dag_gte_2_7():
    td = dagfactory.DagFactory(DAG_FACTORY_KUBERNETES_POD_OPERATOR)
    td.generate_dags(globals())
    assert "example_dag" in globals()


@pytest.mark.skipif(version.parse(AIRFLOW_VERSION) >= version.parse("2.7.0"), reason="Requires Airflow < 2.7.0")
def test_kubernetes_pod_operator_dag_lt_2_7():
    td = dagfactory.DagFactory(DAG_FACTORY_KUBERNETES_POD_OPERATOR_LT_2_7)
    td.generate_dags(globals())
    assert "example_dag" in globals()


def test_variables_as_arguments_dag(monkeypatch):
    override_command = "value_from_variable"
    os.environ["AIRFLOW_VAR_VAR1"] = override_command
    td = dagfactory.DagFactory(DAG_FACTORY_VARIABLES_AS_ARGUMENTS)
    td.generate_dags(globals())
    tasks = globals()["example_dag"].tasks
    for task in tasks:
        if task.task_id == "task_3":
            assert task.bash_command == override_command


@pytest.mark.skipif(
    version.parse(AIRFLOW_VERSION) >= version.parse("3.0.0"),
    reason="Skipping this because yaml import old version of operator",
)
def test_doc_md_file_path(monkeypatch):
    dag_config = f"""
## YML DAG
```yaml
default:
  concurrency: 1
  default_args:
    end_date: 2018-03-05
    owner: default_owner
    retries: 1
    start_date: 2018-03-01
  default_view: tree
  max_active_runs: 1
  orientation: LR
  schedule: 0 1 * * *

example_dag2:
  doc_md_file_path: {DOC_MD_FIXTURE_FILE}
  schedule: None
  tasks:
  - task_id: task_1
    bash_command: echo 1
    operator: airflow.operators.bash.BashOperator
  - task_id: task_2
    bash_command: echo 2
    dependencies:
    - task_1
    operator: airflow.operators.bash.BashOperator
  - task_id: task_3
    bash_command: echo 3
    dependencies:
    - task_1
    operator: airflow.operators.bash.BashOperator

```"""
    YAML_PATH = os.path.join(here, "fixtures_without_default_yaml/dag_factory.yml")

    td = dagfactory.DagFactory(YAML_PATH)
    td.generate_dags(globals())
    generated_doc_md = globals()["example_dag2"].doc_md
    with open(DOC_MD_FIXTURE_FILE, "r") as file:
        expected_doc_md = file.read() + dag_config
    assert generated_doc_md == expected_doc_md


def test_doc_md_callable():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    td.generate_dags(globals())
    expected_doc_md = globals()["example_dag3"].doc_md
    assert str(td.get_dag_configs()["example_dag3"]["doc_md_python_arguments"]) in expected_doc_md


def _get_schedule_value(dag_name: str):
    """Helper function to get schedule value from a DAG based on Airflow version."""
    if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0"):
        return globals()[dag_name].schedule_interval
    else:
        return globals()[dag_name].schedule


def test_schedule():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    td.generate_dags(globals())
    schedule = _get_schedule_value("example_dag2")
    expected_schedule = None
    assert schedule == expected_schedule


def test_no_schedule_supplied():
    td = dagfactory.DagFactory(DAG_FACTORY_NO_OR_NONE_STRING_SCHEDULE)
    td.generate_dags(globals())
    schedule = _get_schedule_value("example_dag_no_schedule")
    expected_schedule = datetime.timedelta(days=1) if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0") else None
    assert schedule == expected_schedule


def test_none_string_schedule_supplied():
    td = dagfactory.DagFactory(DAG_FACTORY_NO_OR_NONE_STRING_SCHEDULE)
    td.generate_dags(globals())
    schedule = _get_schedule_value("example_dag_none_string_schedule")
    expected_schedule = None
    assert schedule == expected_schedule


def test_schedule_interval_supplied():
    td = dagfactory.DagFactory(DAG_FACTORY_SCHEDULE_INTERVAL)
    with pytest.raises(
        exceptions.DagFactoryException,
        match="The `schedule_interval` key is no longer supported in Airflow 3\\.0\\+\\. Use `schedule` instead\\.",
    ):
        td.generate_dags(globals())


def test_dagfactory_dict():
    td = dagfactory.DagFactory(config=DAG_FACTORY_CONFIG)
    expected_default = {
        "default_args": {
            "owner": "airflow",
            "start_date": "2020-01-01",
            "end_date": "2020-01-01",
        },
        "default_view": "graph",
        "schedule": "@daily",
    }
    expected_dag = {
        "example_dag": {
            "tasks": [
                {
                    "task_id": "task_1",
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
            ],
        },
    }
    actual_dag = td.get_dag_configs()
    actual_default = td.get_default_config()
    assert actual_dag == expected_dag
    assert actual_default == expected_default


def test_dagfactory_dict_and_yaml():
    error_message = "Either `config_filepath` or `config` should be provided"
    with pytest.raises(AssertionError, match=error_message):
        dagfactory.DagFactory(config_filepath=TEST_DAG_FACTORY, config=DAG_FACTORY_CONFIG)


def test_get_dag_configs_dict():
    td = dagfactory.DagFactory(config_filepath=TEST_DAG_FACTORY)
    assert not set(dagfactory.SYSTEM_PARAMS).issubset(set(td.get_dag_configs()))


def print_context_callback(context, **kwargs):
    print(context)


def test_generate_dags_with_removal_valid_and_callback():
    td = dagfactory.DagFactory(config=DAG_FACTORY_CALLBACK_CONFIG)
    td.generate_dags(globals())


def test_set_callback_after_loading_config():
    td = dagfactory.DagFactory(config=DAG_FACTORY_CONFIG)  # Generate the DAG factory object
    td.config["default"]["default_args"]["on_success_callback"] = f"{__name__}.print_context_callback"
    td.generate_dags(globals())


def test_build_dag_with_global_default():
    dags = dagfactory.DagFactory(
        config=DAG_FACTORY_CONFIG, default_args_config_path=DEFAULT_ARGS_CONFIG_ROOT
    ).build_dags()

    assert dags.get("example_dag").tasks[0].depends_on_past == True


def test_build_dag_with_global_dag_level_defaults():
    """Test that DAG-level defaults from global defaults.yml are applied to individual DAG configs"""
    global_defaults = {
        "default_args": {
            "owner": "global_owner",
            "start_date": "2020-01-01",
        },
        "schedule": "0 1 * * *",
        "catchup": False,
        "tags": ["global_tag"],
    }

    config = {
        "test_dag": {"tasks": [{"task_id": "task_1", "operator": get_bash_operator_path(), "bash_command": "echo 1"}]},
        "test_dag_override": {
            "catchup": True,
            "tasks": [{"task_id": "task_1", "operator": get_bash_operator_path(), "bash_command": "echo 1"}],
        },
    }

    td = dagfactory.DagFactory(config=config)
    with pytest.MonkeyPatch.context() as m:
        m.setattr(td, "_global_default_args", lambda: global_defaults)
        dags = td.build_dags()

    assert dags["test_dag"].catchup == False
    assert "global_tag" in dags["test_dag"].tags

    # Dag config value should override global default
    assert dags["test_dag_override"].catchup == True
    assert "global_tag" in dags["test_dag_override"].tags


def test_build_dag_with_global_default_dict():
    dags = dagfactory.DagFactory(
        config=DAG_FACTORY_CONFIG,
        default_args_config_dict={
            "default_args": {"start_date": "2025-01-01", "owner": "global_owner", "depends_on_past": True}
        },
    ).build_dags()

    assert dags.get("example_dag").tasks[0].depends_on_past == True


def test_load_invalid_yaml_logs_error(caplog):
    caplog.set_level(logging.ERROR)
    load_yaml_dags(
        globals_dict=globals(),
        dags_folder="tests/fixtures",
        suffix=["invalid_yaml.yml"],
    )
    assert caplog.messages == ["Failed to load dag from tests/fixtures/invalid_yaml.yml"]


def test_load_yaml_dags_default_suffix_succeed(caplog):
    caplog.set_level(logging.INFO)
    load_yaml_dags(
        globals_dict=globals(),
        dags_folder="tests/fixtures",
    )
    assert "Loading DAGs from tests/fixtures" in caplog.messages


@pytest.mark.skipif(
    version.parse(AIRFLOW_VERSION) >= version.parse("3.0.0"),
    reason="Skipping this because yaml import old version of operator",
)
def test_yml_dag_rendering_in_docs():
    dag_path = os.path.join(here, "fixtures_without_default_yaml/dag_md_docs.yml")
    td = dagfactory.DagFactory(
        dag_path,
    )
    td.generate_dags(globals())
    generated_doc_md = globals()["example_dag2"].doc_md
    with open(dag_path, "r") as file:
        expected_doc_md = "## YML DAG\n```yaml\n" + file.read() + "\n```"
    assert generated_doc_md == expected_doc_md


def test_generate_dags_with_default_args_execution_timeout():
    config_dict = {
        "default": {"default_args": {"start_date": "2024-11-11", "execution_timeout": timedelta(seconds=1)}},
        "basic_example_dag": {
            "schedule": "0 3 * * *",
            "tasks": {
                "task_1": {"operator": "airflow.operators.bash.BashOperator", "bash_command": "sleep 5"},
            },
        },
    }
    td = dagfactory.DagFactory(config=config_dict)
    td.generate_dags(globals())
    tasks = globals()["basic_example_dag"].tasks
    assert tasks[0].execution_timeout == datetime.timedelta(seconds=1)


def test_dag_level_start():
    data = """
    my_dag:
      schedule: "0 3 * * *"
      start_date: 2024-11-11
      end_date: 2025-11-11
      dagrun_timeout:
        __type__: datetime.timedelta
        hours: 3
      default_args:
        retry_delay:
          __type__: datetime.timedelta
          seconds: 25
        execution_timeout:
          __type__: datetime.timedelta
          seconds: 15
        sla:
          __type__: datetime.timedelta
          seconds: 10
      tasks:
        task_1:
          operator: airflow.operators.bash.BashOperator
          bash_command: "echo 1"
    """

    # Write to temporary YAML file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        tmp.write(data)
        temp_file = tmp.name

    # Use DagFactory to load and generate DAGs
    df = DagFactory(config_filepath=temp_file)
    df.generate_dags(globals=globals())
    dag = globals()["my_dag"]

    assert dag.start_date == DateTime(2024, 11, 11, 0, 0, 0, tzinfo=Timezone("UTC"))
    assert dag.end_date == DateTime(2025, 11, 11, 0, 0, 0, tzinfo=Timezone("UTC"))
    assert dag.dagrun_timeout == datetime.timedelta(hours=3)
    assert dag.tasks[0].retry_delay == datetime.timedelta(seconds=25)
    assert dag.tasks[0].execution_timeout == datetime.timedelta(seconds=15)
    if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0"):
        assert dag.tasks[0].sla == datetime.timedelta(seconds=10)


def test_retrieve_possible_default_config_dirs_default_path_is_parent(tmp_path):
    # Create structure: tmp_path/a/b/c/dag.yml
    dag_path = tmp_path / "a" / "b" / "c"
    dag_path.mkdir(parents=True)
    dag_file = dag_path / "dag.yml"
    shutil.copyfile(TEST_DAG_FACTORY, str(dag_file))

    default_config_path = tmp_path / "a"

    some_dag = dagfactory.DagFactory(str(dag_file), default_args_config_path=str(default_config_path))

    result = some_dag._retrieve_possible_default_config_dirs()
    expected = [tmp_path / "a" / "b" / "c", tmp_path / "a" / "b", tmp_path / "a"]
    assert result == expected


def test_retrieve_possible_default_config_dirs_default_path_not_in_config_parents(tmp_path):
    # Structure: tmp_path/config/a/b/c/dag.yml, and tmp_path/other as unrelated default path
    config_path = tmp_path / "config" / "a" / "b" / "c"
    config_path.mkdir(parents=True)
    dag_file = config_path / "dag.yml"
    shutil.copyfile(TEST_DAG_FACTORY, str(dag_file))

    unrelated_default_path = tmp_path / "other"
    unrelated_default_path.mkdir()

    some_dag = dagfactory.DagFactory(str(dag_file), default_args_config_path=str(unrelated_default_path))

    result = some_dag._retrieve_possible_default_config_dirs()
    expected = [tmp_path / "config" / "a" / "b" / "c", unrelated_default_path]
    assert result == expected


def test_retrieve_possible_default_config_dirs_no_config_path(tmp_path):
    default_config_path = tmp_path / "default"
    default_config_path.mkdir()

    some_dag = dagfactory.DagFactory(
        config_filepath=None, config={"a": "b"}, default_args_config_path=str(default_config_path)
    )

    result = some_dag._retrieve_possible_default_config_dirs()
    assert result == [default_config_path]


def _write_sample_defaults(path: Path, identifier: str):
    data = {
        "default_args": {"owner": identifier, f"{identifier}_param": identifier},
        "tags": [identifier],
        f"{identifier}_dag_param": identifier,
    }
    with open(path / "defaults.yml", "w") as fp:
        yaml.dump(data, fp)


@patch("dagfactory.dagfactory.DagFactory._serialise_config_md")
def test_default_override_based_on_directory_tree(serialize_config_md_mock, tmp_path):
    # Create structure: tmp_path/a/b/c/dag.yml
    dag_path = tmp_path / "a/b/c"
    dag_path.mkdir(parents=True)
    dag_file = dag_path / "dag.yml"
    shutil.copyfile(DAG_FACTORY_VARIABLES_AS_ARGUMENTS, str(dag_file))

    _write_sample_defaults(tmp_path / "a", "a")
    _write_sample_defaults(tmp_path / "a/b", "b")
    _write_sample_defaults(tmp_path / "a/b/c", "c")

    some_dag = dagfactory.DagFactory(str(dag_file), default_args_config_path=str(tmp_path / "a"))

    result = some_dag.build_dags()
    dag = result["second_example_dag"]
    assert dag.default_args["a_param"] == "a"  # accumulates properties define throughout the directories tree
    assert dag.default_args["b_param"] == "b"  # accumulates properties define throughout the directories tree
    assert dag.default_args["c_param"] == "c"  # accumulates properties define throughout the directories tree
    if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0"):
        assert dag.tags == ["a", "b", "c", "dagfactory"]  # contains closest directory default.yml values
    else:
        assert dag.tags == {"a", "b", "c", "dagfactory"}  # contains closest directory default.yml values
    assert dag.owner == "default_owner"  # defined in the YAML `default` section

    dag_build_params = serialize_config_md_mock.call_args[0]

    dag_name = dag_build_params[0]
    assert dag_name == "second_example_dag"

    dag_config = dag_build_params[1]
    assert dag_config["a_dag_param"] == "a"
    assert dag_config["b_dag_param"] == "b"
    assert dag_config["c_dag_param"] == "c"

    default_config = dag_build_params[2]["default_args"]
    assert default_config["a_param"] == "a"
    assert default_config["b_param"] == "b"
    assert default_config["c_param"] == "c"


# Test to ensure backward compatibility for dictionary-style tasks and task_groups definitions
def test_tasks_and_task_groups_as_dict():
    """Ensure DagFactory accepts tasks and task_groups provided as dictionaries (backwards compatibility)."""
    dict_style_config = {
        "example_dict_dag": {
            "default_args": {
                "owner": "global_owner",
                "start_date": "2020-01-01",
            },
            "schedule": "0 4 * * *",
            # Task groups supplied as a mapping of group_name -> config
            "task_groups": {
                "task_group_1": {
                    "tooltip": "this is a task group",
                    "dependencies": ["task_1"],
                }
            },
            # Tasks supplied as a mapping of task_id -> config (no embedded task_id field)
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
                "task_2": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 2",
                    "dependencies": ["task_1"],
                    "task_group_name": "task_group_1",
                },
            },
        }
    }
    td = dagfactory.DagFactory(config=dict_style_config)
    td.generate_dags(globals())
    assert "example_dict_dag" in globals()
    dag = globals()["example_dict_dag"]
    assert len(dag.tasks) == 2
    # Ensure the second task has been placed inside the task group (task_id is prefixed by the group name)
    assert any(task.task_id.startswith("task_group_1.task_2") for task in dag.tasks)


def test_tasks_and_task_groups_as_dict_yaml(tmp_path):
    """Ensure DagFactory correctly loads YAML where tasks and task_groups are dictionaries."""
    yaml_content = """
default:
    default_args:
        owner: global_owner
        start_date: "2020-01-01"
example_dict_dag_yaml:
  schedule: "0 4 * * *"
  task_groups:
    task_group_1:
      tooltip: "this is a task group"
      dependencies: [task_1]
  tasks:
    task_1:
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo 1"
    task_2:
      operator: airflow.operators.bash.BashOperator
      bash_command: "echo 2"
      dependencies: [task_1]
      task_group_name: task_group_1
"""
    yaml_path = tmp_path / "dict_style_dag.yml"
    yaml_path.write_text(yaml_content)

    td = dagfactory.DagFactory(config_filepath=str(yaml_path))
    td.generate_dags(globals())
    assert "example_dict_dag_yaml" in globals()
    dag = globals()["example_dict_dag_yaml"]
    # Validate two tasks created
    assert len(dag.tasks) == 2
    # Validate grouping
    assert any(task.task_id.startswith("task_group_1.task_2") for task in dag.tasks)


def test_load_dag_config_with_extends(monkeypatch):
    """Test that extends functionality works correctly through get_default_args_from_extends and build_dags."""
    monkeypatch.setenv("AUTO_CONVERT_TO_AF3", "true")
    test_config_path = os.path.join(here, "fixtures/dag_factory_extends.yml")
    fixtures_dir = os.path.join(here, "fixtures")
    td = dagfactory.DagFactory(test_config_path, default_args_config_path=fixtures_dir)

    # Test that _load_dag_config preserves extends key but doesn't process it
    config = td._load_dag_config(test_config_path)
    assert "__extends__" in config
    assert config["__extends__"] == ["extends_base.yml"]

    # Test that get_default_args_from_extends processes extends correctly
    extends_defaults = td.get_default_args_from_extends()
    assert extends_defaults["concurrency"] == 5  # From base config (will be converted to max_active_tasks later)
    assert extends_defaults["max_active_runs"] == 3  # From base config
    assert extends_defaults["default_view"] == "graph"  # From base config
    assert extends_defaults[get_schedule_key()] == "@daily"  # From base config (handles Airflow 2/3 compatibility)
    assert extends_defaults["default_args"]["owner"] == "base_owner"  # From base config
    assert extends_defaults["default_args"]["retries"] == 2  # From base config

    # Test that the full DAG building process works correctly
    dags = td.build_dags()
    dag = dags["example_dag_with_extends"]

    # Verify that base config values are applied
    assert dag.max_active_runs == 3  # From base config
    assert dag.max_active_tasks == 5  # From base config (Airflow 3 equivalent of concurrency)

    # Verify that main config overrides work
    assert dag.schedule == "@hourly"  # Overridden from main config
    assert dag.default_args["owner"] == "main_owner"  # Overridden from main config

    # Verify that base config values are preserved when not overridden
    assert dag.default_args["retries"] == 2  # From base config
    assert dag.default_args["retry_delay_sec"] == 600  # From base config

    # Verify that new values are added
    assert dag.default_args["email"] == "test@example.com"  # New from main config


def test_load_dag_config_with_chained_extends(monkeypatch):
    """Test that chained extends functionality works correctly through get_default_args_from_extends and build_dags."""
    monkeypatch.setenv("AUTO_CONVERT_TO_AF3", "true")
    test_config_path = os.path.join(here, "fixtures/dag_factory_extends_chained.yml")
    fixtures_dir = os.path.join(here, "fixtures")
    td = dagfactory.DagFactory(test_config_path, default_args_config_path=fixtures_dir)

    # Test that _load_dag_config preserves extends key but doesn't process it
    config = td._load_dag_config(test_config_path)
    assert "__extends__" in config
    assert config["__extends__"] == ["dag_factory_extends.yml"]

    # Test that get_default_args_from_extends processes chained extends correctly
    extends_defaults = td.get_default_args_from_extends()

    # Values should come from the base config (extends_base.yml)
    assert extends_defaults["concurrency"] == 5  # From base
    assert extends_defaults["default_view"] == "graph"  # From base
    assert extends_defaults["default_args"]["retries"] == 2  # From base
    assert extends_defaults["default_args"]["retry_delay_sec"] == 600  # From base

    # Values from middle config should override base
    assert extends_defaults[get_schedule_key()] == "@hourly"  # From middle config (overrides @daily from base)
    assert extends_defaults["default_args"]["email"] == "test@example.com"  # From middle config

    # Test that the full DAG building process works correctly
    dags = td.build_dags()
    dag = dags["example_dag_chained_extends"]

    # Verify that chained extends values are applied correctly
    assert dag.max_active_runs == 1  # Final override from main config
    assert dag.max_active_tasks == 5  # From base config
    assert dag.default_args["owner"] == "chained_owner"  # Final override from main config
    assert dag.default_args["retries"] == 2  # From base config (preserved through chain)
    assert dag.default_args["email"] == "test@example.com"  # From middle config (preserved)


def test_load_dag_config_extends_missing_file():
    """Test that get_default_args_from_extends raises appropriate error when extended file is missing."""
    # Create a temporary config that references a non-existent file
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(
            """
__extends__:
  - non_existent_file.yml

default:
  default_args:
    owner: test_owner

test_dag:
  tasks:
    task_1:
      operator: airflow.operators.bash.BashOperator
      bash_command: echo "test"
"""
        )
        temp_config_path = f.name

    try:
        # Set the base directory to the temp directory where the config file is created
        temp_dir = os.path.dirname(temp_config_path)
        with pytest.raises(FileNotFoundError):  # Should raise FileNotFoundError due to missing file
            td = dagfactory.DagFactory(temp_config_path, default_args_config_path=temp_dir)
            td.get_default_args_from_extends()
    finally:
        # Clean up temporary file
        os.unlink(temp_config_path)


def test_load_dag_config_extends_empty_list():
    """Test that _load_dag_config handles empty __extends__ list correctly."""
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(
            """
__extends__: []

default:
  default_args:
    owner: test_owner
    start_date: 2023-01-01

test_dag:
  tasks:
    task_1:
      operator: airflow.operators.bash.BashOperator
      bash_command: echo "test"
"""
        )
        temp_config_path = f.name

    try:
        td = dagfactory.DagFactory(temp_config_path)
        actual = td._load_dag_config(temp_config_path)

        # Verify that the extends key is preserved (even when empty)
        assert "__extends__" in actual
        assert actual["__extends__"] == []

        # Verify that the config is processed normally
        assert actual["default"]["default_args"]["owner"] == "test_owner"
        assert actual["default"]["default_args"]["start_date"] == datetime.date(2023, 1, 1)  # Parsed as date
        assert "test_dag" in actual
    finally:
        # Clean up temporary file
        os.unlink(temp_config_path)


def test_load_dag_config_extends_infinite_loop():
    """Test that get_default_args_from_extends raises appropriate error when infinite extending loop is detected."""
    import tempfile

    # Create two temp files that reference each other to create an infinite loop
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f1:
        f1.write(
            """
__extends__:
  - file2.yml

default:
  default_args:
    owner: file1_owner
"""
        )
        temp_config1_path = f1.name

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f2:
        f2.write(
            """
__extends__:
  - file1.yml

default:
  default_args:
    owner: file2_owner
"""
        )
        temp_config2_path = f2.name

    # Rename files to match the references in the config
    base_dir = os.path.dirname(temp_config1_path)
    file1_path = os.path.join(base_dir, "file1.yml")
    file2_path = os.path.join(base_dir, "file2.yml")

    os.rename(temp_config1_path, file1_path)
    os.rename(temp_config2_path, file2_path)

    try:
        # Test starting from file1 -> file2 -> file1 (infinite loop)
        with pytest.raises(exceptions.DagFactoryConfigException) as exc_info:
            td = dagfactory.DagFactory(file1_path, default_args_config_path=base_dir)
            td.get_default_args_from_extends()

        # Verify that the error message mentions the infinite loop and the problematic file
        error_message = str(exc_info.value)
        assert "Infinite extending loop detected" in error_message
        assert "file1.yml" in error_message
        assert "has already been processed" in error_message

    finally:
        # Clean up temporary files
        for file_path in [file1_path, file2_path]:
            if os.path.exists(file_path):
                os.unlink(file_path)


def test_priority_defaults_extends_and_default_block():
    """Test that defaults.yml, __extends__, and default block are applied with correct priority.

    Priority behavior:
    - For default_args: main config > __extends__ > defaults.yml
    - For DAG-level properties: defaults.yml appears to override __extends__ configs
    - Tags: defaults.yml tags are preserved, with dagfactory auto-added
    """
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # 1. Create defaults.yml (lowest priority)
        defaults_content = """
default_args:
  owner: global_owner
  retries: 5
  start_date: 2020-01-01
max_active_runs: 5
tags: ["global"]
"""
        defaults_file = temp_path / "defaults.yml"
        with open(defaults_file, "w") as f:
            f.write(defaults_content)

        # 2. Create base config for __extends__ (medium priority)
        base_config_content = """
default:
  default_args:
    owner: extends_owner
    retries: 3
    start_date: 2021-01-01
    email: test@extends.com
  max_active_runs: 3
  tags: ["extends"]
"""
        base_config_file = temp_path / "base_config.yml"
        with open(base_config_file, "w") as f:
            f.write(base_config_content)

        # 3. Create main config with __extends__ and default block (highest priority)
        main_config_content = f"""
__extends__:
  - base_config.yml

default:
  default_args:
    owner: main_owner
    retries: 1
    depends_on_past: true
  tags: ["main"]

test_dag:
  tasks:
    task_1:
      operator: airflow.operators.bash.BashOperator
      bash_command: echo "test"
"""
        main_config_file = temp_path / "main_config.yml"
        with open(main_config_file, "w") as f:
            f.write(main_config_content)

        # Create DagFactory with defaults.yml path and main config
        td = dagfactory.DagFactory(
            config_filepath=str(main_config_file),
            default_args_config_path=str(temp_path)
        )

        # Build DAGs to test the full priority chain
        dags = td.build_dags()
        dag = dags["test_dag"]

        # Test default_args priority: main > extends > defaults
        assert dag.default_args["owner"] == "main_owner"  # From main config (highest priority)
        assert dag.default_args["retries"] == 1  # From main config (highest priority)
        assert dag.default_args["start_date"] == DateTime(2021, 1, 1, 0, 0, 0, tzinfo=Timezone("UTC"))  # From extends (medium priority, not overridden)
        assert dag.default_args["email"] == "test@extends.com"  # From extends (medium priority, not overridden)
        assert dag.default_args["depends_on_past"] == True  # From main config (only defined there)

        # Test DAG-level properties priority:
        # For max_active_runs, defaults.yml overrides extends config
        assert dag.max_active_runs == 5  # From defaults.yml (global defaults take precedence for DAG-level props)
        assert dag.owner == "main_owner"  # From main config default_args (highest priority)

        # Test tags behavior (defaults.yml tags are used as base)
        # Both "global" from defaults.yml and "dagfactory" (auto-added) should be present
        assert "global" in dag.tags
        assert "dagfactory" in dag.tags
