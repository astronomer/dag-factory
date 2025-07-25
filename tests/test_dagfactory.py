import datetime
import logging
import os
import shutil
import tempfile
import yaml
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

from tests.utils import get_bash_operator_path, get_schedule_key

here = os.path.dirname(__file__)

from dagfactory import DagFactory, dagfactory, load_yaml_dags

TEST_DAG_FACTORY = os.path.join(here, "fixtures/dag_factory.yml")
DAG_FACTORY_NO_OR_NONE_STRING_SCHEDULE = os.path.join(here, "fixtures/dag_factory_no_or_none_string_schedule.yml")
INVALID_YAML = os.path.join(here, "fixtures/invalid_yaml.yml")
INVALID_DAG_FACTORY = os.path.join(here, "fixtures_without_default/invalid_dag_factory.yml")
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
        get_schedule_key(): "@daily",
    },
    "example_dag": {
        "tasks": {
            "task_1": {
                "operator": get_bash_operator_path(),
                "bash_command": "echo 1",
            },
        },
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
        get_schedule_key(): "0 3 * * *",
        "tags": ["tag1", "tag2"],
        "on_failure_callback": f"{__name__}.print_context_callback",
        "on_success_callback": f"{__name__}.print_context_callback",
        "sla_miss_callback": f"{__name__}.print_context_callback",
        "tasks": {
            "task_1": {
                "operator": get_bash_operator_path(),
                "bash_command": "echo 1",
                "execution_timeout_secs": 5,
                "on_failure_callback": f"{__name__}.print_context_callback",
                "on_success_callback": f"{__name__}.print_context_callback",
                "on_execute_callback": f"{__name__}.print_context_callback",
                "on_retry_callback": f"{__name__}.print_context_callback",
            },
            "task_2": {
                "operator": get_bash_operator_path(),
                "bash_command": "echo 2",
                "dependencies": ["task_1"],
                "on_failure_callback": f"{__name__}.print_context_callback",
                "on_success_callback": f"{__name__}.print_context_callback",
                "on_execute_callback": f"{__name__}.print_context_callback",
                "on_retry_callback": f"{__name__}.print_context_callback",
            },
            "task_3": {
                "operator": get_bash_operator_path(),
                "bash_command": "echo 3",
                "dependencies": ["task_1"],
                "on_failure_callback": f"{__name__}.print_context_callback",
                "on_success_callback": f"{__name__}.print_context_callback",
                "on_execute_callback": f"{__name__}.print_context_callback",
                "on_retry_callback": f"{__name__}.print_context_callback",
            },
        },
    }
}


def test_validate_config_filepath_valid():
    dagfactory.DagFactory._validate_config_filepath(TEST_DAG_FACTORY)


def test_validate_config_filepath_invalid():
    with pytest.raises(Exception):
        dagfactory.DagFactory._validate_config_filepath("config.yml")


@pytest.mark.skipif(
    version.parse(AIRFLOW_VERSION) < version.parse("2.4.0"), reason="Requires Airflow version greater than 2.4.0"
)
def test_load_dag_config_valid(monkeypatch):
    monkeypatch.setenv("AUTO_CONVERT_TO_AF3", "true")
    expected = {
        "default": {
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
            "default_view": "tree",
            "orientation": "LR",
            get_schedule_key(): "0 1 * * *",
        },
        "example_dag": {
            "doc_md": "##here is a doc md string",
            "default_args": {"owner": "custom_owner", "start_date": "2 days"},
            "description": "this is an example dag",
            get_schedule_key(): "0 3 * * *",
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
                "task_2": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 2",
                    "dependencies": ["task_1"],
                },
                "task_3": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 3",
                    "dependencies": ["task_1"],
                },
            },
        },
        "example_dag2": {
            "doc_md_file_path": DOC_MD_FIXTURE_FILE,
            get_schedule_key(): "None",
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
                "task_2": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 2",
                    "dependencies": ["task_1"],
                },
                "task_3": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 3",
                    "dependencies": ["task_1"],
                },
            },
        },
        "example_dag3": {
            "doc_md_python_callable_name": "mydocmdbuilder",
            "doc_md_python_callable_file": DOC_MD_PYTHON_CALLABLE_FILE,
            "doc_md_python_arguments": {"arg1": "arg1", "arg2": "arg2"},
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
            },
        },
        "example_dag4": {
            "vars": {"arg1": "hello", "arg2": "hello world"},
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo hello world",
                },
            },
        },
    }
    td = dagfactory.DagFactory(DAG_FACTORY_VARIABLES_AS_ARGUMENTS)
    actual = td._load_dag_config(TEST_DAG_FACTORY)
    actual["example_dag2"]["doc_md_file_path"] = DOC_MD_FIXTURE_FILE
    actual["example_dag3"]["doc_md_python_callable_file"] = DOC_MD_PYTHON_CALLABLE_FILE
    assert actual == expected


def test_load_dag_config_invalid():
    td = dagfactory.DagFactory(DAG_FACTORY_VARIABLES_AS_ARGUMENTS)
    with pytest.raises(Exception):
        td._load_dag_config(INVALID_YAML)


@pytest.mark.skipif(
    version.parse(AIRFLOW_VERSION) < version.parse("2.4.0"),
    reason="Require Airflow >=2.4.0",
)
def test_get_dag_configs(monkeypatch):
    monkeypatch.setenv("AUTO_CONVERT_TO_AF3", "true")
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    expected = {
        "example_dag": {
            "doc_md": "##here is a doc md string",
            "default_args": {"owner": "custom_owner", "start_date": "2 days"},
            "description": "this is an example dag",
            get_schedule_key(): "0 3 * * *",
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
                "task_2": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 2",
                    "dependencies": ["task_1"],
                },
                "task_3": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 3",
                    "dependencies": ["task_1"],
                },
            },
        },
        "example_dag2": {
            "doc_md_file_path": DOC_MD_FIXTURE_FILE,
            get_schedule_key(): "None",
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
                "task_2": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 2",
                    "dependencies": ["task_1"],
                },
                "task_3": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 3",
                    "dependencies": ["task_1"],
                },
            },
        },
        "example_dag3": {
            "doc_md_python_callable_name": "mydocmdbuilder",
            "doc_md_python_callable_file": DOC_MD_PYTHON_CALLABLE_FILE,
            "doc_md_python_arguments": {"arg1": "arg1", "arg2": "arg2"},
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
            },
        },
        "example_dag4": {
            "vars": {"arg1": "hello", "arg2": "hello world"},
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo hello world",
                },
            },
        },
    }
    actual = td.get_dag_configs()
    actual["example_dag2"]["doc_md_file_path"] = DOC_MD_FIXTURE_FILE
    actual["example_dag3"]["doc_md_python_callable_file"] = DOC_MD_PYTHON_CALLABLE_FILE
    assert actual == expected


def test_get_default_config():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    expected = {
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
        "default_view": "tree",
        "orientation": "LR",
        get_schedule_key(): "0 1 * * *",
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
    monkeypatch.setenv("AUTO_CONVERT_TO_AF3", "true")
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
  dagrun_timeout_sec: 600
  default_args:
    end_date: 2018-03-05
    owner: default_owner
    retries: 1
    retry_delay_sec: 300
    start_date: 2018-03-01
  default_view: tree
  max_active_runs: 1
  orientation: LR
  schedule_interval: 0 1 * * *

example_dag2:
  doc_md_file_path: {DOC_MD_FIXTURE_FILE}
  schedule_interval: None
  tasks:
    task_1:
      bash_command: echo 1
      operator: airflow.operators.bash.BashOperator
    task_2:
      bash_command: echo 2
      dependencies:
      - task_1
      operator: airflow.operators.bash.BashOperator
    task_3:
      bash_command: echo 3
      dependencies:
      - task_1
      operator: airflow.operators.bash.BashOperator

```"""
    YAML_PATH = os.path.join(here, "fixtures_without_default/dag_factory.yml")

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


def test_schedule_interval():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    td.generate_dags(globals())
    if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0"):
        schedule_interval = globals()["example_dag2"].schedule_interval
        expected_schedule_interval = datetime.timedelta(days=1)
    else:
        schedule_interval = globals()["example_dag2"].schedule
        expected_schedule_interval = None
    assert schedule_interval == expected_schedule_interval


def test_no_schedule_supplied():
    td = dagfactory.DagFactory(DAG_FACTORY_NO_OR_NONE_STRING_SCHEDULE)
    td.generate_dags(globals())
    if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0"):
        schedule_interval = globals()["example_dag_no_schedule"].schedule_interval
        expected_schedule_interval = datetime.timedelta(days=1)
    else:
        schedule_interval = globals()["example_dag_no_schedule"].schedule
        expected_schedule_interval = None
    assert schedule_interval == expected_schedule_interval


def test_none_string_schedule_supplied():
    td = dagfactory.DagFactory(DAG_FACTORY_NO_OR_NONE_STRING_SCHEDULE)
    td.generate_dags(globals())
    if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0"):
        schedule_interval = globals()["example_dag_none_string_schedule"].schedule_interval
        expected_schedule_interval = datetime.timedelta(days=1)
    else:
        schedule_interval = globals()["example_dag_none_string_schedule"].schedule
        expected_schedule_interval = None
    assert schedule_interval == expected_schedule_interval


def test_dagfactory_dict():
    td = dagfactory.DagFactory(config=DAG_FACTORY_CONFIG)
    expected_default = {
        "default_args": {
            "owner": "airflow",
            "start_date": "2020-01-01",
            "end_date": "2020-01-01",
        },
        "default_view": "graph",
        get_schedule_key(): "@daily",
    }
    expected_dag = {
        "example_dag": {
            "tasks": {
                "task_1": {
                    "operator": get_bash_operator_path(),
                    "bash_command": "echo 1",
                },
            },
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
        get_schedule_key(): "0 1 * * *",
        "catchup": False,
        "tags": ["global_tag"],
    }

    config = {
        "test_dag": {"tasks": {"task_1": {"operator": get_bash_operator_path(), "bash_command": "echo 1"}}},
        "test_dag_override": {
            "catchup": True,
            "tasks": {"task_1": {"operator": get_bash_operator_path(), "bash_command": "echo 1"}},
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
    dag_path = os.path.join(here, "fixtures_without_default/dag_md_docs.yml")
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
        "default": {"default_args": {"start_date": "2024-11-11", "execution_timeout": 1}},
        "basic_example_dag": {
            "schedule_interval": "0 3 * * *",
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
      schedule_interval: "0 3 * * *"
      start_date: 2024-11-11
      end_date: 2025-11-11
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
    assert dag.tags == ["a", "b", "c", "dagfactory"]  # contains closest directory default.yml values
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
