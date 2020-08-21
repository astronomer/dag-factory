import os
import datetime
import pytest
from airflow.models.variable import Variable
from packaging import version
from airflow import __version__ as AIRFLOW_VERSION

here = os.path.dirname(__file__)

from dagfactory import dagfactory

TEST_DAG_FACTORY = os.path.join(here, "fixtures/dag_factory.yml")
INVALID_YAML = os.path.join(here, "fixtures/invalid_yaml.yml")
INVALID_DAG_FACTORY = os.path.join(here, "fixtures/invalid_dag_factory.yml")
DAG_FACTORY_KUBERNETES_POD_OPERATOR = os.path.join(here, "fixtures/dag_factory_kubernetes_pod_operator.yml")
DAG_FACTORY_VARIABLES_AS_ARGUMENTS = os.path.join(here, "fixtures/dag_factory_variables_as_arguments.yml")

DOC_MD_FIXTURE_FILE = os.path.join(here, "fixtures/mydocfile.md")
DOC_MD_PYTHON_CALLABLE_FILE = os.path.join(here, "fixtures/doc_md_builder.py")

@pytest.fixture(autouse=True)
def build_path_for_doc_md():
    with open(TEST_DAG_FACTORY,'r') as f:
        oldText = f.read()
        newText = oldText.replace('{here}', here)
    with open(TEST_DAG_FACTORY,'w') as f:
        f.write(newText)
    yield
    with open(TEST_DAG_FACTORY,'w') as f:
        f.write(oldText)

def test_validate_config_filepath_valid():
    dagfactory.DagFactory._validate_config_filepath(TEST_DAG_FACTORY)


def test_validate_config_filepath_invalid():
    with pytest.raises(Exception):
        dagfactory.DagFactory._validate_config_filepath("config.yml")


def test_load_config_valid():
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
            "schedule_interval": "0 1 * * *",
        },
        "example_dag": {
            "doc_md": "##here is a doc md string",
            "default_args": {"owner": "custom_owner", "start_date": "2 days"},
            "description": "this is an example dag",
            "schedule_interval": "0 3 * * *",
            "tasks": {
                "task_1": {
                    "operator": "airflow.operators.bash_operator.BashOperator",
                    "bash_command": "echo 1",
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
        },
        "example_dag2": {
            "doc_md_file_path" : DOC_MD_FIXTURE_FILE,
            "tasks": {
                "task_1": {
                    "operator": "airflow.operators.bash_operator.BashOperator",
                    "bash_command": "echo 1",
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
            }
        },
        "example_dag3": {
            "doc_md_python_callable_name" : "mydocmdbuilder",
            "doc_md_python_callable_file": DOC_MD_PYTHON_CALLABLE_FILE,
            "doc_md_python_arguments": {"arg1": "arg1", "arg2": "arg2"},
            "tasks": {
                "task_1": {
                    "operator": "airflow.operators.bash_operator.BashOperator",
                    "bash_command": "echo 1",
                },
            }
        },
    }
    actual = dagfactory.DagFactory._load_config(TEST_DAG_FACTORY)
    actual['example_dag2']['doc_md_file_path'] = DOC_MD_FIXTURE_FILE
    actual['example_dag3']['doc_md_python_callable_file'] = DOC_MD_PYTHON_CALLABLE_FILE
    assert actual == expected


def test_load_config_invalid():
    with pytest.raises(Exception):
        dagfactory.DagFactory._load_config(INVALID_YAML)


def test_get_dag_configs():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    expected = {
        "example_dag": {
            "doc_md": "##here is a doc md string",
            "default_args": {"owner": "custom_owner", "start_date": "2 days"},
            "description": "this is an example dag",
            "schedule_interval": "0 3 * * *",
            "tasks": {
                "task_1": {
                    "operator": "airflow.operators.bash_operator.BashOperator",
                    "bash_command": "echo 1",
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
        },
        "example_dag2": {
            "doc_md_file_path": DOC_MD_FIXTURE_FILE,
            "tasks": {
                "task_1": {
                    "operator": "airflow.operators.bash_operator.BashOperator",
                    "bash_command": "echo 1",
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
            }
        },
        "example_dag3": {
            "doc_md_python_callable_name": "mydocmdbuilder",
            "doc_md_python_callable_file": DOC_MD_PYTHON_CALLABLE_FILE,
            "doc_md_python_arguments": {"arg1": "arg1", "arg2": "arg2"},
            "tasks": {
                "task_1": {
                    "operator": "airflow.operators.bash_operator.BashOperator",
                    "bash_command": "echo 1",
                },
            }
        },
    }
    actual = td.get_dag_configs()
    actual['example_dag2']['doc_md_file_path'] = DOC_MD_FIXTURE_FILE
    actual['example_dag3']['doc_md_python_callable_file'] = DOC_MD_PYTHON_CALLABLE_FILE
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
        "schedule_interval": "0 1 * * *",
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

    del td.config['example_dag']
    del td.config['example_dag2']
    td.clean_dags(globals())
    assert "example_dag" not in globals()
    assert "example_dag2" not in globals()
    assert "fake_example_dag" not in globals()

def test_generate_dags_invalid():
    td = dagfactory.DagFactory(INVALID_DAG_FACTORY)
    with pytest.raises(Exception):
        td.generate_dags(globals())

def test_kubernetes_pod_operator_dag():
    td = dagfactory.DagFactory(DAG_FACTORY_KUBERNETES_POD_OPERATOR)
    td.generate_dags(globals())
    assert "example_dag" in globals()

def test_variables_as_arguments_dag():
    override_command = 'value_from_variable'
    if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.10"):
        os.environ['AIRFLOW_VAR_VAR1'] = override_command
    else:
        Variable.set("var1",override_command)
    td = dagfactory.DagFactory(DAG_FACTORY_VARIABLES_AS_ARGUMENTS)
    td.generate_dags(globals())
    tasks = globals()['example_dag'].tasks
    for task in tasks:
        if task.task_id == "task_3":
            assert task.bash_command == override_command

def test_doc_md_file_path():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    td.generate_dags(globals())
    generated_doc_md = globals()['example_dag2'].doc_md
    with open(DOC_MD_FIXTURE_FILE,"r") as file:
        expected_doc_md = file.read()
    assert generated_doc_md == expected_doc_md

def test_doc_md_callable():
    td = dagfactory.DagFactory(TEST_DAG_FACTORY)
    td.generate_dags(globals())
    expected_doc_md = globals()['example_dag3'].doc_md
    assert str(td.get_dag_configs()['example_dag3']['doc_md_python_arguments']) == expected_doc_md