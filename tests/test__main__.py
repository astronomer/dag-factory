import shutil
from filecmp import cmp
from pathlib import Path
from re import search
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

from dagfactory import __version__
from dagfactory.__main__ import app

EXAMPLE_YAML_AF2_DAGS = Path(__file__).parent.parent / "dev/dags/airflow2"
EXAMPLE_YAML_AF3_DAGS = Path(__file__).parent.parent / "dev/dags/airflow3"
EXAMPLE_YAML_INVALID_DAG = Path(__file__).parent.parent / "dev/dags/invalid.yaml"

runner = CliRunner()


@pytest.fixture
def tmp_yaml_file(tmp_path):
    file_path = tmp_path / "valid.yaml"
    file_path.write_text("key: value\n")
    return file_path


@pytest.fixture
def tmp_invalid_yaml_file(tmp_path):
    file_path = tmp_path / "invalid.yaml"
    file_path.write_text("key: [unclosed\n")
    return file_path


def write_yaml(path: Path, data: dict):
    with open(path, "w") as f:
        yaml.dump(data, f, sort_keys=False)


def test_version_option():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"DAG Factory {__version__}" in result.output


def test_help_output_when_no_command():
    result = runner.invoke(app, [])
    assert result.exit_code == 0
    assert "DAG Factory" in result.output
    # typer's exact "Usage:" formatting (brackets/ellipsis around COMMAND/ARGS, and whether
    # ANSI color codes split "Usage:" from "dagfactory") varies by version and environment,
    # so just check the pieces that are stable across both.
    assert "Usage" in result.output
    assert "dagfactory" in result.output
    assert "OPTIONS" in result.output
    assert "COMMAND" in result.output
    assert "ARGS" in result.output


def test_help_option():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Usage" in result.output
    assert "Show the version and exit" in result.output


def test_lint_path_not_exist():
    result = runner.invoke(app, ["lint", "nonexistent.yaml"])
    assert result.exit_code != 0
    assert "does not exist" in result.stdout


def test_lint_no_yaml_files(tmp_path):
    (tmp_path / "not_yaml.txt").write_text("hello")
    result = runner.invoke(app, ["lint", str(tmp_path)])
    assert result.exit_code == 0
    assert "No YAML files found" in result.stdout


def test_lint_valid_yaml(tmp_yaml_file):
    result = runner.invoke(app, ["lint", str(tmp_yaml_file)])
    assert result.exit_code == 0
    assert "no errors found" in result.stdout.lower()


def test_lint_exclude_single_file(tmp_yaml_file, tmp_path):
    ignore_file = tmp_path / "ignore.yaml"
    ignore_file.write_text("key: value\n")
    result = runner.invoke(app, ["lint", str(tmp_path), "--ignore", str(ignore_file)])
    assert result.exit_code == 0
    assert "Ignored 1 YAML file" in result.stdout
    assert "no errors found" in result.stdout.lower()


def test_lint_exlucde_single_file_with_airflowignore(tmp_yaml_file, tmp_path):
    airflowignore_yaml = tmp_path / "ignored.yaml"
    airflowignore_yaml.write_text("key: value\n")

    (tmp_path / ".airflowignore").write_text("ignored\n")

    dummy_ignore = tmp_path / "dummy.txt"
    dummy_ignore.write_text("noop")

    result = runner.invoke(app, ["lint", str(tmp_path), "--ignore", str(dummy_ignore)])
    assert result.exit_code == 0
    assert "Ignored 1 YAML file" in result.stdout
    assert "no errors found" in result.stdout.lower()


def test_lint_exclude_multiple_files(tmp_yaml_file, tmp_path):
    ignore_first_yaml = tmp_path / "first.yaml"
    ignore_first_yaml.write_text("key: value\n")
    ignore_second_yaml = tmp_path / "second.yaml"
    ignore_second_yaml.write_text("key: value\n")
    result = runner.invoke(app, ["lint", str(tmp_path), "--ignore", f"{ignore_first_yaml},{ignore_second_yaml}"])
    assert result.exit_code == 0
    assert "Ignored 2 YAML files" in result.stdout
    assert "no errors found" in result.stdout.lower()


def test_lint_exclude_multiple_files_with_airflowignore(tmp_yaml_file, tmp_path):
    ignore_first_yaml = tmp_path / "first.yaml"
    ignore_first_yaml.write_text("key: value\n")
    ignore_second_yaml = tmp_path / "second.yaml"
    ignore_second_yaml.write_text("key: value\n")

    airflowignore_third_yaml = tmp_path / "third.yaml"
    airflowignore_third_yaml.write_text("key: value\n")

    (tmp_path / ".airflowignore").write_text("third\n")

    result = runner.invoke(
        app,
        ["lint", str(tmp_path), "--ignore", f"{ignore_first_yaml},{ignore_second_yaml}"],
    )
    assert result.exit_code == 0
    assert "Ignored 3 YAML files" in result.stdout
    assert "no errors found" in result.stdout.lower()


@patch("dagfactory.__main__.Table.add_row")
def test_lint_invalid_yaml(mock_add_row, tmp_invalid_yaml_file):
    result = runner.invoke(app, ["lint", str(tmp_invalid_yaml_file)])
    assert result.exit_code == 1
    row = mock_add_row.call_args[0]
    assert "invalid.yaml" in row[0]
    assert "Syntax Error" in row[1].plain
    assert "while parsing a flow sequence" in row[2].plain
    assert "Analysed 1 files, found 1 invalid YAML files" in result.stdout
    assert len(row[2].plain) == 32  # Cropped error message


@patch("dagfactory.__main__.Table.add_row")
def test_lint_invalid_yaml_verbose(mock_add_row, tmp_invalid_yaml_file):
    result = runner.invoke(app, ["lint", str(tmp_invalid_yaml_file), "--verbose"])
    assert result.exit_code == 1
    row = mock_add_row.call_args[0]
    assert "invalid.yaml" in row[0]
    assert "Syntax Error" in row[1].plain
    assert "while parsing a flow sequence" in row[2].plain
    assert "Analysed 1 files, found 1 invalid YAML files" in result.stdout
    assert len(row[2].plain) == 200  # Full error message


def test_convert_diff_only(tmpdir):
    original_file = EXAMPLE_YAML_AF2_DAGS / "example_params.yml"
    converted_file = tmpdir / "test_dag.yaml"
    shutil.copy(original_file, converted_file)

    result = runner.invoke(app, ["convert", str(converted_file)])

    assert result.exit_code == 0

    assert "Tried to convert 1 file, converted 1 file, no errors found." in result.stdout

    assert "Diff for" in result.stdout
    assert "-  schedule_interval: '@daily'" in result.stdout
    assert "+  schedule: '@daily'" in result.stdout
    assert "-    operator: airflow.operators.bash.BashOperator" in result.stdout
    assert "+    operator: airflow.providers.standard.operators.bash.BashOperator" in result.stdout

    # check that the converted file remains unchanged
    assert cmp(original_file, converted_file, shallow=False)


def test_convert_override_writes_file(tmpdir):
    original_file = EXAMPLE_YAML_AF2_DAGS / "example_params.yml"
    converted_file = tmpdir / "example_params.yaml"
    shutil.copy(original_file, converted_file)

    result = runner.invoke(app, ["convert", str(converted_file), "--override"])

    assert result.exit_code == 0

    assert "Tried to convert 1 file, converted 1 file, no errors found." in result.stdout

    # check that the converted file changed
    assert not cmp(original_file, converted_file, shallow=False)

    converted_data = yaml.load(converted_file.read_text("utf-8"), Loader=yaml.FullLoader)

    assert "schedule_interval" not in converted_data["example_params"]
    assert converted_data["example_params"]["schedule"] == "@daily"
    assert (
        converted_data["example_params"]["tasks"][0]["operator"]
        == "airflow.providers.standard.operators.bash.BashOperator"
    )


def test_convert_no_changes(tmpdir):
    original_file = EXAMPLE_YAML_AF3_DAGS / "example_taskflow.yml"
    converted_file = tmpdir / "example_taskflow.yml"
    shutil.copy(original_file, converted_file)

    result = runner.invoke(app, ["convert", str(converted_file), "--override"])

    assert "Tried to convert 1 file, converted 0 files, no errors found." in result.stdout

    assert result.exit_code == 0
    assert cmp(original_file, converted_file, shallow=False)
    assert "No changes needed" in result.stdout


def test_convert_invalid_yaml(tmpdir):
    original_file = EXAMPLE_YAML_INVALID_DAG
    converted_file = tmpdir / "invalid.yml"
    shutil.copy(original_file, converted_file)

    result = runner.invoke(app, ["convert", str(converted_file), "--override"])

    assert "Tried to convert 1 file, converted 0 files, found 1 invalid YAML file." in result.stdout
    assert result.exit_code == 1
    assert cmp(original_file, converted_file, shallow=False)
    assert "Failed to convert" in result.stdout


def test_generate_help():

    result = runner.invoke(app, ["generate", "--help"])
    assert result.exit_code == 0
    assert search(r"\bpy_dags_dir\b", result.output)
    assert search(r"\byaml_file_dir\b", result.output)


def test_generate_missing_arguments():
    result = runner.invoke(app, ["generate"])
    assert result.exit_code != 0


def test_generate_input_dir_not_exist():
    result = runner.invoke(app, ["generate", "/nonexistent/yaml/dir", "/tmp/output"])
    assert result.exit_code != 0
    assert "'/nonexistent/yaml/dir'" in result.output
    assert "does not exist" in result.output


def test_generate_creates_output_dir_if_not_exist(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated/nested/new"

    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    assert output_dir.exists()


def test_generate_no_yaml_files(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"

    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    assert "No YAML files found" in result.output


def test_generate_creates_py_file(tmp_path):
    test_dag_content = """
        my_dag:
          schedule: "@daily"
          start_date: "2026-03-09"
          tasks:
            task_a:
              operator: "airflow.operators.bash.BashOperator"
              bash_command: "echo 'hello world'"
    """

    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()

    output_dir = tmp_path / "generated"

    (yaml_dir / "my_test_dag.yaml").write_text(test_dag_content)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    output_py_file = output_dir / "my_test_dag.py"
    assert result.exit_code == 0
    assert "my_dag" in result.output
    assert "generated successfully" in result.output
    assert output_py_file.exists()


def test_generate_py_file_has_content(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"

    (
        yaml_dir / "my_dag.yaml"
    ).write_text("""                                                                                                                                                                                                                           
        my_dag:
          schedule: "@daily"                                                                                                                                                                                                                                                    
          start_date: "2024-01-01"                                
          tasks:
            task_a:
              operator: airflow.operators.bash.BashOperator                                                                                                                                                                                                                     
              bash_command: "echo hello"
      """)

    runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])

    content = (output_dir / "my_dag.py").read_text()
    assert "from airflow" in content
    assert "my_dag" in content
    assert "Generated by dagfactory codegen" in content


def test_generate_tasks_with_decorator(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "my_dag.yaml").write_text("""
        my_dag:
          schedule: "@daily"
          start_date: "2024-01-01"
          tasks:
            - task_id: "task_a"
              decorator: "airflow.sdk.definitions.decorators.task"
              python_callable: "sample.some_number"
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    content = (output_dir / "my_dag.py").read_text()
    assert "from airflow.sdk.definitions.decorators import task" in content
    assert "from sample import some_number" in content
    assert "task_a = task(task_id='task_a', python_callable=some_number)()" in content


def test_generate_non_dag_yaml_is_ignored(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "datasets.yaml").write_text("""
datasets:
  - name: dataset_1
    uri: s3://bucket/dataset_1
  - name: dataset_2
    uri: s3://bucket/dataset_2
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    assert not any(output_dir.iterdir()) if output_dir.exists() else True


def test_generate_task_without_operator_or_decorator(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "my_dag.yaml").write_text("""
my_dag:
  schedule: "@daily"
  start_date: "2024-01-01"
  tasks:
    - task_id: "task_a"
      bash_command: "echo hello"
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    # A skipped DAG must fail the command — a CI pipeline must not silently ship fewer DAGs.
    assert result.exit_code == 1
    assert "Skipping DAG" in result.output
    assert "operator" in result.output or "decorator" in result.output


def test_generate_default_config_inherited_by_dags(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "my_dag.yaml").write_text("""
default:
  default_args:
    start_date: "2024-01-01"
  tasks:
    - task_id: "task_a"
      operator: "airflow.operators.bash.BashOperator"
      bash_command: "echo hello"

my_dag:
  schedule: "@daily"
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    assert "my_dag" in result.output
    assert "generated successfully" in result.output
    assert (output_dir / "my_dag.py").exists()


def test_generate_exits_zero_when_nothing_skipped(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "my_dag.yaml").write_text("""
my_dag:
  schedule: "@daily"
  start_date: "2024-01-01"
  tasks:
    - task_id: "task_a"
      operator: "airflow.operators.bash.BashOperator"
      bash_command: "echo hello"
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    assert (output_dir / "my_dag.py").exists()


def test_generate_ignores_shared_defaults_yml_file(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "defaults.yml").write_text("""
default_args:
  start_date: "2025-01-01"
  owner: "global_owner"
    """)
    (yaml_dir / "my_dag.yaml").write_text("""
my_dag:
  schedule: "@daily"
  tasks:
    task_a:
      operator: "airflow.operators.bash.BashOperator"
      bash_command: "echo hello"
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    # `defaults.yml` must not be treated as a DAG definition and generated on its own
    assert not (output_dir / "defaults.py").exists()
    assert (output_dir / "my_dag.py").exists()


def test_generate_merges_shared_defaults_yml_into_dags(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "defaults.yml").write_text("""
default_args:
  start_date: "2025-01-01"
  owner: "global_owner"
    """)
    (yaml_dir / "my_dag.yaml").write_text("""
my_dag:
  schedule: "@daily"
  tasks:
    task_a:
      operator: "airflow.operators.bash.BashOperator"
      bash_command: "echo hello"
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    content = (output_dir / "my_dag.py").read_text()
    assert 'pendulum.parse("2025-01-01T00:00:00+00:00")' in content
    assert "'owner': 'global_owner'" in content


def test_generate_tasks_as_list(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "my_dag.yaml").write_text("""
        my_dag:
          schedule: "@daily"
          start_date: "2024-01-01"
          tasks:
            - task_id: "task_a"
              operator: "airflow.operators.bash.BashOperator"
              bash_command: "echo hello"
            - task_id: "task_b"
              operator: "airflow.operators.bash.BashOperator"
              bash_command: "echo hello"
    """)
    result = runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    assert result.exit_code == 0
    assert "my_dag" in result.output
    assert "generated successfully" in result.output
    assert (output_dir / "my_dag.py").exists()


def test_generate_dag_params(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "my_dag.yaml").write_text("""
my_dag:
  schedule: "@daily"
  catchup: false
  start_date: "2024-01-01"
  tags:
    - example
    - test
  tasks:
    - task_id: "task_a"
      operator: "airflow.operators.bash.BashOperator"
      bash_command: "echo hello"
    """)
    runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    content = (output_dir / "my_dag.py").read_text()
    assert "schedule='@daily'" in content
    assert "catchup=False" in content
    assert 'start_date=pendulum.parse("2024-01-01T00:00:00+00:00")' in content
    assert "tags=" in content


def test_generate_schedule_as_asset_list(tmp_path):
    yaml_dir = tmp_path / "yamls"
    yaml_dir.mkdir()
    output_dir = tmp_path / "generated"
    (yaml_dir / "my_dag.yaml").write_text("""
my_dag:
  schedule:
    - "s3://bucket/dataset1.json"
    - "s3://bucket/dataset2.json"
  tasks:
    - task_id: "task_a"
      operator: "airflow.operators.bash.BashOperator"
      bash_command: "echo hello"
    """)
    runner.invoke(app, ["generate", str(yaml_dir), str(output_dir)])
    content = (output_dir / "my_dag.py").read_text()
    assert "from airflow.sdk import Asset" in content
    assert "Asset('s3://bucket/dataset1.json')" in content
    assert "Asset('s3://bucket/dataset2.json')" in content
