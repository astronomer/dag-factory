import shutil
from filecmp import cmp
from pathlib import Path
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
    assert "dagfactory [OPTIONS] COMMAND [ARGS]" in result.output


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


def test_lint_exclude_multiple_files(tmp_yaml_file, tmp_path):
    ignore_first_yaml = tmp_path / "first.yaml"
    ignore_first_yaml.write_text("key: value\n")
    ignore_second_yaml = tmp_path / "second.yaml"
    ignore_second_yaml.write_text("key: value\n")
    result = runner.invoke(app, ["lint", str(tmp_path), "--ignore", f"{ignore_first_yaml},{ignore_second_yaml}"])
    assert result.exit_code == 0
    assert "Ignored 2 YAML files" in result.stdout
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
