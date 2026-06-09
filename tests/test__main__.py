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
def tmp_valid_loader(tmp_path):
    """Write a tiny .py loader that registers a clean inline DAG via load_yaml_dags."""
    file_path = tmp_path / "valid_loader.py"
    file_path.write_text(
        """
from dagfactory import load_yaml_dags

load_yaml_dags(
    globals_dict=globals(),
    config_dict={
        "ok_dag": {
            "tasks": [{"task_id": "t", "operator": "airflow.operators.bash.BashOperator", "bash_command": "echo"}],
        }
    },
    defaults_config_dict={"default_args": {"start_date": "2025-01-01", "owner": "test"}},
)
"""
    )
    return file_path


@pytest.fixture
def tmp_invalid_loader(tmp_path):
    """Write a .py loader whose inline DAG uses a removed-in-AF3 parameter."""
    file_path = tmp_path / "invalid_loader.py"
    file_path.write_text(
        """
from dagfactory import load_yaml_dags

load_yaml_dags(
    globals_dict=globals(),
    config_dict={
        "bad_dag": {
            "schedule_interval": "@daily",
            "tasks": [{"task_id": "t", "operator": "x"}],
        }
    },
    defaults_config_dict={"default_args": {"start_date": "2025-01-01"}},
)
"""
    )
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
    result = runner.invoke(app, ["lint", "nonexistent.py"])
    assert result.exit_code != 0
    assert "does not exist" in result.stdout


def test_lint_no_python_files(tmp_path):
    (tmp_path / "not_python.txt").write_text("hello")
    result = runner.invoke(app, ["lint", str(tmp_path)])
    assert result.exit_code == 0
    assert "No lintable files found" in result.stdout


@patch("dagfactory.__main__.Table.add_row")
def test_lint_directory_filters_non_loaders(mock_add_row, tmp_path):
    """Directory with one loader and one unrelated .py — only the loader is linted."""
    (tmp_path / "loader.py").write_text(
        "from dagfactory import load_yaml_dags\n"
        "load_yaml_dags(globals_dict=globals(),\n"
        "               config_dict={'d': {'tasks': [{'task_id':'t','operator':'x'}]}},\n"
        "               defaults_config_dict={'default_args': {'start_date': '2025-01-01'}})\n"
    )
    (tmp_path / "utils.py").write_text("def helper():\n    return 42\n")
    # Use schema mode: the loader's `operator: x` is a fake path that build
    # mode would correctly reject; we're testing the directory-filter logic
    # here, not the build pipeline.
    result = runner.invoke(app, ["lint", "--schema-only", str(tmp_path)])
    assert result.exit_code == 0
    assert "Analysed 1 file(s)" in result.stdout
    # Only one row added — for loader.py, not utils.py.
    rows = [call.args[0] for call in mock_add_row.call_args_list]
    assert len(rows) == 1
    assert "loader.py" in rows[0]
    assert "utils.py" not in rows[0]


def test_lint_directory_no_dagfactory_imports(tmp_path):
    """Directory with .py files that don't import dagfactory — message tells user why."""
    (tmp_path / "a.py").write_text("import os\n")
    (tmp_path / "b.py").write_text("def f(): pass\n")
    result = runner.invoke(app, ["lint", str(tmp_path)])
    assert result.exit_code == 0
    assert "No lintable files found" in result.stdout
    assert "2 .py file(s) scanned" in result.stdout
    # Hint about the new flag should be shown when YAMLs aren't included.
    assert "--lint-yaml-in-dir" in result.stdout


@patch("dagfactory.__main__.Table.add_row")
def test_lint_directory_excludes_yaml_by_default(mock_add_row, tmp_path):
    """Directory walk lints only .py loaders by default — YAMLs are skipped."""
    (tmp_path / "loader.py").write_text(
        "from dagfactory import load_yaml_dags\n"
        "load_yaml_dags(globals_dict=globals(),\n"
        "               config_dict={'d': {'tasks': [{'task_id':'t','operator':'x'}]}},\n"
        "               defaults_config_dict={'default_args': {'start_date': '2025-01-01'}})\n"
    )
    (tmp_path / "config.yml").write_text(
        "my_dag:\n  default_args: {start_date: '2025-01-01'}\n  tasks: [{task_id: t, operator: x}]\n"
    )
    result = runner.invoke(app, ["lint", "--schema-only", str(tmp_path)])
    assert result.exit_code == 0
    rows = [call.args[0] for call in mock_add_row.call_args_list]
    assert len(rows) == 1
    assert "loader.py" in rows[0]
    assert "config.yml" not in rows[0]


@patch("dagfactory.__main__.Table.add_row")
def test_lint_directory_includes_yaml_with_flag(mock_add_row, tmp_path):
    """With --lint-yaml-in-dir, directory walk picks up .py loaders AND .yml files."""
    (tmp_path / "loader.py").write_text(
        "from dagfactory import load_yaml_dags\n"
        "load_yaml_dags(globals_dict=globals(),\n"
        "               config_dict={'d': {'tasks': [{'task_id':'t','operator':'x'}]}},\n"
        "               defaults_config_dict={'default_args': {'start_date': '2025-01-01'}})\n"
    )
    (tmp_path / "config.yml").write_text(
        "my_dag:\n  default_args: {start_date: '2025-01-01'}\n  tasks: [{task_id: t, operator: x}]\n"
    )
    result = runner.invoke(app, ["lint", "--schema-only", "--lint-yaml-in-dir", str(tmp_path)])
    assert result.exit_code == 0
    rows = [call.args[0] for call in mock_add_row.call_args_list]
    assert len(rows) == 2
    rendered = " ".join(rows)
    assert "loader.py" in rendered
    assert "config.yml" in rendered


def test_lint_single_non_loader_py_is_skipped(tmp_path):
    """Single explicit .py target that doesn't import dagfactory — visible skip."""
    target = tmp_path / "utils.py"
    target.write_text("def helper():\n    return 42\n")
    result = runner.invoke(app, ["lint", str(target)])
    assert result.exit_code == 0
    assert "does not import dagfactory" in result.stdout


def test_lint_yaml_file_is_validated(tmp_path):
    """A .yml target is now accepted and validated (no defaults applied)."""
    yml = tmp_path / "dag.yml"
    yml.write_text(
        "my_dag:\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    # Schema mode — the placeholder `operator: x` would correctly fail real
    # build, but we just want to confirm the YAML lint path works.
    result = runner.invoke(app, ["lint", "--schema-only", str(yml)])
    assert result.exit_code == 0
    assert "no errors found" in result.stdout.lower()


def test_lint_yaml_content_via_option(tmp_path):
    """Inline --yaml-content is accepted and validated."""
    yml = (
        "my_dag:\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    result = runner.invoke(app, ["lint", "--schema-only", "--yaml-content", yml])
    assert result.exit_code == 0
    assert "no errors found" in result.stdout.lower()


def test_lint_rejects_path_and_yaml_content_together(tmp_path):
    """Mutually exclusive: providing both should error."""
    yml = tmp_path / "x.yml"
    yml.write_text("a: b\n")
    result = runner.invoke(app, ["lint", str(yml), "--yaml-content", "k: v"])
    assert result.exit_code != 0
    assert "either a path argument or --yaml-content" in result.stdout


def test_lint_rejects_unknown_suffix(tmp_path):
    """Files that aren't .py / .yml / .yaml are rejected with a clear message."""
    p = tmp_path / "notes.txt"
    p.write_text("hello")
    result = runner.invoke(app, ["lint", str(p)])
    assert result.exit_code != 0
    assert ".py loader files or .yml/.yaml configs" in result.stdout


def test_lint_valid_loader(tmp_valid_loader):
    result = runner.invoke(app, ["lint", str(tmp_valid_loader)])
    assert result.exit_code == 0
    assert "no errors found" in result.stdout.lower()


@patch("dagfactory.__main__.Table.add_row")
def test_lint_invalid_loader(mock_add_row, tmp_invalid_loader):
    result = runner.invoke(app, ["lint", str(tmp_invalid_loader)])
    assert result.exit_code == 1
    # Find the row whose status is "Error"
    error_rows = [
        call.args
        for call in mock_add_row.call_args_list
        if hasattr(call.args[1], "plain") and call.args[1].plain == "Error"
    ]
    assert error_rows, "expected at least one Error row"
    row = error_rows[0]
    assert "invalid_loader" in row[0]
    assert "schedule_interval" in row[2].plain
    assert "Analysed 1 file(s), found 1 file(s) with errors" in result.stdout


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


