from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from dagfactory import __version__
from dagfactory.__main__ import app

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
