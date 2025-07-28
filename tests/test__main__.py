from typer.testing import CliRunner

from dagfactory import __version__
from dagfactory.__main__ import app

runner = CliRunner()


def test_version_option():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"DAG Factory {__version__}" in result.output

    result_short = runner.invoke(app, ["-v"])
    assert result_short.exit_code == 0
    assert f"DAG Factory {__version__}" in result_short.output


def test_help_output_when_no_command():
    result = runner.invoke(app, [])
    assert result.exit_code == 0
    assert "DAG Factory" in result.output
    assert "--help" in result.output


def test_help_option():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Usage" in result.output
    assert "Show the version and exit" in result.output
