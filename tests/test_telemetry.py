import logging
from unittest.mock import patch

import pytest

from dagfactory import telemetry


def test_should_emit_is_true_by_default():
    assert telemetry.should_emit()


@patch("dagfactory.settings.enable_telemetry", True)
def test_should_emit_is_true_when_only_enable_telemetry_is_true():
    assert telemetry.should_emit()


@patch("dagfactory.settings.do_not_track", True)
def test_should_emit_is_false_when_do_not_track():
    assert not telemetry.should_emit()


@patch("dagfactory.settings.no_analytics", True)
def test_should_emit_is_false_when_no_analytics():
    assert not telemetry.should_emit()


def test_collect_standard_usage_metrics():
    metrics = telemetry.collect_standard_usage_metrics()
    expected_keus = [
        "airflow_version",
        "dagfactory_version",
        "platform_machine",
        "platform_system",
        "python_version",
        "variables",
    ]
    assert sorted(metrics.keys()) == expected_keus


class MockFailedResponse:
    is_success = False
    status_code = "404"
    text = "Non existent URL"


@patch("dagfactory.telemetry.httpx.get", return_value=MockFailedResponse())
def test_emit_usage_metrics_fails(mock_httpx_get, caplog):
    sample_metrics = {
        "dagfactory_version": "0.2.0a1",
        "airflow_version": "2.10.1",
        "python_version": "3.11",
        "platform_system": "darwin",
        "platform_machine": "amd64",
        "variables": {"a": 1, "b": 2},
    }
    is_success = telemetry.emit_usage_metrics(sample_metrics)
    mock_httpx_get.assert_called_once_with(
        "https://astronomer.gateway.scarf.sh/dag-factory/v1/0.2.0a1/2.10.1/3.11/darwin/amd64?dagfactory_version=0.2.0a1&airflow_version=2.10.1&python_version=3.11&platform_system=darwin&platform_machine=amd64&variables=%7B%27a%27%3A+1%2C+%27b%27%3A+2%7D",
        timeout=5.0,
    )
    assert not is_success
    log_msg = "Unable to emit usage metrics to https://astronomer.gateway.scarf.sh/dag-factory/v1/0.2.0a1/2.10.1/3.11/darwin/amd64?dagfactory_version=0.2.0a1&airflow_version=2.10.1&python_version=3.11&platform_system=darwin&platform_machine=amd64&variables=%7B%27a%27%3A+1%2C+%27b%27%3A+2%7D. Status code: 404. Message: Non existent URL"
    assert caplog.text.startswith("WARNING")
    assert log_msg in caplog.text


@pytest.mark.integration
def test_emit_usage_metrics_succeeds(caplog):
    caplog.set_level(logging.DEBUG)
    sample_metrics = {
        "dagfactory_version": "0.2.0a1",
        "airflow_version": "2.10.1",
        "python_version": "3.11",
        "platform_system": "darwin",
        "platform_machine": "amd64",
        "variables": {"a": 1, "b": 2},
    }
    is_success = telemetry.emit_usage_metrics(sample_metrics)
    assert is_success
    assert caplog.text.startswith("DEBUG")
    assert "Telemetry is enabled. Emitting the following usage metrics to" in caplog.text


@patch("dagfactory.telemetry.should_emit", return_value=False)
def test_emit_usage_metrics_if_enabled_fails(mock_should_emit, caplog):
    caplog.set_level(logging.DEBUG)
    assert not telemetry.emit_usage_metrics_if_enabled("any", {})
    assert caplog.text.startswith("DEBUG")
    assert "Telemetry is disabled. To enable it, export AIRFLOW__DAG_FACTORY__ENABLE_TELEMETRY=True." in caplog.text


@patch("dagfactory.telemetry.should_emit", return_value=True)
@patch("dagfactory.telemetry.collect_standard_usage_metrics", return_value={"k1": "v1", "variables": {}})
@patch("dagfactory.telemetry.emit_usage_metrics")
def test_emit_usage_metrics_if_enabled_succeeds(
    mock_emit_usage_metrics, mock_collect_standard_usage_metrics, mock_should_emit
):
    assert telemetry.emit_usage_metrics_if_enabled("any", {"k2": "v2"})
    mock_emit_usage_metrics.assert_called_once()
    assert mock_emit_usage_metrics.call_args.args[0] == {"k1": "v1", "variables": {"k2": "v2"}, "type": "any"}
