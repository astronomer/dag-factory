from __future__ import annotations

import platform
from urllib.parse import urlencode

import httpx
from airflow import __version__ as airflow_version
from packaging.version import parse

import dagfactory
from dagfactory import constants
from dagfactory import settings


def should_emit() -> bool:
    """
    Identify if telemetry metrics should be emitted or not.
    """
    return settings.enable_telemetry and not parse(dagfactory.__version__).is_prerelease


def collect_standard_usage_metrics() -> dict[str, object]:
    """
    Return standard telemetry metrics.
    """
    metrics = {
        "dagfactory": {
            "version": dagfactory.__version__
        },
        "system": {
            "airflow_version": airflow_version,
            "platform_system": platform.system(),
            "platform_machine": platform.machine(),
            "python_version": platform.python_version(),

        }
    }
    return metrics


def emit_usage_metrics(metrics):
    """
    Emit desired telemetry metrics to remote telemetry endpoint.
    """
    query_string = urlencode(metrics)
    scarf_url = f"{constants.TELEMETRY_URL}?{query_string}"
    httpx.get(scarf_url, timeout=constants.TELEMETRY_TIMEOUT)


def emit_usage_metrics_if_enabled(additional_metrics):
    """
    Checks if telemetry should be emitted, fetch standard metrics, complement with custom metrics
    and emit them to remote telemetry endpoint.
    """
    if should_emit():
        metrics = collect_standard_usage_metrics()
        metrics["dagfactory"].update(additional_metrics)
        emit_usage_metrics(metrics)
