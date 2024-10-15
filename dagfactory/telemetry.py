from __future__ import annotations

import logging
import platform
from urllib.parse import urlencode

import httpx
from airflow import __version__ as airflow_version
from packaging.version import parse

import dagfactory
from dagfactory import constants
from dagfactory import settings

CUSTOM_METRICS_KEY = "dagfactory"


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
        CUSTOM_METRICS_KEY: {
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
    telemetry_url = constants.TELEMETRY_URL.format(version=dagfactory.__version__) + f"?{query_string}"
    logging.debug(
        "Emitting the following usage metrics to %s: %s",
        telemetry_url,
        metrics
    )
    response = httpx.get(telemetry_url, timeout=constants.TELEMETRY_TIMEOUT)
    if not response.is_success:
        logging.warning(
            "Unable to emit usage metrics to %s. Status code: %s. Message: %s",
            telemetry_url,
            response.status_code,
            response.text
        )


def emit_usage_metrics_if_enabled(additional_metrics):
    """
    Checks if telemetry should be emitted, fetch standard metrics, complement with custom metrics
    and emit them to remote telemetry endpoint.
    """
    if should_emit():
        metrics = collect_standard_usage_metrics()
        metrics[CUSTOM_METRICS_KEY].update(additional_metrics)
        emit_usage_metrics(metrics)
        return True
    else:
        logging.debug(
            "Telemetry is disabled"
        )
        return False