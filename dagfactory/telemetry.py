from __future__ import annotations

import logging
import platform
from urllib.parse import urlencode

import httpx
from airflow import __version__ as airflow_version

import dagfactory
from dagfactory import constants, settings


def should_emit() -> bool:
    """
    Identify if telemetry metrics should be emitted or not.
    """
    return settings.enable_telemetry and not settings.do_not_track and not settings.no_analytics


def collect_standard_usage_metrics() -> dict[str, object]:
    """
    Return standard telemetry metrics.
    """
    metrics = {
        "dagfactory_version": dagfactory.__version__,
        "airflow_version": airflow_version,
        "python_version": platform.python_version(),
        "platform_system": platform.system(),
        "platform_machine": platform.machine(),
        "variables": {},
    }
    return metrics


def emit_usage_metrics(metrics: dict[str, object]) -> bool:
    """
    Emit desired telemetry metrics to remote telemetry endpoint.

    The metrics must contain the necessary fields to build the TELEMETRY_URL.
    """
    query_string = urlencode(metrics)
    telemetry_url = constants.TELEMETRY_URL.format(
        **metrics, telemetry_version=constants.TELEMETRY_VERSION, query_string=query_string
    )
    logging.debug("Telemetry is enabled. Emitting the following usage metrics to %s: %s", telemetry_url, metrics)
    response = httpx.get(telemetry_url, timeout=constants.TELEMETRY_TIMEOUT)
    if not response.is_success:
        logging.warning(
            "Unable to emit usage metrics to %s. Status code: %s. Message: %s",
            telemetry_url,
            response.status_code,
            response.text,
        )
    return response.is_success


def emit_usage_metrics_if_enabled(event_type: str, additional_metrics: dict[str, object]) -> bool:
    """
    Checks if telemetry should be emitted, fetch standard metrics, complement with custom metrics
    and emit them to remote telemetry endpoint.

    :returns: If the event was successfully sent to the telemetry backend or not.
    """
    if should_emit():
        metrics = collect_standard_usage_metrics()
        metrics["type"] = event_type
        metrics["variables"].update(additional_metrics)
        is_success = emit_usage_metrics(metrics)
        return is_success
    else:
        logging.debug("Telemetry is disabled. To enable it, export AIRFLOW__DAG_FACTORY__ENABLE_TELEMETRY=True.")
        return False
