import os

from airflow.configuration import conf


def convert_to_boolean(value: str | None):
    value = str(value).lower().strip()
    if value in ("f", "false", "0", "", "none"):
        return False
    return True


enable_telemetry = conf.getboolean("dag_factory", "enable_telemetry", fallback=True)
do_not_track = convert_to_boolean(os.getenv("DO_NOT_TRACK"))
no_analytics = convert_to_boolean(os.getenv("SCARF_NO_ANALYTICS"))