from __future__ import annotations

import os

from airflow.configuration import conf


def convert_to_boolean(value: str | None) -> bool:
    """
    Convert a string that represents a boolean to a Python boolean.
    """
    value = str(value).lower().strip()
    if value in ("f", "false", "0", "", "none"):
        return False
    return True


enable_telemetry = conf.getboolean("dag_factory", "enable_telemetry", fallback=True)
do_not_track = convert_to_boolean(os.getenv("DO_NOT_TRACK"))
no_analytics = convert_to_boolean(os.getenv("SCARF_NO_ANALYTICS"))
strict_mode = conf.getboolean("dag_factory", "strict_mode", fallback=False)

# Report parameter problems while building. This controls reporting only:
# a parameter the installed Airflow does not accept is dropped either way,
# because _build_dag_kwargs consults the same metadata to decide what to
# forward. Findings are logged; set strict_mode to turn errors fatal.
validate_on_build = conf.getboolean("dag_factory", "validate_on_build", fallback=True)
