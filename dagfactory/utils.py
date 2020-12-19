"""Module contains various utilities used by dag-factory"""
import importlib.util
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AnyStr, Dict, Match, Optional, Pattern, Union

import pendulum


def get_datetime(
    date_value: Union[str, datetime, date], timezone: str = "UTC"
) -> datetime:
    """
    Takes value from DAG config and generates valid datetime. Defaults to
    today, if not a valid date or relative time (1 hours, 1 days, etc.)

    :param date_value: either a datetime (or date) or a relative time as string
    :type date_value: Uniont[datetime, date, str]
    :param timezone: string value representing timezone for the DAG
    :type timezone: str
    :returns: datetime for date_value
    :type: datetime.datetime
    """
    try:
        local_tz: pendulum.timezone = pendulum.timezone(timezone)
    except Exception as err:
        raise "Failed to create timezone" from err
    if isinstance(date_value, datetime):
        return date_value.replace(tzinfo=local_tz)
    if isinstance(date_value, date):
        return datetime.combine(date=date_value, time=datetime.min.time()).replace(
            tzinfo=local_tz
        )
    rel_delta: timedelta = get_time_delta(date_value)
    now: datetime = (
        datetime.today()
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .replace(tzinfo=local_tz)
    )
    if not rel_delta:
        return now
    return now - rel_delta


def get_time_delta(time_string: str) -> timedelta:
    """
    Takes a time string (1 hours, 10 days, etc.) and returns
    a python timedelta object

    :param time_string: the time value to convert to a timedelta
    :type time_string: str
    :returns: datetime.timedelta for relative time
    :type datetime.timedelta
    """
    # pylint: disable=line-too-long
    rel_time: Pattern = re.compile(
        pattern=r"((?P<hours>\d+?)\s+hour)?((?P<minutes>\d+?)\s+minute)?((?P<seconds>\d+?)\s+second)?((?P<days>\d+?)\s+day)?",
        # noqa
        flags=re.IGNORECASE,
    )
    parts: Optional[Match[AnyStr]] = rel_time.match(string=time_string)
    if not parts:
        raise Exception(f"Invalid relative time: {time_string}")
    # https://docs.python.org/3/library/re.html#re.Match.groupdict
    parts: Dict[str, str] = parts.groupdict()
    time_params = {}
    if all(value is None for value in parts.values()):
        raise Exception(f"Invalid relative time: {time_string}")
    for time_unit, magnitude in parts.items():
        if magnitude:
            time_params[time_unit]: int = int(magnitude)
    return timedelta(**time_params)


def merge_configs(
    config: Dict[str, Any], default_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Merges a `default` config with DAG config. Used to set default values
    for a group of DAGs.

    :param config: config to merge in default values
    :type config:  Dict[str, Any]
    :param default_config: config to merge default values from
    :type default_config: Dict[str, Any]
    :returns: dict with merged configs
    :type: Dict[str, Any]
    """
    for key in default_config:
        if key in config:
            if isinstance(config[key], dict) and isinstance(default_config[key], dict):
                merge_configs(config[key], default_config[key])
        else:
            config[key]: Any = default_config[key]
    return config


def get_python_callable(python_callable_name, python_callable_file):
    """
    Uses python filepath and callable name to import a valid callable
    for use in PythonOperator.

    :param python_callable_name: name of python callable to be imported
    :type python_callable_name:  str
    :param python_callable_file: aboslute path of python file with callable
    :type python_callable_file: str
    :returns: python calllable
    :type: callable
    """

    if not os.path.isabs(python_callable_file):
        raise Exception("`python_callable_file` must be absolute path")

    python_file_path = Path(python_callable_file).resolve()
    module_name = python_file_path.stem
    spec = importlib.util.spec_from_file_location(module_name, python_callable_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_name] = module
    python_callable = getattr(module, python_callable_name)

    return python_callable


def check_dict_key(item_dict: Dict[str, Any], key: str) -> bool:
    """
    Check if the key is included in given dictionary, and has a valid value.

    :param item_dict: a dictionary to test
    :type item_dict: Dict[str, Any]
    :param key: a key to test
    :type key: str
    :return: result to check
    :type: bool
    """
    return bool(key in item_dict and item_dict[key] is not None)
