"""Module contains various utilities used by dag-factory"""

import ast
import importlib.util
import json
import logging
import os
import re
import sys
import types
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AnyStr, Dict, List, Match, Optional, Pattern, Tuple, Union

import pendulum
import yaml

from dagfactory.exceptions import DagFactoryException


def _import_from_string(class_path):
    """Dynamically import a class from a string."""
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Could not import '{class_path}': {e}")


def get_datetime(date_value: Union[str, datetime, date], timezone: str = "UTC") -> datetime:
    """
    Takes value from DAG config and generates valid datetime. Defaults to
    today, if not a valid date or relative time (1 hours, 1 days, etc.)

    :param date_value: either a datetime (or date), a date string or a relative time as string
    :type date_value: Uniont[datetime, date, str]
    :param timezone: string value representing timezone for the DAG
    :type timezone: str
    :returns: datetime for date_value
    :type: datetime.datetime
    """
    try:
        local_tz: pendulum.timezone = pendulum.timezone(timezone)
    except Exception as err:
        raise DagFactoryException("Failed to create timezone") from err
    if isinstance(date_value, datetime):
        return date_value.replace(tzinfo=local_tz)
    if isinstance(date_value, date):
        return datetime.combine(date=date_value, time=datetime.min.time()).replace(tzinfo=local_tz)
    # Try parsing as date string
    try:
        return pendulum.parse(date_value).replace(tzinfo=local_tz)
    except pendulum.parsing.exceptions.ParserError:
        # Try parsing as relative time string
        rel_delta: timedelta = get_time_delta(date_value)
        now: datetime = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=local_tz)
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
        raise DagFactoryException(f"Invalid relative time: {time_string}")
    # https://docs.python.org/3/library/re.html#re.Match.groupdict
    parts: Dict[str, str] = parts.groupdict()
    time_params = {}
    if all(value is None for value in parts.values()):
        raise DagFactoryException(f"Invalid relative time: {time_string}")
    for time_unit, magnitude in parts.items():
        if magnitude:
            time_params[time_unit]: int = int(magnitude)
    return timedelta(**time_params)


def merge_configs(config: Dict[str, Any], default_config: Dict[str, Any]) -> Dict[str, Any]:
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
    :param python_callable_file: absolute path of python file with callable
    :type python_callable_file: str
    :returns: python callable
    :type: callable
    """

    if not os.path.isabs(python_callable_file):
        raise DagFactoryException("`python_callable_file` must be absolute path")

    python_file_path = Path(python_callable_file).resolve()
    module_name = python_file_path.stem
    spec = importlib.util.spec_from_file_location(module_name, python_callable_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[module_name] = module
    python_callable = getattr(module, python_callable_name)

    return python_callable


def get_python_callable_lambda(lambda_expr):
    """
    Uses lambda expression in a string to create a valid callable
    for use in operators/sensors.

    :param lambda_expr: the lambda expression to be converted
    :type lambda_expr:  str
    :returns: python callable
    :type: callable
    """

    tree = ast.parse(lambda_expr)
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.Expr):
        raise DagFactoryException("`lambda_expr` must be a single lambda")
    # the second parameter below is used only for logging
    code = compile(tree, "lambda_expr_to_callable.py", "exec")
    python_callable = types.LambdaType(code.co_consts[0], {})

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


def convert_to_snake_case(input_string: str) -> str:
    """
    Converts the string to snake case if camel case.

    :param input_string: the string to be converted
    :type input_string: str
    :return: string converted to snake case
    :type: str
    """
    # pylint: disable=line-too-long
    # source: https://www.geeksforgeeks.org/python-program-to-convert-camel-case-string-to-snake-case/
    return "".join("_" + i.lower() if i.isupper() else i for i in input_string).lstrip("_")


def check_template_searchpath(template_searchpath: Union[str, List[str]]) -> bool:
    """
    Check if template_searchpath is valid
    :param template_searchpath: a list or str to test
    :type template_searchpath: Union[str, List[str]]
    :return: result to check
    :type: bool
    """
    if isinstance(template_searchpath, str):
        if not os.path.isabs(template_searchpath):
            raise DagFactoryException("template_searchpath must be absolute paths")
        if not os.path.isdir(template_searchpath):
            raise DagFactoryException("template_searchpath must be existing paths")
        return True
    if isinstance(template_searchpath, list):
        for path in template_searchpath:
            if not os.path.isabs(path):
                raise DagFactoryException("template_searchpath must be absolute paths")
            if not os.path.isdir(path):
                raise DagFactoryException("template_searchpath must be existing paths")
        return True
    return False


def get_expand_partial_kwargs(
    task_params: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Union[Dict[str, Any], Any]], Dict[str, Union[Dict[str, Any], Any]]]:
    """
    Getting expand and partial kwargs if existed from task_params
    :param task_params: a dictionary with original task params from yaml
    :type task_params: Dict[str, Any]
    :return: dictionaries with task parameters
    :type: Tuple[
    Dict[str, Any],
    Dict[str, Union[Dict[str, Any], Any]],
    Dict[str, Union[Dict[str, Any], Any]],
    """

    expand_kwargs: Dict[str, Union[Dict[str, Any], Any]] = {}
    partial_kwargs: Dict[str, Union[Dict[str, Any], Any]] = {}
    for expand_key, expand_value in task_params["expand"].items():
        expand_kwargs[expand_key] = expand_value
    # remove dag-factory specific parameter
    del task_params["expand"]
    if check_dict_key(task_params, "partial"):
        for partial_key, partial_value in task_params["partial"].items():
            partial_kwargs[partial_key] = partial_value
        # remove dag-factory specific parameter
        del task_params["partial"]
    return task_params, expand_kwargs, partial_kwargs


def is_partial_duplicated(partial_kwargs: Dict[str, Any], task_params: Dict[str, Any]) -> bool:
    """
    Check if there are duplicated keys in partial_kwargs and task_params
    :param partial_kwargs: a partial kwargs to check duplicates in
    :type partial_kwargs: Dict[str, Any]
    :param task_params: a task params kwargs to check duplicates
    :type task_params: Dict[str, Any]
    :return: is there are duplicates
    :type: bool
    """

    for key in partial_kwargs:
        task_duplicated_kwarg = task_params.get(key, None)
    if task_duplicated_kwarg is not None:
        raise DagFactoryException("Duplicated partial kwarg! It's already in task_params.")
    return False


def get_datasets_uri_yaml_file(file_path: str, datasets_filter: str) -> List[str]:
    """
    Retrieves the URIs of datasets from a YAML file based on a given filter.

    :param file_path: The path to the YAML file.
    :type file_path: str
    :param datasets_filter: A list of dataset names to filter the results.
    :type datasets_filter: List[str]
    :return: A list of dataset URIs that match the filter.
    :rtype: List[str]
    """
    try:
        with open(file_path, "r", encoding="UTF-8") as file:
            data = yaml.safe_load(file)

            datasets = data.get("datasets", [])
            datasets_result_uri = [
                dataset["uri"] for dataset in datasets if dataset["name"] in datasets_filter and "uri" in dataset
            ]
            return datasets_result_uri
    except FileNotFoundError:
        logging.error("Error: File '%s' not found.", file_path)
        raise


def get_datasets_map_uri_yaml_file(file_path: str, datasets_filter: str) -> Dict[str, str]:
    """
    Retrieves the URIs of datasets from a YAML file based on a given filter.

    :param file_path: The path to the YAML file.
    :type file_path: str
    :param datasets_filter: A list of dataset names to filter the results.
    :type datasets_filter: List[str]
    :return: A Dict of dataset URIs that match the filter.
    :rtype: Dict[str, str]
    """
    try:
        with open(file_path, "r", encoding="UTF-8") as file:
            data = yaml.safe_load(file)

            datasets = data.get("datasets", [])
            datasets_result_dict = {
                dataset["name"]: dataset["uri"]
                for dataset in datasets
                if dataset["name"] in datasets_filter and "uri" in dataset
            }
            return datasets_result_dict
    except FileNotFoundError:
        logging.error("Error: File '%s' not found.", file_path)
        raise


def extract_dataset_names(expression) -> List[str]:
    dataset_pattern = r"\b[a-zA-Z_][a-zA-Z0-9_]*\b"
    datasets = re.findall(dataset_pattern, expression)
    return datasets


def extract_storage_names(expression) -> List[str]:
    storage_pattern = r"[a-zA-Z][a-zA-Z0-9+.-]*://[a-zA-Z0-9\-_/\.]+"
    storages = re.findall(storage_pattern, expression)
    return storages


def make_valid_variable_name(uri) -> str:
    return re.sub(r"\W|^(?=\d)", "_", uri)


def parse_list_datasets(datasets: Union[List[str], str]) -> str:
    if isinstance(datasets, list):
        datasets = " & ".join(datasets)
    return datasets


def get_json_serialized_callable(data_obj):
    """
    Takes a dictionary object and returns a callable that
    returns JSON serialized string. If it's already a string,
    validates that it's valid JSON.

    :param data_obj: dict or JSON-formatted str
    :returns: callable returning JSON serialized str
    """
    if isinstance(data_obj, dict):
        serialized_json = json.dumps(data_obj)
    elif isinstance(data_obj, str):
        try:
            # Validate JSON string
            json.loads(data_obj)
            serialized_json = data_obj
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON provided: {e}")
    else:
        raise TypeError(f"data_obj must be a dict or str, not {type(data_obj)}")

    return lambda **kwargs: serialized_json


def update_yaml_structure(data):
    """Convert an Airflow 2-style YAML DAG config to be compatible with Airflow 3.

    This function updates the DAG YAML structure to match changes introduced in Airflow 3,
    such as replacing deprecated fields (e.g., 'schedule_interval' → 'schedule') and
    adjusting operator import paths to the new module layout.
    """
    operator_map = {
        "airflow.operators.dummy_operator.DummyOperator": "airflow.providers.standard.operators.empty.EmptyOperator",
        "airflow.operators.empty.EmptyOperator": "airflow.providers.standard.operators.empty.EmptyOperator",
        "airflow.operators.bash.BashOperator": "airflow.providers.standard.operators.bash.BashOperator",
        "airflow.operators.bash_operator.BashOperator": "airflow.providers.standard.operators.bash.BashOperator",
        "airflow.operators.python_operator.PythonOperator": "airflow.providers.standard.operators.python.PythonOperator",
        "airflow.operators.python.PythonOperator": "airflow.providers.standard.operators.python.PythonOperator",
        "airflow.sensors.external_task.ExternalTaskSensor": "airflow.providers.standard.sensors.external_task.ExternalTaskSensor",
        "airflow.sensors.external_task_sensor.ExternalTaskSensor": "airflow.providers.standard.sensors.external_task.ExternalTaskSensor",
        "airflow.decorators.task": "airflow.sdk.definitions.decorators.task",
    }
    if isinstance(data, dict):
        keys_to_update = []
        for key, value in data.items():
            # Recursively process nested dictionaries or lists
            if isinstance(value, (dict, list)):
                update_yaml_structure(value)

            # Mark keys to rename after loop (avoid modifying dict while iterating)
            if key == "schedule_interval":
                keys_to_update.append(("schedule_interval", "schedule"))
            if key == "operator":
                data[key] = operator_map[value] if operator_map.get(value) is not None else value

        # Perform renames after iteration
        for old_key, new_key in keys_to_update:
            data[new_key] = data.pop(old_key)

    elif isinstance(data, list):
        for item in data:
            update_yaml_structure(item)

    return data


def cast_with_type(data):
    """Recursively cast dictionaries with a __type__ key."""
    if isinstance(data, dict):
        # Handle typed list
        if data.get("__type__") == "builtins.list" and "items" in data:
            return [cast_with_type(item) for item in data["items"]]

        # Normal typed dict
        processed = {k: cast_with_type(v) for k, v in data.items() if k != "__type__"}

        if "__type__" in data:
            class_type = _import_from_string(data["__type__"])
            return class_type(**processed)

        return processed

    elif isinstance(data, list):
        return [cast_with_type(item) for item in data]

    return data
