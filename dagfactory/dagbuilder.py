"""Module contains code for generating tasks and constructing a DAG"""

from __future__ import annotations

import ast
import inspect
import logging
import os
import re
import warnings
from copy import deepcopy
from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, List, Tuple, Union

from airflow import configuration
from packaging import version

from dagfactory.utils import check_dict_key

try:
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.variable import Variable
except ImportError:
    from airflow.models import BaseOperator, Variable
    from airflow.models.dag import DAG

from airflow.datasets import Dataset
from airflow.models import MappedOperator
from airflow.utils.module_loading import import_string
from airflow.utils.task_group import TaskGroup
from airflow.version import version as AIRFLOW_VERSION

try:  # Try Airflow 3
    from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
    from airflow.providers.standard.sensors.python import PythonSensor
except ImportError:
    from airflow.operators.python import BranchPythonOperator, PythonOperator
    from airflow.sensors.python import PythonSensor


logger = logging.getLogger(__name__)

# Try to import HttpOperator and HttpSensor only if the package is installed
try:
    from airflow.providers.http.operators.http import HttpOperator
    from airflow.providers.http.sensors.http import HttpSensor

    HTTP_OPERATOR_CLASS = HttpOperator
    HTTP_SENSOR_CLASS = HttpSensor
except ImportError:  # pragma: no cover
    try:
        # TODO: Remove this when apache-airflow-providers-http >= 5.0.0
        from airflow.providers.http.operators.http import SimpleHttpOperator
        from airflow.providers.http.sensors.http import HttpSensor

        HTTP_OPERATOR_CLASS = SimpleHttpOperator
        HTTP_SENSOR_CLASS = HttpSensor
    except ImportError:  # pragma: no cover
        HTTP_OPERATOR_CLASS = None
        HTTP_SENSOR_CLASS = None
        logger.info("Package apache-airflow-providers-http is not installed.")

# Try to import SqlSensor only if the package is installed
try:
    from airflow.providers.common.sql.sensors.sql import SqlSensor

    SQL_SENSOR_CLASS = SqlSensor
except ImportError:
    logger.info("Package apache-airflow-providers-common-sql is not installed.")
    SQL_SENSOR_CLASS = None

from dagfactory import parsers, utils
from dagfactory.constants import AIRFLOW3_MAJOR_VERSION
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException

# these are params only used in the DAG factory, not in the tasks
SYSTEM_PARAMS: List[str] = ["operator", "dependencies", "task_group_name", "parent_group_name"]
INSTALLED_AIRFLOW_VERSION = version.parse(AIRFLOW_VERSION)


class DagBuilder:
    """
    Generates tasks and a DAG from a config.

    :param dag_name: the name of the DAG
    :param dag_config: a dictionary containing configuration for the DAG
    :param default_config: a dictionary containing defaults for all DAGs
        in the YAML file
    """

    def __init__(
        self, dag_name: str, dag_config: Dict[str, Any], default_config: Dict[str, Any], yml_dag: str = ""
    ) -> None:
        self.dag_name: str = dag_name
        self.dag_config: Dict[str, Any] = deepcopy(dag_config)
        self.default_config: Dict[str, Any] = deepcopy(default_config)
        self._yml_dag = yml_dag

    # pylint: disable=too-many-branches,too-many-statements
    def get_dag_params(self) -> Dict[str, Any]:
        """
        Merges default config with dag config, sets dag_id, and extropolates dag_start_date

        :returns: dict of dag parameters
        """
        dag_params: Dict[str, Any] = utils.merge_configs(self.dag_config, self.default_config)
        dag_params["dag_id"]: str = self.dag_name

        # If there are no default_args, add an empty dictionary
        dag_params["default_args"] = {} if "default_args" not in dag_params else dag_params["default_args"]

        if utils.check_dict_key(dag_params, "start_date"):
            dag_params["start_date"]: datetime = utils.get_datetime(
                date_value=dag_params["start_date"],
                timezone=dag_params.get("timezone", "UTC"),
            )

        if utils.check_dict_key(dag_params, "end_date"):
            dag_params["end_date"]: datetime = utils.get_datetime(
                date_value=dag_params["end_date"],
                timezone=dag_params.get("timezone", "UTC"),
            )

        # Convert from 'end_date: Union[str, datetime, date]' to 'end_date: datetime'
        if utils.check_dict_key(dag_params["default_args"], "end_date"):
            dag_params["default_args"]["end_date"]: datetime = utils.get_datetime(
                date_value=dag_params["default_args"]["end_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )

        # Parse callbacks at the DAG-level and at the Task-level, configured in default_args. Note that the version
        # check has gone into the set_callback method
        for callback_type in [
            "on_execute_callback",
            "on_success_callback",
            "on_failure_callback",
            "on_retry_callback",  # Not applicable at the DAG-level
            "on_skipped_callback",  # Not applicable at the DAG-level
            "sla_miss_callback",  # Not applicable at the default_args level
        ]:
            if callback_type == "sla_miss_callback" and version.parse(AIRFLOW_VERSION) >= version.parse("3.1.0"):
                # sla_miss_callbacks are removed as of 3.1.0
                logger.info("The sla_miss_callback has been removed in Airflow 3.1.0.")
                continue

            # Here, we are parsing both the DAG-level params and default_args for callbacks. Previously, this was
            # copy-and-pasted for each callback type and each configuration option (via a string import, function
            # defined via YAML, or file path and name

            # First, check at the DAG-level for just the single field (via a string or via a provider callback that
            # takes parameters). Since "on_retry_callback" and "on_skipped_callback" is only applicable at the
            # Task-level, we are skipping that callback type here.
            if callback_type not in ("on_retry_callback", "on_skipped_callback"):
                if utils.check_dict_key(dag_params, callback_type):
                    dag_params[callback_type]: Callable = self.set_callback(
                        parameters=dag_params, callback_type=callback_type
                    )

                # Then, check at the DAG-level for a file path and name
                if utils.check_dict_key(dag_params, f"{callback_type}_name") and utils.check_dict_key(
                    dag_params, f"{callback_type}_file"
                ):
                    dag_params[callback_type] = self.set_callback(
                        parameters=dag_params, callback_type=callback_type, has_name_and_file=True
                    )

            # SLAs are defined at the DAG-level, and will be applied to every task.
            # https://www.astronomer.io/docs/learn/error-notifications-in-airflow/. Here, we are not going to add
            # callbacks for sla_miss_callback, or on_skipped_callback if the Airflow version is less than 2.7.0
            if (callback_type != "sla_miss_callback") or not (
                callback_type == "on_skipped_callback" and version.parse(AIRFLOW_VERSION) < version.parse("2.7.0")
            ):
                # Next, check for a callback at the Task-level using default_args
                if utils.check_dict_key(dag_params["default_args"], callback_type):
                    dag_params["default_args"][callback_type]: Callable = self.set_callback(
                        parameters=dag_params["default_args"], callback_type=callback_type
                    )

                # Finally, check for file path and name at the Task-level using default_args
                if utils.check_dict_key(dag_params["default_args"], f"{callback_type}_name") and utils.check_dict_key(
                    dag_params["default_args"], f"{callback_type}_file"
                ):
                    dag_params["default_args"][callback_type] = self.set_callback(
                        parameters=dag_params["default_args"], callback_type=callback_type, has_name_and_file=True
                    )

        if utils.check_dict_key(dag_params, "template_searchpath"):
            if isinstance(dag_params["template_searchpath"], (list, str)) and utils.check_template_searchpath(
                dag_params["template_searchpath"]
            ):
                dag_params["template_searchpath"]: Union[str, List[str]] = dag_params["template_searchpath"]
            else:
                raise DagFactoryException("template_searchpath is not valid!")

        if utils.check_dict_key(dag_params, "render_template_as_native_obj"):
            if isinstance(dag_params["render_template_as_native_obj"], bool):
                dag_params["render_template_as_native_obj"]: bool = dag_params["render_template_as_native_obj"]
            else:
                raise DagFactoryException("render_template_as_native_obj should be bool type!")

        if check_dict_key(dag_params["default_args"], "start_date"):
            dag_params["default_args"]["start_date"]: datetime = utils.get_datetime(
                date_value=dag_params["default_args"]["start_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )

        return dag_params

    @staticmethod
    def _handle_http_sensor(operator_obj, task_params):
        # Only handle if HttpOperator/HttpSensor are available
        if HTTP_OPERATOR_CLASS and issubclass(operator_obj, HTTP_OPERATOR_CLASS):
            headers = task_params.get("headers", {})
            content_type = headers.get("Content-Type", "").lower()

            if "data" in task_params and "application/json" in content_type:
                task_params["data"]: Callable = utils.get_json_serialized_callable(task_params["data"])

                if "Content-Type" not in headers:
                    headers["Content-Type"] = "application/json"
                task_params["headers"] = headers
        elif HTTP_SENSOR_CLASS and issubclass(operator_obj, HTTP_SENSOR_CLASS):
            response_check_name = task_params.pop("response_check_name", None)
            response_check_file = task_params.pop("response_check_file", None)
            response_check_lambda = task_params.pop("response_check_lambda", None)
            if response_check_name:
                task_params["response_check"]: Callable = utils.get_python_callable(
                    response_check_name, response_check_file
                )
            elif response_check_lambda:
                task_params["response_check"]: Callable = utils.get_python_callable_lambda(response_check_lambda)
        return task_params

    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-locals
    @staticmethod
    def make_task(operator: str, task_params: Dict[str, Any]) -> BaseOperator:
        """
        Takes an operator and params and creates an instance of that operator.

        :returns: instance of operator object
        """
        # class is a Callable https://stackoverflow.com/a/34578836/3679900
        operator_obj: Callable[..., BaseOperator] = import_string(operator)
        # pylint: disable=too-many-nested-blocks
        if issubclass(operator_obj, (PythonOperator, BranchPythonOperator, PythonSensor)):
            if (
                not task_params.get("python_callable")
                and not task_params.get("python_callable_name")
                and not task_params.get("python_callable_file")
            ):
                # pylint: disable=line-too-long
                raise DagFactoryException(
                    "Failed to create task. PythonOperator, BranchPythonOperator and PythonSensor requires \
                    `python_callable_name` and `python_callable_file` "
                    "parameters.\nOptionally you can load python_callable "
                    "from a file. with the special pyyaml notation:\n"
                    "  python_callable_file: !!python/name:my_module.my_func"
                )
            if not task_params.get("python_callable"):
                task_params["python_callable"]: Callable = utils.get_python_callable(
                    task_params["python_callable_name"], task_params["python_callable_file"]
                )
                # remove dag-factory specific parameters
                # Airflow 2.0 doesn't allow these to be passed to operator
                del task_params["python_callable_name"]
                del task_params["python_callable_file"]
            elif isinstance(task_params["python_callable"], str):
                task_params["python_callable"]: Callable = import_string(task_params["python_callable"])

        # Check for the custom success and failure callables in SqlSensor. These are considered
        # optional, so no failures in case they aren't found. Note: there's no reason to
        # declare both a callable file and a lambda function for success/failure parameter.
        # If both are found the object will not throw and error, instead callable file will
        # take precedence over the lambda function
        if SQL_SENSOR_CLASS and issubclass(operator_obj, SQL_SENSOR_CLASS):
            # Success checks
            if task_params.get("success_check_file") and task_params.get("success_check_name"):
                task_params["success"]: Callable = utils.get_python_callable(
                    task_params["success_check_name"], task_params["success_check_file"]
                )
                del task_params["success_check_name"]
                del task_params["success_check_file"]
            elif task_params.get("success_check_lambda"):
                task_params["success"]: Callable = utils.get_python_callable_lambda(task_params["success_check_lambda"])
                del task_params["success_check_lambda"]
            # Failure checks
            if task_params.get("failure_check_file") and task_params.get("failure_check_name"):
                task_params["failure"]: Callable = utils.get_python_callable(
                    task_params["failure_check_name"], task_params["failure_check_file"]
                )
                del task_params["failure_check_name"]
                del task_params["failure_check_file"]
            elif task_params.get("failure_check_lambda"):
                task_params["failure"]: Callable = utils.get_python_callable_lambda(task_params["failure_check_lambda"])
                del task_params["failure_check_lambda"]

        # Only handle HTTP operator/sensor if the package is installed
        if (HTTP_OPERATOR_CLASS or HTTP_SENSOR_CLASS) and issubclass(
            operator_obj, (HTTP_OPERATOR_CLASS, HTTP_SENSOR_CLASS)
        ):
            task_params = DagBuilder._handle_http_sensor(operator_obj, task_params)

        DagBuilder.adjust_general_task_params(task_params)

        expand_kwargs: Dict[str, Union[Dict[str, Any], Any]] = {}
        if utils.check_dict_key(task_params, "expand") or utils.check_dict_key(task_params, "partial"):
            # Getting expand and partial kwargs from task_params
            (task_params, expand_kwargs, partial_kwargs) = utils.get_expand_partial_kwargs(task_params)

            # If there are partial_kwargs we should merge them with existing task_params
            if partial_kwargs and not utils.is_partial_duplicated(partial_kwargs, task_params):
                task_params.update(partial_kwargs)

        task: Union[BaseOperator, MappedOperator] = (
            operator_obj(**task_params)
            if not expand_kwargs
            else operator_obj.partial(**task_params).expand(**expand_kwargs)
        )
        return task

    @staticmethod
    def make_task_groups(task_groups: Dict[str, Any], dag: DAG) -> Dict[str, "TaskGroup"]:
        """Takes a DAG and task group configurations. Creates TaskGroup instances.

        :param task_groups: Task group configuration from the YAML configuration file.
        :param dag: DAG instance that task groups to be added.
        """
        task_groups_dict: Dict[str, "TaskGroup"] = {}
        for task_group_name, task_group_conf in task_groups.items():
            DagBuilder.make_nested_task_groups(
                task_group_name, task_group_conf, task_groups_dict, task_groups, None, dag
            )

        return task_groups_dict

    @staticmethod
    def _init_task_group_callback_param(task_group_conf):
        """
        _init_task_group_callback_param

        Handle configuring callbacks for TaskGroups in this method in this helper-method

        :param task_group_conf: dict containing the configuration of the TaskGroup
        """
        # The Airflow version needs to be at least 2.2.0, and default args must be present. Basically saying here: if
        # it's not the case that we're using at least Airflow 2.2.0 and default_args are present, then return the
        # TaskGroup configuration without doing anything
        if not (
            version.parse(AIRFLOW_VERSION) >= version.parse("2.2.0")
            and isinstance(task_group_conf.get("default_args"), dict)
        ):
            return task_group_conf

        # Check the callback types that can be in the default_args of the TaskGroup
        for callback_type in [
            "on_execute_callback",
            "on_success_callback",
            "on_failure_callback",
            "on_retry_callback",
            "on_skipped_callback",  # This is only available AIRFLOW_VERSION >= 2.7.0
        ]:
            # on_skipped_callback can only be added to the default_args of a TaskGroup for AIRFLOW_VERSION >= 2.7.0
            if callback_type == "on_skipped_callback" and version.parse(AIRFLOW_VERSION) < version.parse("2.7.0"):
                continue

            # First, check for a str, str with params, or provider callback
            if utils.check_dict_key(task_group_conf["default_args"], callback_type):
                task_group_conf["default_args"][callback_type]: Callable = DagBuilder.set_callback(
                    parameters=task_group_conf["default_args"], callback_type=callback_type
                )

            # Then, check for a file path and name
            if utils.check_dict_key(task_group_conf["default_args"], f"{callback_type}_name") and utils.check_dict_key(
                task_group_conf["default_args"], f"{callback_type}_file"
            ):
                task_group_conf["default_args"][callback_type] = DagBuilder.set_callback(
                    parameters=task_group_conf["default_args"],
                    callback_type=callback_type,
                    has_name_and_file=True,
                )

        return task_group_conf

    @staticmethod
    def make_nested_task_groups(
        task_group_name: str,
        task_group_conf: Any,
        task_groups_dict: Dict[str, "TaskGroup"],
        task_groups: Dict[str, Any],
        circularity_check_queue: List[str] | None,
        dag: DAG,
    ):
        """Takes a DAG and task group configurations. Creates nested TaskGroup instances.
        :param task_group_name: The name of the task group to be created
        :param task_group_conf: Configuration details for the task group, which may include parent group information.
        :param task_groups_dict: A dictionary where the created TaskGroup instances are stored, keyed by task group name.
        :param task_groups: Task group configuration from the YAML configuration file.
        :param circularity_check_queue: A list used to track the task groups being processed to detect circular dependencies.
        :param dag: DAG instance that task groups to be added.
        """
        if task_group_name in task_groups_dict:
            return

        if circularity_check_queue is None:
            circularity_check_queue = []

        if task_group_name in circularity_check_queue:
            error_string = "Circular dependency detected:\n"
            index = circularity_check_queue.index(task_group_name)
            while index < len(circularity_check_queue):
                error_string += f"{circularity_check_queue[index]} depends on {task_group_name}\n"
                index += 1
            raise Exception(error_string)

        circularity_check_queue.append(task_group_name)

        if task_group_conf.get("parent_group_name"):
            parent_group_name = task_group_conf["parent_group_name"]
            parent_group_conf = task_groups[parent_group_name]
            DagBuilder.make_nested_task_groups(
                parent_group_name, parent_group_conf, task_groups_dict, task_groups, circularity_check_queue, dag
            )
            task_group_conf["parent_group"] = task_groups_dict[parent_group_name]

        task_group_conf["group_id"] = task_group_name
        task_group_conf["dag"] = dag

        task_group_conf = DagBuilder._init_task_group_callback_param(task_group_conf)

        task_group = TaskGroup(**{k: v for k, v in task_group_conf.items() if k not in SYSTEM_PARAMS})
        task_groups_dict[task_group_name] = task_group

    @staticmethod
    def set_dependencies(
        tasks_config: Dict[str, Dict[str, Any]],
        operators_dict: Dict[str, BaseOperator],
        task_groups_config: Dict[str, Dict[str, Any]],
        task_groups_dict: Dict[str, "TaskGroup"],
    ):
        """Take the task configurations in YAML file and operator
        instances, then set the dependencies between tasks.

        :param tasks_config: Raw task configuration from YAML file
        :param operators_dict: Dictionary for operator instances
        :param task_groups_config: Raw task group configuration from YAML file
        :param task_groups_dict: Dictionary for task group instances
        """
        tasks_and_task_groups_config = {**tasks_config, **task_groups_config}
        tasks_and_task_groups_instances = {**operators_dict, **task_groups_dict}
        for name, conf in tasks_and_task_groups_config.items():
            # if task is in a task group, group_id is prepended to its name
            if conf.get("task_group"):
                group_id = conf["task_group"].group_id
                name = f"{group_id}.{name}"
            if conf.get("dependencies"):
                source: Union[BaseOperator, "TaskGroup"] = tasks_and_task_groups_instances[name]

                for dep in conf["dependencies"]:
                    if tasks_and_task_groups_config[dep].get("task_group"):
                        group_id = tasks_and_task_groups_config[dep]["task_group"].group_id
                        dep = f"{group_id}.{dep}"
                    dep: Union[BaseOperator, "TaskGroup"] = tasks_and_task_groups_instances[dep]

                    source.set_upstream(dep)

    @staticmethod
    def replace_expand_values(task_conf: Dict, tasks_dict: Dict[str, BaseOperator]):
        """
        Replaces any expand values in the task configuration with their corresponding XComArg value.
        :param: task_conf: the configuration dictionary for the task.
        :type: Dict
        :param: tasks_dict: a dictionary containing the tasks for the current DAG run.
        :type: Dict[str, BaseOperator]

        :returns: updated conf dict with expanded values replaced with their XComArg values.
        :type: Dict
        """

        for expand_key, expand_value in task_conf["expand"].items():
            if ".output" in expand_value:
                task_id = expand_value.split(".output")[0]
                if task_id in tasks_dict:
                    task_conf["expand"][expand_key] = tasks_dict[task_id].output
            elif "XcomArg" in expand_value:
                task_id = re.findall(r"\(+(.*?)\)", expand_value)[0]
                if task_id in tasks_dict:
                    task_conf["expand"][expand_key] = tasks_dict[task_id].output
        return task_conf

    @staticmethod
    def safe_eval(condition_string: str, dataset_map: dict) -> Any:
        """
        Safely evaluates a condition string using the provided dataset map.

        :param condition_string: A string representing the condition to evaluate.
            Example: "(dataset_custom_1 & dataset_custom_2) | dataset_custom_3".
        :type condition_string: str
        :param dataset_map: A dictionary where keys are valid variable names (dataset aliases),
                             and values are Dataset objects.
        :type dataset_map: dict

        :returns: The result of evaluating the condition.
        :rtype: Any
        """
        tree = ast.parse(condition_string, mode="eval")
        evaluator = parsers.SafeEvalVisitor(dataset_map)
        return evaluator.evaluate(tree)

    @staticmethod
    def _extract_and_transform_datasets(datasets_conditions: str) -> Tuple[str, Dict[str, Any]]:
        """
        Extracts dataset names and storage paths from the conditions string and transforms them into valid variable names.

        :param datasets_conditions: A string of conditions dataset URIs to be evaluated in the condition.
        :type datasets_conditions: str

        :returns: A tuple containing the transformed conditions string and the dataset map.
        :rtype: Tuple[str, Dict[str, Any]]
        """
        dataset_map = {}
        datasets_filter: List[str] = utils.extract_dataset_names(datasets_conditions) + utils.extract_storage_names(
            datasets_conditions
        )

        for uri in datasets_filter:
            valid_variable_name = utils.make_valid_variable_name(uri)
            datasets_conditions = datasets_conditions.replace(uri, valid_variable_name)
            dataset_map[valid_variable_name] = Dataset(uri)

        return datasets_conditions, dataset_map

    @staticmethod
    def evaluate_condition_with_datasets(datasets_conditions: str) -> Any:
        """
        Evaluates a condition using the dataset filter, transforming URIs into valid variable names.

        :param datasets_conditions: A string of conditions dataset URIs to be evaluated in the condition.
        :type datasets_conditions: str

        :returns: The result of the logical condition evaluation with URIs replaced by valid variable names.
        :rtype: Any
        """
        datasets_conditions, dataset_map = DagBuilder._extract_and_transform_datasets(datasets_conditions)
        evaluated_condition = DagBuilder.safe_eval(datasets_conditions, dataset_map)
        return evaluated_condition

    @staticmethod
    def process_file_with_datasets(file: str, datasets_conditions: str) -> Any:
        """
        Processes datasets from a file and evaluates conditions if provided.

        :param file: The file path containing dataset information in a YAML or other structured format.
        :type file: str
        :param datasets_conditions: A string of dataset conditions to filter and process.
        :type datasets_conditions: str

        :returns: The result of the condition evaluation if `condition_string` is provided, otherwise a list of `Dataset` objects.
        :rtype: Any
        """
        is_airflow_version_at_least_2_9 = version.parse(AIRFLOW_VERSION) >= version.parse("2.9.0")
        datasets_conditions, dataset_map = DagBuilder._extract_and_transform_datasets(datasets_conditions)

        if is_airflow_version_at_least_2_9:
            map_datasets = utils.get_datasets_map_uri_yaml_file(file, list(dataset_map.keys()))
            dataset_map = {alias_dataset: Dataset(uri) for alias_dataset, uri in map_datasets.items()}
            evaluated_condition = DagBuilder.safe_eval(datasets_conditions, dataset_map)
            return evaluated_condition
        else:
            datasets_uri = utils.get_datasets_uri_yaml_file(file, list(dataset_map.keys()))
            return [Dataset(uri) for uri in datasets_uri]

    @staticmethod
    def configure_schedule(dag_params: Dict[str, Any], dag_kwargs: Dict[str, Any]) -> None:
        """
        Configures the schedule for the DAG based on parameters and the Airflow version.

        :param dag_params: A dictionary containing DAG parameters, including scheduling configuration.
            Example: {"schedule": {"file": "datasets.yaml", "datasets": ["dataset_1"], "conditions": "dataset_1 & dataset_2"}}
        :type dag_params: Dict[str, Any]
        :param dag_kwargs: A dictionary for setting the resulting schedule configuration for the DAG.
        :type dag_kwargs: Dict[str, Any]

        :raises KeyError: If required keys like "schedule" or "datasets" are missing in the parameters.
        :returns: None. The function updates `dag_kwargs` in-place.
        """
        # We want to align the schedule key with the Airflow version 3.0+, so we raise an error if the `schedule_interval` key is used
        if utils.check_dict_key(dag_params, "schedule_interval"):
            raise DagFactoryException(
                "The `schedule_interval` key is no longer supported in Airflow 3.0+. Use `schedule` instead."
            )

        # The `schedule_interval` parameter was deprecated in Airflow 2 and removed in Airflow 3.
        schedule_key = "schedule"

        if INSTALLED_AIRFLOW_VERSION.major < AIRFLOW3_MAJOR_VERSION:
            is_airflow_version_at_least_2_9 = version.parse(AIRFLOW_VERSION) >= version.parse("2.9.0")
            has_schedule_attr = utils.check_dict_key(dag_params, "schedule")

            if has_schedule_attr:
                schedule: Dict[str, Any] = dag_params.get("schedule")

                # Only check for file and datasets attributes if schedule is a dict
                has_file_attr = isinstance(schedule, dict) and utils.check_dict_key(schedule, "file")
                has_datasets_attr = isinstance(schedule, dict) and utils.check_dict_key(schedule, "datasets")

                if has_file_attr and has_datasets_attr:
                    file = schedule.get("file")
                    datasets: Union[List[str], str] = schedule.get("datasets")
                    datasets_conditions: str = utils.parse_list_datasets(datasets)
                    dag_kwargs[schedule_key] = DagBuilder.process_file_with_datasets(file, datasets_conditions)

                elif has_datasets_attr and is_airflow_version_at_least_2_9:
                    datasets = schedule["datasets"]
                    datasets_conditions: str = utils.parse_list_datasets(datasets)
                    dag_kwargs[schedule_key] = DagBuilder.evaluate_condition_with_datasets(datasets_conditions)

                else:
                    if isinstance(schedule, str):
                        # check if it's "none" (case-insensitive, with whitespace)
                        if schedule.strip().lower() == "none":
                            dag_kwargs[schedule_key] = None
                        else:
                            dag_kwargs[schedule_key] = schedule
                    elif isinstance(schedule, list):
                        # if schedule is a list, check if it's a list of URIs
                        # Filter out any empty strings or None values
                        valid_uris = [uri for uri in schedule if uri and uri.strip()]
                        dag_kwargs[schedule_key] = valid_uris
                    else:
                        # For other types, use the schedule as is
                        dag_kwargs[schedule_key] = schedule

                # Only pop keys if schedule is a dict
                if isinstance(schedule, dict):
                    if has_file_attr:
                        schedule.pop("file")
                    if has_datasets_attr:
                        schedule.pop("datasets")
        else:
            schedule = dag_params.get("schedule")
            if (
                utils.check_dict_key(dag_params, "schedule")
                and isinstance(schedule, str)
                and schedule.strip().lower() == "none"
            ):
                dag_kwargs[schedule_key] = None
            else:
                dag_kwargs[schedule_key] = dag_params.get("schedule")

    @staticmethod
    def _normalise_tasks_config(tasks_cfg: Any) -> Dict[str, Dict[str, Any]]:
        """Ensure tasks configuration is in the canonical dict form.

        Dag authors may provide tasks either as a mapping of ``task_id`` -> config
        or as a *list* of configs each containing a ``task_id`` key. This helper
        converts the latter to the former so that the rest of the builder logic
        can operate on a single, predictable structure.

        :param tasks_cfg: the raw ``tasks`` value from the YAML / dict config
        """
        # Nothing provided – let the caller decide how to handle later.
        if tasks_cfg is None:
            return {}

        # Already in the desired form
        if isinstance(tasks_cfg, dict):
            return tasks_cfg

        if isinstance(tasks_cfg, list):
            converted: Dict[str, Dict[str, Any]] = {}

            for entry in tasks_cfg:
                if not isinstance(entry, dict) or "task_id" not in entry:
                    raise DagFactoryConfigException(
                        "Each task definition in the list must be a mapping that contains a 'task_id' key"
                    )

                task_id = entry["task_id"]

                if task_id in converted:
                    raise DagFactoryConfigException(f"Duplicate task_id detected in tasks list: '{task_id}'")

                # Exclude task_id from the configuration body – historically it
                # is represented by the mapping key, not within the dict.
                task_conf = {k: v for k, v in entry.items() if k != "task_id"}
                converted[task_id] = task_conf

            return converted

        raise DagFactoryConfigException("'tasks' must be either a mapping or a list of task configs")

    @staticmethod
    def _normalise_task_groups_config(task_groups_cfg: Any) -> Dict[str, Dict[str, Any]]:
        """Convert a list-based task_groups definition into dict form.

        Accepts either the canonical mapping of ``group_name`` -> config or a list where each item is a mapping
        containing a ``group_name`` key. Performs duplicate detection and basic validation.

        :param task_groups_cfg: the raw ``task_groups`` value from the YAML / dict config
        """

        if task_groups_cfg is None:
            return {}

        if isinstance(task_groups_cfg, dict):
            return task_groups_cfg

        if isinstance(task_groups_cfg, list):
            converted: Dict[str, Dict[str, Any]] = {}

            for entry in task_groups_cfg:
                if not isinstance(entry, dict) or "group_name" not in entry:
                    raise DagFactoryConfigException(
                        "Each task_group definition in the list must be a mapping that contains a 'group_name' key"
                    )

                group_id = entry["group_name"]

                if group_id in converted:
                    raise DagFactoryConfigException(f"Duplicate group_name detected in task_groups list: '{group_id}'")

                group_conf = {k: v for k, v in entry.items() if k != "group_name"}
                converted[group_id] = group_conf

            return converted

        raise DagFactoryConfigException("'task_groups' must be either a mapping or a list of group configs")

    # pylint: disable=too-many-locals
    def build(self) -> Dict[str, Union[str, DAG]]:
        """
        Generates a DAG from the DAG parameters.

        :returns: dict with dag_id and DAG object
        :type: Dict[str, Union[str, DAG]]
        """
        dag_params: Dict[str, Any] = self.get_dag_params()

        dag_params["tasks"] = DagBuilder._normalise_tasks_config(dag_params.get("tasks"))

        dag_params["task_groups"] = DagBuilder._normalise_task_groups_config(dag_params.get("task_groups"))

        dag_kwargs: Dict[str, Any] = {}

        dag_kwargs["dag_id"] = dag_params["dag_id"]
        if version.parse(AIRFLOW_VERSION) >= version.parse("2.9.0"):
            dag_kwargs["dag_display_name"] = dag_params.get("dag_display_name", dag_params["dag_id"])

        dag_kwargs["description"] = dag_params.get("description", None)

        if "concurrency" in dag_params:
            warnings.warn(
                "`concurrency` param is deprecated. Please use max_active_tasks.", category=DeprecationWarning
            )
            dag_kwargs["max_active_tasks"] = dag_params["concurrency"]
        else:
            dag_kwargs["max_active_tasks"] = dag_params.get(
                "max_active_tasks", configuration.conf.getint("core", "max_active_tasks_per_dag")
            )

        if dag_params.get("timetable"):
            dag_kwargs["timetable"] = dag_params.get("timetable")

        dag_kwargs["catchup"] = dag_params.get(
            "catchup", configuration.conf.getboolean("scheduler", "catchup_by_default")
        )

        dag_kwargs["max_active_runs"] = dag_params.get(
            "max_active_runs", configuration.conf.getint("core", "max_active_runs_per_dag")
        )

        dag_kwargs["dagrun_timeout"] = dag_params.get("dagrun_timeout", None)

        if INSTALLED_AIRFLOW_VERSION.major < AIRFLOW3_MAJOR_VERSION:

            dag_kwargs["default_view"] = dag_params.get(
                "default_view", configuration.conf.get("webserver", "dag_default_view")
            )

            dag_kwargs["orientation"] = dag_params.get(
                "orientation", configuration.conf.get("webserver", "dag_orientation")
            )

        dag_kwargs["template_searchpath"] = dag_params.get("template_searchpath", None)

        dag_kwargs["render_template_as_native_obj"] = dag_params.get("render_template_as_native_obj", False)

        if version.parse(AIRFLOW_VERSION) < version.parse("3.1.0"):
            # sla_miss_callback is fully-deprecated as of 3.1.0
            dag_kwargs["sla_miss_callback"] = dag_params.get("sla_miss_callback", None)

        dag_kwargs["on_success_callback"] = dag_params.get("on_success_callback", None)

        dag_kwargs["on_failure_callback"] = dag_params.get("on_failure_callback", None)

        dag_kwargs["default_args"] = dag_params.get("default_args", None)

        dag_kwargs["doc_md"] = dag_params.get("doc_md", None)

        dag_kwargs["access_control"] = dag_params.get("access_control", None)

        dag_kwargs["is_paused_upon_creation"] = dag_params.get("is_paused_upon_creation", None)

        DagBuilder.configure_schedule(dag_params, dag_kwargs)

        dag_kwargs["params"] = dag_params.get("params", None)

        dag_kwargs["start_date"] = dag_params.get("start_date", None)
        dag_kwargs["end_date"] = dag_params.get("end_date", None)

        dag: DAG = DAG(**dag_kwargs)

        if dag_params.get("doc_md_file_path"):
            if not os.path.isabs(dag_params.get("doc_md_file_path")):
                raise DagFactoryException("`doc_md_file_path` must be absolute path")

            with open(dag_params.get("doc_md_file_path"), "r", encoding="utf-8") as file:
                dag.doc_md = file.read()

        if dag_params.get("doc_md_python_callable_file") and dag_params.get("doc_md_python_callable_name"):
            doc_md_callable = utils.get_python_callable(
                dag_params.get("doc_md_python_callable_name"), dag_params.get("doc_md_python_callable_file")
            )
            dag.doc_md = doc_md_callable(**dag_params.get("doc_md_python_arguments", {}))

        # Render YML DAG in DAG Docs
        if self._yml_dag:
            subtitle = "## YML DAG"

            if dag.doc_md is None:
                dag.doc_md = f"{subtitle}\n```yaml\n{self._yml_dag}\n```"
            else:
                dag.doc_md += f"\n{subtitle}\n```yaml\n{self._yml_dag}\n```"

        tags = dag_params.get("tags", [])
        if "dagfactory" not in tags:
            tags.append("dagfactory")
        dag.tags = tags

        tasks: Dict[str, Dict[str, Any]] = dag_params["tasks"]

        # add a property to mark this dag as an auto-generated on
        dag.is_dagfactory_auto_generated = True

        # create dictionary of task groups
        task_groups_dict: Dict[str, "TaskGroup"] = self.make_task_groups(dag_params.get("task_groups", {}), dag)

        # create dictionary to track tasks and set dependencies
        tasks_dict: Dict[str, BaseOperator] = {}
        tasks_tuples = self.topological_sort_tasks(tasks)
        for task_name, task_conf in tasks_tuples:
            task_conf["task_id"]: str = task_name
            task_conf["dag"]: DAG = dag

            if task_groups_dict and task_conf.get("task_group_name"):
                task_conf["task_group"] = task_groups_dict[task_conf.get("task_group_name")]

            params: Dict[str, Any] = {k: v for k, v in task_conf.items() if k not in SYSTEM_PARAMS}

            if "operator" in task_conf:
                operator: str = task_conf["operator"]

                if task_conf.get("expand"):
                    task_conf = self.replace_expand_values(task_conf, tasks_dict)

                task: Union[BaseOperator, MappedOperator] = DagBuilder.make_task(operator=operator, task_params=params)
                tasks_dict[task.task_id]: BaseOperator = task

            elif "decorator" in task_conf:
                task = DagBuilder.make_decorator(
                    decorator_import_path=task_conf["decorator"], task_params=params, tasks_dict=tasks_dict
                )
                tasks_dict[task_name]: BaseOperator = task
            else:
                raise DagFactoryConfigException("Tasks must define either 'operator' or 'decorator")

        # set task dependencies after creating tasks
        self.set_dependencies(tasks, tasks_dict, dag_params.get("task_groups", {}), task_groups_dict)

        return {"dag_id": dag_params["dag_id"], "dag": dag}

    @staticmethod
    def topological_sort_tasks(tasks_configs: dict[str, Any]) -> list[tuple(str, Any)]:
        """
        Use the Kahn's algorithm to sort topologically the tasks:
        (https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm)

        The complexity is O(N + D) where N: total tasks and D: number of dependencies.

        :returns: topologically sorted list containing tuples (task name, task config)
        """
        # Step 1: Build the downstream (adjacency) tasks list and the upstream dependencies (in-degree) count
        downstream_tasks = {}
        upstream_dependencies_count = {}

        for task_name, _ in tasks_configs.items():
            downstream_tasks[task_name] = []
            upstream_dependencies_count[task_name] = 0

        for task_name, task_conf in tasks_configs.items():
            for upstream_task in task_conf.get("dependencies", []):
                # there are cases when dependencies contains references to TaskGroups and not Tasks - we skip those
                if upstream_task in tasks_configs:
                    downstream_tasks[upstream_task].append(task_name)
                    upstream_dependencies_count[task_name] += 1

        # Step 2: Find all tasks with no dependencies
        tasks_without_dependencies = [
            task for task in upstream_dependencies_count if not upstream_dependencies_count[task]
        ]
        sorted_tasks = []

        # Step 3: Perform topological sort
        while tasks_without_dependencies:
            current = tasks_without_dependencies.pop(0)
            sorted_tasks.append((current, tasks_configs[current]))

            for child in downstream_tasks[current]:
                upstream_dependencies_count[child] -= 1
                if upstream_dependencies_count[child] == 0:
                    tasks_without_dependencies.append(child)

        # If not all tasks are processed, there is a cycle (not applicable for DAGs)
        if len(sorted_tasks) != len(tasks_configs):
            raise ValueError("Cycle detected in task dependencies!")

        return sorted_tasks

    @staticmethod
    def adjust_general_task_params(task_params: dict[str, Any]):
        """Adjusts in place the task params argument"""
        # Used by airflow.sensors.external_task_sensor.ExternalTaskSensor
        if utils.check_dict_key(task_params, "execution_date_fn"):
            python_callable: Callable = import_string(task_params["execution_date_fn"])
            task_params["execution_date_fn"] = python_callable
        elif utils.check_dict_key(task_params, "execution_delta"):
            execution_delta = utils.get_time_delta(task_params["execution_delta"])
            task_params["execution_delta"] = execution_delta
        elif utils.check_dict_key(task_params, "execution_date_fn_name") and utils.check_dict_key(
            task_params, "execution_date_fn_file"
        ):
            task_params["execution_date_fn"]: Callable = utils.get_python_callable(
                task_params["execution_date_fn_name"], task_params["execution_date_fn_file"]
            )
            del task_params["execution_date_fn_name"]
            del task_params["execution_date_fn_file"]

        # on_execute_callback is an Airflow 2.0 feature
        for callback_type in [
            "on_execute_callback",
            "on_success_callback",
            "on_failure_callback",
            "on_retry_callback",
            "on_skipped_callback",
        ]:
            if utils.check_dict_key(task_params, callback_type):
                task_params[callback_type]: Callable = DagBuilder.set_callback(
                    parameters=task_params, callback_type=callback_type
                )

            # Check for file path and name
            if utils.check_dict_key(task_params, f"{callback_type}_name") and utils.check_dict_key(
                task_params, f"{callback_type}_file"
            ):
                task_params[callback_type] = DagBuilder.set_callback(
                    parameters=task_params, callback_type=callback_type, has_name_and_file=True
                )

        # use variables as arguments on operator
        if utils.check_dict_key(task_params, "variables_as_arguments"):
            variables: List[Dict[str, str]] = task_params.get("variables_as_arguments")
            for variable in variables:
                default_argument_name = "default"
                if INSTALLED_AIRFLOW_VERSION.major < AIRFLOW3_MAJOR_VERSION:
                    default_argument_name = "default_var"
                variable_value = Variable.get(variable["variable"], **{default_argument_name: None})
                if variable_value is not None:
                    task_params[variable["attribute"]] = variable_value
            del task_params["variables_as_arguments"]

        if version.parse(AIRFLOW_VERSION) < version.parse("3.0.0"):
            for key in ["inlets", "outlets"]:
                if utils.check_dict_key(task_params, key):
                    if utils.check_dict_key(task_params[key], "file") and utils.check_dict_key(
                        task_params[key], "datasets"
                    ):
                        file = task_params[key]["file"]
                        datasets_filter = task_params[key]["datasets"]
                        datasets_uri = utils.get_datasets_uri_yaml_file(file, datasets_filter)

                        del task_params[key]["file"]
                        del task_params[key]["datasets"]
                    else:
                        datasets_uri = task_params[key]

                    if key in task_params and datasets_uri:
                        task_params[key] = [Dataset(uri) for uri in datasets_uri]

    @staticmethod
    def make_decorator(
        decorator_import_path: str, task_params: Dict[str, Any], tasks_dict: dict(str, Any)
    ) -> BaseOperator:
        """
        Takes a decorator and params and creates an instance of that decorator.

        :returns: instance of operator object
        """
        # Check mandatory fields
        mandatory_keys_set1 = set(["python_callable_name", "python_callable_file"])

        # Fetch the Python callable
        if set(mandatory_keys_set1).issubset(task_params):
            python_callable: Callable = utils.get_python_callable(
                task_params["python_callable_name"], task_params["python_callable_file"]
            )
            # Remove dag-factory specific parameters since Airflow 2.0 doesn't allow these to be passed to operator
            del task_params["python_callable_name"]
            del task_params["python_callable_file"]
        elif "python_callable" in task_params:
            python_callable: Callable = import_string(task_params["python_callable"])
        else:
            raise DagFactoryException(
                "Failed to create task. Decorator-based tasks require \
                `python_callable_name` and `python_callable_file` "
                "parameters.\nOptionally you can load python_callable "
                "from a file. with the special pyyaml notation:\n"
                "  python_callable: !!python/name:my_module.my_func"
            )

        task_params["python_callable"] = python_callable

        decorator: Callable[..., BaseOperator] = import_string(decorator_import_path)
        task_params.pop("decorator")

        DagBuilder.adjust_general_task_params(task_params)

        callable_args_keys = inspect.getfullargspec(python_callable).args
        callable_kwargs = {}
        decorator_kwargs = dict(**task_params)
        for arg_key, arg_value in task_params.items():
            if arg_key in callable_args_keys:
                decorator_kwargs.pop(arg_key)
                if isinstance(arg_value, str) and arg_value.startswith("+"):
                    upstream_task_name = arg_value.split("+")[-1]
                    callable_kwargs[arg_key] = tasks_dict[upstream_task_name]
                else:
                    callable_kwargs[arg_key] = arg_value

        expand_kwargs = decorator_kwargs.pop("expand", {})
        partial_kwargs = decorator_kwargs.pop("partial", {})

        if ("map_index_template" in decorator_kwargs) and (version.parse(AIRFLOW_VERSION) < version.parse("2.7.0")):
            raise DagFactoryConfigException(
                "The dynamic task mapping argument `map_index_template` is only supported since Airflow 2.7"
            )

        if expand_kwargs and partial_kwargs:
            if callable_kwargs:
                raise DagFactoryConfigException(
                    "When using dynamic task mapping, all the task arguments should be defined in expand and partial."
                )
            DagBuilder.replace_kwargs_values_as_tasks(expand_kwargs, tasks_dict)
            DagBuilder.replace_kwargs_values_as_tasks(partial_kwargs, tasks_dict)
            return decorator(**decorator_kwargs).partial(**partial_kwargs).expand(**expand_kwargs)
        elif expand_kwargs:
            DagBuilder.replace_kwargs_values_as_tasks(expand_kwargs, tasks_dict)
            return decorator(**decorator_kwargs).expand(**expand_kwargs)
        else:
            return decorator(**decorator_kwargs)(**callable_kwargs)

    @staticmethod
    def replace_kwargs_values_as_tasks(kwargs: dict(str, Any), tasks_dict: dict(str, Any)):
        for key, value in kwargs.items():
            if isinstance(value, str) and value.startswith("+"):
                upstream_task_name = value.split("+")[-1]
                kwargs[key] = tasks_dict[upstream_task_name]

    @staticmethod
    def set_callback(parameters: Union[dict, str], callback_type: str, has_name_and_file=False) -> Callable:
        """
        Update the passed-in config with the callback.

        :param parameters:
        :param callback_type:
        :param has_name_and_file:
        :returns: Callable
        """

        # There is scenario where a callback is passed in via a file and a name. For the most part, this will be a
        # Python callable that is treated similarly to a Python callable that the PythonOperator may leverage. That
        # being said, what if this is not a Python callable? What if this is another type?
        if has_name_and_file:
            on_state_callback_callable: Callable = utils.get_python_callable(
                python_callable_name=parameters[f"{callback_type}_name"],
                python_callable_file=parameters[f"{callback_type}_file"],
            )

            # Delete the callback_type name and file
            del parameters[f"{callback_type}_name"]
            del parameters[f"{callback_type}_file"]

            return on_state_callback_callable

        # If the value stored at parameters[callback_type] is a string, it should be imported under the assumption that
        # it is a function that is "ready to be called". If not returning the function, something like this could be
        # used to update the config parameters[callback_type] = import_string(parameters[callback_type])
        if isinstance(parameters[callback_type], str):
            return import_string(parameters[callback_type])

        # Otherwise, if the parameter[callback_type] is a dictionary, it should be treated similar to the Python
        # callable
        elif isinstance(parameters[callback_type], dict):
            # Pull the on_failure_callback dictionary from dag_params
            on_state_callback_params: dict = parameters[callback_type]

            # Check to see if there is a "callback" key in the on_failure_callback dictionary. If there is, parse
            # out that callable, and add the parameters
            if utils.check_dict_key(on_state_callback_params, "callback"):
                if isinstance(on_state_callback_params["callback"], str):
                    on_state_callback_callable: Callable = import_string(on_state_callback_params["callback"])
                    del on_state_callback_params["callback"]

                    # Return the callable, this time, using the params provided in the YAML file, rather than a .py
                    # file with a callable configured. If not returning the partial, something like this could be used
                    # to update the config ... parameters[callback_type]: Callable = partial(...)
                    if hasattr(on_state_callback_callable, "notify"):
                        return on_state_callback_callable(**on_state_callback_params)

                    return partial(on_state_callback_callable, **on_state_callback_params)

        raise DagFactoryConfigException(f"Invalid type passed to {callback_type}")
