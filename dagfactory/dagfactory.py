"""Module contains code for loading a DagFactory config and generating DAGs"""

import logging
import os
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from airflow.configuration import conf as airflow_conf

try:
    from airflow.sdk.definitions.dag import DAG
except ImportError:
    from airflow.models import DAG
from airflow.version import version as AIRFLOW_VERSION
from packaging import version

from dagfactory.dagbuilder import DagBuilder
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException
from dagfactory.utils import cast_with_type, update_yaml_structure

# these are params that cannot be a dag name
SYSTEM_PARAMS: List[str] = ["default", "task_groups"]


class DagFactory:
    """
    Takes a YAML config or a python dictionary and generates DAGs.

    :param config_filepath: the filepath of the DAG factory YAML config file.
        Must be absolute path to file. Cannot be used with `config`.
    :type config_filepath: str
    :param config: DAG factory config dictionary. Cannot be used with `config_filepath`.
    :type config: dict
    :param default_args_config_path: The path to a file that contains the default arguments for that DAG.
    :type default_args_config_path: str
    :param default_args_config_dict: A dictionary of default arguments for that DAG, as an alternative to default_args_config_path.
    :type default_args_config_dict: dict
    """

    def __init__(
        self,
        config_filepath: Optional[str] = None,
        config: Optional[dict] = None,
        default_args_config_path: str = airflow_conf.get("core", "dags_folder"),
        default_args_config_dict: Optional[dict] = None,
    ) -> None:
        # Handle the config(_filepath)
        assert bool(config_filepath) ^ bool(config), "Either `config_filepath` or `config` should be provided"

        if config_filepath:
            DagFactory._validate_config_filepath(config_filepath=config_filepath)
            self.config: Dict[str, Any] = self._load_dag_config(config_filepath=config_filepath)
        if config:
            self.config: Dict[str, Any] = config

        # These default args are a bit different; these are not the "default" structure that is applied to certain DAGs.
        # These are in-fact the "default" default_args
        if default_args_config_dict:
            # Log a warning if the default_args parameter is specified. If both the default_args and
            # default_args_file_path are passed, we'll throw an exception.
            logging.warning(
                "Manually specifying `default_args_config_dict` will override the values in the `defaults.yml` file."
            )

            if default_args_config_path != airflow_conf.get("core", "dags_folder"):
                raise DagFactoryException("Cannot pass both `default_args_config_dict` and `default_args_config_path`.")

        # We'll still go ahead and set both values. They'll be referenced in _global_default_args.
        self.default_args_config_path: str = default_args_config_path
        self.default_args_config_dict: Optional[dict] = default_args_config_dict

    def _load_yaml_config(self, config_filepath: str) -> Dict[str, Any]:
        """For loading yaml config file, including DAG config and default args config."""

        def __join(loader: yaml.FullLoader, node: yaml.Node) -> str:
            seq = loader.construct_sequence(node)
            return "".join([str(i) for i in seq])

        def __or(loader: yaml.FullLoader, node: yaml.Node) -> str:
            seq = loader.construct_sequence(node)
            return " | ".join([f"({str(i)})" for i in seq])

        def __and(loader: yaml.FullLoader, node: yaml.Node) -> str:
            seq = loader.construct_sequence(node)
            return " & ".join([f"({str(i)})" for i in seq])

        yaml.add_constructor("!join", __join, yaml.FullLoader)
        yaml.add_constructor("!or", __or, yaml.FullLoader)
        yaml.add_constructor("!and", __and, yaml.FullLoader)

        with open(config_filepath, "r", encoding="utf-8") as fp:
            config_with_env = os.path.expandvars(fp.read())
            config: Dict[str, Any] = yaml.load(stream=config_with_env, Loader=yaml.FullLoader)
            config = cast_with_type(config)
        return config

    def _global_default_args(self):
        """
        If self.default_args exists, use this as the global default_args (to be applied to each DAG). Otherwise, fall
        back to the defaults.yml file.
        """
        if self.default_args_config_dict:
            return self.default_args_config_dict

        default_args_yml = Path(self.default_args_config_path) / "defaults.yml"

        if default_args_yml.exists():
            return self._load_yaml_config(default_args_yml)

    @staticmethod
    def _serialise_config_md(dag_name, dag_config, default_config):
        # Remove empty task_groups if it exists
        # We inject it if not supply by user
        # https://github.com/astronomer/dag-factory/blob/e53b456d25917b746d28eecd1e896595ae0ee62b/dagfactory/dagfactory.py#L102
        if dag_config.get("task_groups") == {}:
            del dag_config["task_groups"]

        # Convert default_config to YAML format
        default_config = {"default": default_config}
        default_config_yaml = yaml.dump(default_config, default_flow_style=False, allow_unicode=True, sort_keys=False)

        # Convert dag_config to YAML format
        dag_config = {dag_name: dag_config}
        dag_config_yaml = yaml.dump(dag_config, default_flow_style=False, allow_unicode=True, sort_keys=False)

        # Combine the two YAML outputs with appropriate formatting
        dag_yml = default_config_yaml + "\n" + dag_config_yaml

        return dag_yml

    @staticmethod
    def _validate_config_filepath(config_filepath: str) -> None:
        """
        Validates config file path is absolute
        """
        if not os.path.isabs(config_filepath):
            raise DagFactoryConfigException("DAG Factory `config_filepath` must be absolute path")

    def _load_dag_config(self, config_filepath: str) -> Dict[str, Any]:
        """
        Loads DAG config file to dictionary

        :returns: dict from YAML config file
        """
        # pylint: disable=consider-using-with
        try:
            config = self._load_yaml_config(config_filepath)
            # This will only invoke in the CI
            # Make yaml DAG compatible for Airflow 3
            if version.parse(AIRFLOW_VERSION) >= version.parse("3.0.0") and os.getenv("AUTO_CONVERT_TO_AF3"):
                config = update_yaml_structure(config)

        except Exception as err:
            raise DagFactoryConfigException("Invalid DAG Factory config file") from err
        return config

    def get_dag_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns configuration for each the DAG in factory

        :returns: dict with configuration for dags
        """
        return {dag: self.config[dag] for dag in self.config.keys() if dag not in SYSTEM_PARAMS}

    def get_default_config(self) -> Dict[str, Any]:
        """
        Returns defaults for the DAG factory. If no defaults exist, returns empty dict.

        :returns: dict with default configuration
        """
        return self.config.get("default", {})

    def build_dags(self) -> Dict[str, DAG]:
        """Build DAGs using the config file."""
        dag_configs: Dict[str, Dict[str, Any]] = self.get_dag_configs()
        global_default_args = self._global_default_args()
        default_config: Dict[str, Any] = self.get_default_config()

        # If global_default_args is None, then default_config will remain as is. Otherwise, we'll (try) go ahead and
        # update the default args using global_default_args
        if isinstance(global_default_args, dict):
            # Previously, default_config was being overwritten completely to only container the default_args
            # key-value pair. This was updated as part of issue-295 to not overwrite the entire default_config
            # dictionary, and instead update the default_args key-value pair of the default_config dictionary
            default_config["default_args"] = {
                **global_default_args.get("default_args", {}),
                **default_config.get("default_args", {}),
            }

        dags: Dict[str, Any] = {}

        for dag_name, dag_config in dag_configs.items():
            dag_config["task_groups"] = dag_config.get("task_groups", {})
            dag_builder: DagBuilder = DagBuilder(
                dag_name=dag_name,
                dag_config=dag_config,
                default_config=default_config,
                yml_dag=self._serialise_config_md(dag_name, dag_config, default_config),
            )
            try:
                dag: Dict[str, Union[str, DAG]] = dag_builder.build()
                dags[dag["dag_id"]]: DAG = dag["dag"]
            except Exception as err:
                raise DagFactoryException(f"Failed to generate dag {dag_name}: {err}") from err

        return dags

    # pylint: disable=redefined-builtin
    @staticmethod
    def register_dags(dags: Dict[str, DAG], globals: Dict[str, Any]) -> None:
        """Adds `dags` to `globals` so Airflow can discover them.

        :param dags: Dict of DAGs to be registered.
        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        for dag_id, dag in dags.items():
            globals[dag_id]: DAG = dag

    def generate_dags(self, globals: Dict[str, Any]) -> None:
        """
        Generates DAGs from YAML config

        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        dags: Dict[str, Any] = self.build_dags()
        self.register_dags(dags, globals)

    def clean_dags(self, globals: Dict[str, Any]) -> None:
        """
        Clean old DAGs that are not on YAML config but were auto-generated through dag-factory

        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        dags: Dict[str, Any] = self.build_dags()

        # filter dags that exists in globals and is auto-generated by dag-factory
        dags_in_globals: Dict[str, Any] = {}
        for k, glb in globals.items():
            if isinstance(glb, DAG) and hasattr(glb, "is_dagfactory_auto_generated"):
                dags_in_globals[k] = glb

        # finding dags that doesn't exist anymore
        dags_to_remove: List[str] = list(set(dags_in_globals) - set(dags))

        # removing dags from DagBag
        for dag_to_remove in dags_to_remove:
            del globals[dag_to_remove]


def load_yaml_dags(
    globals_dict: Dict[str, Any],
    dags_folder: str = airflow_conf.get("core", "dags_folder"),
    default_args_config_path: str = airflow_conf.get("core", "dags_folder"),
    suffix=None,
):
    """
    Loads all the yaml/yml files in the dags folder

    The dags folder is defaulted to the airflow dags folder if unspecified.
    And the prefix is set to yaml/yml by default. However, it can be
    interesting to load only a subset by setting a different suffix.

    :param globals_dict: The globals() from the file used to generate DAGs
    :param dags_folder: Path to the folder you want to get recursively scanned
    :param default_args_config_path: The Folder path where defaults.yml exist.
    :param suffix: file suffix to filter `in` what files to scan for dags
    """
    # chain all file suffixes in a single iterator
    logging.info("Loading DAGs from %s", dags_folder)
    if suffix is None:
        suffix = [".yaml", ".yml"]
    candidate_dag_files = []
    for suf in suffix:
        candidate_dag_files = list(chain(candidate_dag_files, Path(dags_folder).rglob(f"*{suf}")))
    for config_file_path in candidate_dag_files:
        config_file_abs_path = str(config_file_path.absolute())
        logging.info("Loading %s", config_file_abs_path)
        try:
            factory = DagFactory(config_file_abs_path, default_args_config_path=default_args_config_path)
            factory.generate_dags(globals_dict)
        except Exception:  # pylint: disable=broad-except
            logging.exception("Failed to load dag from %s", config_file_path)
        else:
            logging.info("DAG loaded: %s", config_file_path)
