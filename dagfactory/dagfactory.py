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

from dagfactory._yaml import load_yaml_file
from dagfactory.constants import DEFAULTS_FILE_NAMES
from dagfactory.dagbuilder import DagBuilder
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException

# these are params that cannot be a dag name
SYSTEM_PARAMS: List[str] = ["default", "task_groups"]


class _DagFactory:
    """
    Takes a YAML config or a python dictionary and generates DAGs.

    :param config_filepath: the filepath of the DAG factory YAML config file.
        Must be absolute path to file. Cannot be used with `config`.
    :type config_filepath: str
    :param config_dict: DAG factory config dictionary. Cannot be used with `config_filepath`.
    :type config_dict: dict
    :param defaults_config_path: The path to a file that contains the default arguments for that DAG.
    :type defaults_config_path: str
    :param defaults_config_dict: A dictionary of default arguments for that DAG, as an alternative to default_args_config_path.
    :type defaults_config_dict: dict
    """

    def __init__(
        self,
        config_filepath: Optional[str] = None,
        config_dict: Optional[dict] = None,
        defaults_config_path: str = airflow_conf.get("core", "dags_folder"),
        defaults_config_dict: Optional[dict] = None,
    ) -> None:
        # Handle the config(_filepath)
        assert bool(config_filepath) ^ bool(config_dict), "Either `config_filepath` or `config` should be provided"

        if config_filepath:
            _DagFactory._validate_config_filepath(config_filepath=config_filepath)
            self.config: Dict[str, Any] = self._load_dag_config(config_filepath=config_filepath)
        if config_dict:
            self.config = config_dict

        # These default args are a bit different; these are not the "default" structure that is applied to certain DAGs.
        # These are in-fact the "default" default_args
        if defaults_config_dict:
            # Log a warning if the default_args parameter is specified. If both the default_args and
            # default_args_file_path are passed, we'll throw an exception.
            logging.warning(
                "Manually specifying `default_args_config_dict` will override the values in the `defaults.yml` file."
            )

            if defaults_config_path != airflow_conf.get("core", "dags_folder"):
                raise DagFactoryException("Cannot pass both `default_args_config_dict` and `default_args_config_path`.")

        # We'll still go ahead and set both values. They'll be referenced in _global_default_args.
        self.config_file_path: str = config_filepath
        self.default_args_config_path: str = defaults_config_path
        self.defaults_config_dict: Optional[dict] = defaults_config_dict

    def _global_default_args(self):
        """
        If self.default_args exists, use this as the global default_args (to be applied to each DAG). Otherwise, fall
        back to the defaults.yml file.
        """
        if self.defaults_config_dict:
            return self.defaults_config_dict

        configs_list = self._retrieve_default_config_list()
        merged_default_config = {
            "default_args": self._merge_default_args_from_list_configs(configs_list),
            **self._merge_dag_args_from_list_configs(configs_list),
        }
        return merged_default_config

    def _retrieve_possible_default_config_dirs(self):
        """
        Return a list of possible directories with the `defaults.yml` file.
        The returned directories are sorted by priority, with the top-priority directory being the first element.
        """
        if self.config_file_path:
            dag_yml_file_parent_dirs = Path(self.config_file_path).parents
        else:
            dag_yml_file_parent_dirs = []

        default_config_root_dir_path = Path(self.default_args_config_path)

        # Assuming default_config_root_dir_path is a parent directory of the dag_yml_file_dir_path
        if default_config_root_dir_path in dag_yml_file_parent_dirs:
            index_top_most_default_dir = dag_yml_file_parent_dirs.index(default_config_root_dir_path)
            return [path for path in dag_yml_file_parent_dirs][: index_top_most_default_dir + 1]
        elif dag_yml_file_parent_dirs:
            return [dag_yml_file_parent_dirs[0], default_config_root_dir_path]
        else:
            return [default_config_root_dir_path]

    def _retrieve_default_yaml_filepaths(self):
        """
        Return the paths to existing `defaults.yml` files relevant to run the YAML DAG of interest.
        The YAML filepaths are sorted by priority, with the top-priority directory being the first element.
        """
        default_yaml_filepaths = []
        possible_default_yml_dirs = self._retrieve_possible_default_config_dirs()
        for default_yml_dir in possible_default_yml_dirs:
            for default_file_name in DEFAULTS_FILE_NAMES:
                default_yml_filepath = default_yml_dir / default_file_name
                if default_yml_filepath.exists():
                    default_yaml_filepaths.append(default_yml_filepath)
                    break  # Only use the first one found (yml preferred over yaml)
        return default_yaml_filepaths

    def _retrieve_default_config_list(self):
        """
        Merges the default configuration with the priority configuration.
        """
        list_of_yaml_paths = self._retrieve_default_yaml_filepaths()
        configs_list = []

        # We change the order so that the configuration that should override all others is the last element
        for yaml_path in reversed(list_of_yaml_paths):
            configs_list.append(self._load_dag_config(config_filepath=yaml_path))

        return configs_list

    @staticmethod
    def _merge_default_args_from_list_configs(configs_list: list[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Given a list of dictionaries that is sorted by priority, with the dictionary that takes precedence by the end, merge
        their "default_args" key-value pairs.

        Return a dictionary with the "default_args" key-value pairs merged.
        """
        return {key: value for config in configs_list for key, value in config.get("default_args", {}).items()}

    @staticmethod
    def _merge_dag_args_from_list_configs(configs_list: list[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Given a list of configuration dictionaries that is sorted by priority, with the dictionary that takes precedence by the end, merge them.
        If there are redundant keys, the correspondent value of the last configuration dictionary will be used.
        """
        final_config = {}
        for config in configs_list:
            for key, value in config.items():
                if key != "default_args":
                    if key != "tags":
                        final_config[key] = value
                    else:
                        final_config[key] = sorted(list(set(final_config.get("tags", []) + value)))

        return final_config

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
        return load_yaml_file(config_filepath)

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
        if isinstance(global_default_args, dict):
            default_config["default_args"] = self._merge_default_args_from_list_configs(
                [global_default_args, default_config]
            )

        dags: Dict[str, Any] = {}

        if isinstance(global_default_args, dict):
            dag_level_args = self._merge_dag_args_from_list_configs([global_default_args])
        else:
            dag_level_args = {}

        for dag_name, dag_config in dag_configs.items():
            # Apply DAG-level default arguments from global_default_args to each dag_config,
            # this is helpful because some arguments are not supported in default_args.
            if isinstance(global_default_args, dict):
                dag_config = {**dag_level_args, **dag_config}

            dag_config["task_groups"] = dag_config.get("task_groups", {})
            dag_builder: DagBuilder = DagBuilder(
                dag_name=dag_name,
                dag_config=dag_config,
                default_config=default_config,
                yml_dag=self._serialise_config_md(dag_name, dag_config, default_config),
            )
            dag: Dict[str, Union[str, DAG]] = dag_builder.build()
            dags[dag["dag_id"]]: DAG = dag["dag"]

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

    def _generate_dags(self, globals: Dict[str, Any]) -> None:
        """
        Generates DAGs from YAML config

        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        dags: Dict[str, Any] = self.build_dags()
        self.register_dags(dags, globals)


def load_yaml_dags(
    globals_dict: Dict[str, Any],
    dags_folder: str = airflow_conf.get("core", "dags_folder"),
    config_filepath: Optional[str] = None,
    defaults_config_path: str = airflow_conf.get("core", "dags_folder"),
    config_dict: Optional[dict] = None,
    defaults_config_dict: Optional[dict] = None,
    suffix=None,
):
    """
    Loads YAML or YML files in a specified folder (or from a specific YAML file or dictionary)

    The dags folder is defaulted to the airflow dags folder if unspecified.
    And the prefix is set to yaml/yml by default. However, it can be
    interesting to load only a subset by setting a different suffix.

    :param globals_dict: The globals() from the file used to generate DAGs
    :param dags_folder: Path to the folder you want to get recursively scanned
    :param config_filepath: A YAML path for DAG config.
    :param defaults_config_path: The Folder path where defaults.yml exist.
    :param config_dict: The DAG dictionary.
    :param defaults_config_dict: The dictionary that hold default value.
    :param suffix: file suffix to filter `in` what files to scan for dags
    """
    # TODO: Support ignoring yml files in load_yaml_dags
    # https://github.com/astronomer/dag-factory/issues/527
    # chain all file suffixes in a single iterator
    logging.info("Loading DAGs from %s", dags_folder)
    if suffix is None:
        suffix = [".yaml", ".yml"]
    candidate_dag_files = []

    if config_filepath:
        factory = _DagFactory(config_filepath=config_filepath, defaults_config_path=defaults_config_path)
        factory._generate_dags(globals_dict)
    elif config_dict:
        factory = _DagFactory(config_dict=config_dict, defaults_config_dict=defaults_config_dict)
        factory._generate_dags(globals_dict)
    else:
        for suf in suffix:
            candidate_dag_files = list(chain(candidate_dag_files, Path(dags_folder).rglob(f"*{suf}")))
        for config_file_path in candidate_dag_files:
            config_file_abs_path = str(config_file_path.absolute())
            logging.info("Loading %s", config_file_abs_path)
            try:
                factory = _DagFactory(config_file_abs_path, defaults_config_dict=defaults_config_dict)
                factory._generate_dags(globals_dict)
            except Exception:  # pylint: disable=broad-except
                logging.exception("Failed to load dag from %s", config_file_path)
            else:
                logging.info("DAG loaded: %s", config_file_path)
