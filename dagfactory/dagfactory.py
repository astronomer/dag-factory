"""Module contains code for loading a DagFactory config and generating DAGs"""

import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import yaml
from airflow.configuration import conf as airflow_conf
from pathspec.gitignore import GitIgnoreSpec

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
    :param defaults_config_path: The root directory to search for defaults.yml/defaults.yaml files.
    :type defaults_config_path: str
    :param defaults_config_dict: A dictionary of default arguments for that DAG, as an alternative to defaults_config_path.
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
                raise DagFactoryException("Cannot pass both `default_args_config_dict` and `defaults_config_path`.")

        # We'll still go ahead and set both values. They'll be referenced in _global_default_args.
        self.config_file_path: str = config_filepath
        self.defaults_config_path: str = defaults_config_path
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

        default_config_root_dir_path = Path(self.defaults_config_path)

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


def _read_airflowignore(ignore_file: Path) -> List[str]:
    """Load ignore patterns from a single .airflowignore file."""
    ignore_patterns: List[str] = []

    try:
        with open(ignore_file, "r", encoding="utf-8") as f:
            for line in f:
                pattern = line.split("#", 1)[0].strip()
                if pattern:
                    ignore_patterns.append(pattern)
    except (OSError, IOError) as e:
        logging.warning("Failed to read .airflowignore file at %s: %s", ignore_file, e)

    return ignore_patterns


def _iter_dags_folder_contents(dags_folder: Path) -> Iterator[Tuple[Path, List[str], List[str]]]:
    """Yield directory contents while following symlinked directories once."""
    visited_dirs = set()
    ignore_patterns_by_dir: Dict[Path, List[str]] = {}

    for root, dirs, files in os.walk(dags_folder, topdown=True, followlinks=True):
        root_path = Path(root)

        try:
            resolved_root = root_path.resolve(strict=False)
        except OSError:
            resolved_root = root_path.absolute()

        if resolved_root in visited_dirs:
            dirs[:] = []
            continue

        visited_dirs.add(resolved_root)
        dirs.sort()
        files.sort()

        if ".airflowignore" in files:
            ignore_file = root_path / ".airflowignore"
            ignore_patterns_by_dir[ignore_file.parent] = _read_airflowignore(ignore_file)

        dirs[:] = [
            subdir
            for subdir in dirs
            if not _should_ignore_path(root_path / subdir, dags_folder, ignore_patterns_by_dir)
        ]

        yield root_path, dirs, files


def _load_airflowignore(dags_folder: str) -> Dict[Path, List[str]]:
    """
    Loads ignore patterns from .airflowignore files in the dags folder tree.

    The .airflowignore file follows the same format as Airflow's .airflowignore:
    - Each line contains a pattern to ignore
    - Empty lines and inline comments starting with # are ignored
    - Patterns support glob-style wildcards
    - Nested .airflowignore files are applied relative to the directory that contains them
    - Symlinked directories are traversed during discovery

    :param dags_folder: Path to the DAGs folder
    :type dags_folder: str
    :returns: Mapping of directories to ignore patterns from .airflowignore files
    :rtype: Dict[Path, List[str]]
    """
    dags_folder_path = Path(dags_folder)
    ignore_patterns: Dict[Path, List[str]] = {}

    for root_path, _dirs, files in _iter_dags_folder_contents(dags_folder_path):
        if ".airflowignore" not in files:
            continue

        ignore_file = root_path / ".airflowignore"
        ignore_patterns[ignore_file.parent] = _read_airflowignore(ignore_file)

    return ignore_patterns


def _lexical_relative_path(path: Path, root: Path) -> str:
    """Return a lexical POSIX-style path relative to root without resolving symlinks."""
    return Path(os.path.abspath(path)).relative_to(Path(os.path.abspath(root))).as_posix()


def _get_dag_ignore_file_syntax() -> str:
    """Return the configured Airflow ignore syntax, defaulting to glob."""
    return airflow_conf.get("core", "dag_ignore_file_syntax", fallback="glob").lower()


@lru_cache(maxsize=None)
def _compile_airflowignore_spec(patterns: tuple[str, ...]) -> GitIgnoreSpec:
    """Compile .airflowignore patterns using gitwildmatch semantics."""
    return GitIgnoreSpec.from_lines(patterns)


@lru_cache(maxsize=None)
def _compile_airflowignore_regex(pattern: str) -> re.Pattern[str]:
    """Compile an airflowignore regexp pattern."""
    return re.compile(pattern)


def _matches_airflowignore_patterns(path: str, patterns: List[str]) -> Optional[bool]:
    """Return the last matching gitignore-style decision for a scoped relative path."""
    spec = _compile_airflowignore_spec(tuple(patterns))
    decision: Optional[bool] = None

    for pattern in spec.patterns:
        if pattern.include is None:
            continue
        if pattern.match_file(path):
            decision = pattern.include

    return decision


def _matches_airflowignore_regex(path: str, patterns: List[str]) -> bool:
    """Return whether any regexp pattern matches the DAG-root-relative path."""
    for pattern in patterns:
        try:
            if _compile_airflowignore_regex(pattern).search(path) is not None:
                return True
        except re.error as exc:
            logging.warning("Ignoring invalid regex '%s' from .airflowignore: %s", pattern, exc)
    return False


def _should_ignore_file(file_path: Path, dags_folder: Path, ignore_patterns_by_dir: Dict[Path, List[str]]) -> bool:
    """
    Checks if a file should be ignored based on ignore patterns.

    Patterns use gitignore/Airflow-style gitwildmatch semantics, including directory
    patterns ending in `/` and negation via `!`. Each .airflowignore applies relative
    to the directory that contains it, and nested .airflowignore files are evaluated
    from the DAG root down to the file's parent so later matches can override earlier ones.

    This allows for flexible matching, e.g.:
    - "test_*.yml" matches any file starting with "test_" and ending with ".yml"
    - "subdir/*.yaml" matches any .yaml file in the subdir directory
    - "backup/**/*.yaml" matches any .yaml file in backup directory or any subdirectory

    :param file_path: Path to the file to check
    :type file_path: Path
    :param dags_folder: Path to the DAGs folder (base directory)
    :type dags_folder: Path
    :param ignore_patterns_by_dir: Mapping of directory paths to ignore patterns
    :type ignore_patterns_by_dir: Dict[Path, List[str]]
    :returns: True if the file should be ignored, False otherwise
    :rtype: bool
    """
    return _should_ignore_path(file_path, dags_folder, ignore_patterns_by_dir)


def _should_ignore_path(path: Path, dags_folder: Path, ignore_patterns_by_dir: Dict[Path, List[str]]) -> bool:
    """Checks if a file or directory should be ignored based on ignore patterns."""
    if not ignore_patterns_by_dir:
        return False

    ignore_file_syntax = _get_dag_ignore_file_syntax()

    try:
        relative_path_str = _lexical_relative_path(path, dags_folder)
    except ValueError:
        # Files outside dags_folder only match root-level basename patterns without path separators.
        relative_path_str = ""

    if ignore_file_syntax == "regexp":
        if not relative_path_str:
            return False

        scoped_patterns: List[str] = []
        path_parts = Path(relative_path_str).parts
        ancestor_dirs = [dags_folder]
        for index in range(1, len(path_parts)):
            ancestor_dirs.append(dags_folder / Path(*path_parts[:index]))

        for ignore_dir in ancestor_dirs:
            patterns = ignore_patterns_by_dir.get(ignore_dir)
            if patterns:
                scoped_patterns.extend(patterns)

        if path.is_dir():
            relative_path_str = f"{relative_path_str}/"

        return _matches_airflowignore_regex(relative_path_str, scoped_patterns)

    if ignore_file_syntax not in {"", "glob"}:
        raise ValueError(f"Unsupported ignore_file_syntax: {ignore_file_syntax}")

    ignored = False

    if relative_path_str:
        path_parts = Path(relative_path_str).parts
        ancestor_dirs = [dags_folder]
        for index in range(1, len(path_parts)):
            ancestor_dirs.append(dags_folder / Path(*path_parts[:index]))

        for ignore_dir in ancestor_dirs:
            patterns = ignore_patterns_by_dir.get(ignore_dir)
            if not patterns:
                continue

            scoped_relative_path = _lexical_relative_path(path, ignore_dir)
            if path.is_dir():
                scoped_relative_path = f"{scoped_relative_path}/"

            match = _matches_airflowignore_patterns(scoped_relative_path, patterns)
            if match is not None:
                ignored = match

        return ignored

    root_patterns = ignore_patterns_by_dir.get(dags_folder, [])
    if root_patterns:
        match = _matches_airflowignore_patterns(path.name, root_patterns)
        if match is not None:
            ignored = match

    return ignored


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
    logging.info("Loading DAGs from %s", dags_folder)
    if suffix is None:
        suffix = [".yaml", ".yml"]
    candidate_dag_files = []

    if config_filepath:
        factory = _DagFactory(
            config_filepath=config_filepath,
            defaults_config_path=defaults_config_path,
            defaults_config_dict=defaults_config_dict,
        )
        factory._generate_dags(globals_dict)
    elif config_dict:
        factory = _DagFactory(
            config_dict=config_dict,
            defaults_config_path=defaults_config_path,
            defaults_config_dict=defaults_config_dict,
        )
        factory._generate_dags(globals_dict)
    else:
        dags_folder_path = Path(dags_folder)
        ignore_patterns = _load_airflowignore(dags_folder)

        for root_path, _dirs, files in _iter_dags_folder_contents(dags_folder_path):
            for file_name in files:
                if any(file_name.endswith(suf) for suf in suffix):
                    candidate_dag_files.append(root_path / file_name)

        for config_file_path in candidate_dag_files:
            if _should_ignore_file(config_file_path, dags_folder_path, ignore_patterns):
                logging.debug("Ignoring file %s (matched ignore pattern)", config_file_path)
                continue

            config_file_abs_path = str(config_file_path.absolute())
            logging.info("Loading %s", config_file_abs_path)
            try:
                factory = _DagFactory(
                    config_file_abs_path,
                    defaults_config_path=defaults_config_path,
                    defaults_config_dict=defaults_config_dict,
                )
                factory._generate_dags(globals_dict)
            except Exception:  # pylint: disable=broad-except
                logging.exception("Failed to load dag from %s", config_file_path)
            else:
                logging.info("DAG loaded: %s", config_file_path)
