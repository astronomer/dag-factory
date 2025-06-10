"""Module for loading single DAG YAML files with the new format."""

import logging
import os
from pathlib import Path
from typing import Any, Dict

import yaml
from airflow.configuration import conf as airflow_conf

from dagfactory.dagfactory import DagFactory
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException


def load_single_dag(
    globals_dict: Dict[str, Any],
    dag_file_path: str,
    default_args_config_path: str = airflow_conf.get("core", "dags_folder"),
) -> None:
    """
    Loads a single DAG from a YAML file with the new format.
    The file should be named <dag-id>.dag.yaml or <dag-id>.dag.yml.
    The YAML file should contain a single DAG definition without a top-level dag_id key.

    :param globals_dict: The globals() from the file used to generate DAGs
    :param dag_file_path: Path to the YAML file containing the DAG definition
    :param default_args_config_path: The folder path where defaults.yml exists
    """
    if not os.path.isabs(dag_file_path):
        raise DagFactoryConfigException("DAG file path must be absolute path")

    file_path = Path(dag_file_path)
    if not file_path.exists():
        raise DagFactoryConfigException(f"DAG file not found: {dag_file_path}")

    if not file_path.name.endswith((".dag.yaml", ".dag.yml")):
        raise DagFactoryConfigException("DAG file must end with .dag.yaml or .dag.yml")

    # Extract DAG ID from filename
    dag_id = file_path.stem.split(".")[0]

    if not dag_id:
        raise DagFactoryConfigException("DAG ID is required in the YAML file name")

    logging.info("Loading DAG from %s", dag_file_path)
    try:
        # Load the YAML file
        with open(dag_file_path, "r", encoding="utf-8") as fp:
            config_with_env = os.path.expandvars(fp.read())
            dag_config = yaml.load(stream=config_with_env, Loader=yaml.FullLoader)

        # Create a config with the DAG ID as the top-level key
        config = {dag_id: dag_config}

        # Create DAG factory and generate DAG
        factory = DagFactory(config=config, default_args_config_path=default_args_config_path)
        factory.generate_dags(globals_dict)
        logging.info("DAG loaded: %s", dag_file_path)
    except Exception as err:
        raise DagFactoryException(f"Failed to load DAG from {dag_file_path}: {err}") from err


def load_single_dag_directory(
    globals_dict: Dict[str, Any],
    dags_folder: str = airflow_conf.get("core", "dags_folder"),
    default_args_config_path: str = airflow_conf.get("core", "dags_folder"),
) -> None:
    """
    Loads all DAGs from a directory that match the new format (*.dag.yaml or *.dag.yml).

    :param globals_dict: The globals() from the file used to generate DAGs
    :param dags_folder: Path to the folder containing DAG YAML files
    :param default_args_config_path: The folder path where defaults.yml exists
    """
    logging.info("Loading DAGs from %s", dags_folder)

    # Find all .dag.yaml and .dag.yml files
    dag_files = []
    for suffix in [".dag.yaml", ".dag.yml"]:
        dag_files.extend(Path(dags_folder).rglob(f"*{suffix}"))

    for dag_file in dag_files:
        try:
            load_single_dag(
                globals_dict=globals_dict,
                dag_file_path=str(dag_file.absolute()),
                default_args_config_path=default_args_config_path,
            )
        except Exception:  # pylint: disable=broad-except
            logging.exception("Failed to load DAG from %s", dag_file)
