import os
from typing import Dict, Any, Union

import yaml
from airflow.models import DAG

from dagfactory.dagbuilder import DagBuilder


class DagFactory(object):
    """
    :param config_filepath: the filepath of the DAG factory YAML config file.
        Must be absolute path to file.
    """

    def __init__(self, config_filepath: str) -> None:
        DagFactory._validate_config_filepath(config_filepath=config_filepath)
        self.config_filepath: str = config_filepath
        self.config: Dict[str, Any] = DagFactory._load_config(config_filepath=config_filepath)

    @staticmethod
    def _validate_config_filepath(config_filepath: str) -> None:
        """
        Validates config file path is absolute
        """
        if not os.path.isabs(config_filepath):
            raise Exception("DAG Factory `config_filepath` must be absolute path")

    @staticmethod
    def _load_config(config_filepath: str) -> Dict[str, Any]:
        """
        Loads YAML config file to dictionary

        :returns: dict from YAML config file
        """
        try:
            config: Dict[str, Any] = yaml.load(stream=open(config_filepath, "r"))
        except Exception as e:
            raise Exception(f"Invalid DAG Factory config file; err: {e}")
        return config

    def get_dag_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns configuration for each the DAG in factory

        :returns: dict with configuration for dags
        """
        return {dag: self.config[dag] for dag in self.config.keys() if dag != "default"}

    def get_default_config(self) -> Dict[str, Any]:
        """
        Returns defaults for the DAG factory. If no defaults exist, returns empty dict.

        :returns: dict with default configuration
        """
        return self.config.get("default", {})

    def generate_dags(self, globals: Dict[str, Any]) -> None:
        """
        Generates DAGs from YAML config

        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        dag_configs: Dict[str, Dict[str, Any]] = self.get_dag_configs()
        default_config: Dict[str, Any] = self.get_default_config()

        for dag_name, dag_config in dag_configs.items():
            dag_builder: DagBuilder = DagBuilder(dag_name=dag_name,
                                                 dag_config=dag_config,
                                                 default_config=default_config)
            try:
                dag: Dict[str, Union[str, DAG]] = dag_builder.build()
            except Exception as e:
                raise Exception(
                    f"Failed to generate dag {dag_name}. make sure config is properly populated. err:{e}"
                )
            globals[dag["dag_id"]]: DAG = dag["dag"]
