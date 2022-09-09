"""Module contains code for loading a DagFactory config and generating DAGs"""
import datetime
import os
import traceback
from typing import Any, Dict, Optional, Union, List

import pendulum
import yaml
from airflow.configuration import conf
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from dagfactory.dagbuilder import DagBuilder

# these are params that cannot be a dag name
SYSTEM_PARAMS: List[str] = ["default", "task_groups"]
ALLOWED_CONFIG_FILE_SUFFIX: List[str] = ["yaml", "yml"]


class DagFactory:
    """
    Takes a YAML config or a python dictionary and generates DAGs.

    :param config_filepath: the filepath of the DAG factory YAML config file.
        Must be absolute path to file. Cannot be used with `config`.
    :type config_filepath: str
    :param config: DAG factory config dictionary. Cannot be user with `config_filepath`.
    :type config: dict
    """

    DAGBAG_IMPORT_ERROR_TRACEBACKS = conf.getboolean('core', 'dagbag_import_error_tracebacks')
    DAGBAG_IMPORT_ERROR_TRACEBACK_DEPTH = conf.getint('core', 'dagbag_import_error_traceback_depth')

    def __init__(
            self,
            config_filepath: Optional[str] = None,
            default_config: Optional[dict] = None,
            config: Optional[dict] = None
    ) -> None:
        assert bool(config_filepath) ^ bool(
            config
        ), "Either `config_filepath` or `config` should be provided"
        self.default_config = default_config
        if config_filepath:
            DagFactory._validate_config_filepath(config_filepath=config_filepath)
            self.config: Dict[str, Any] = DagFactory._load_config(
                config_filepath=config_filepath
            )
        if config:
            self.config: Dict[str, Any] = config

    @classmethod
    def from_directory(cls, config_dir, globals: Dict[str, Any], root_default_config: Optional[Dict[str, Any]] = None):
        """
        Make instances of DagFactory for each yaml configuration files within a directory
        """
        cls._validate_config_filepath(config_dir)
        subs = os.listdir(config_dir)

        # get default configurations if exist
        allowed_default_filename = ['default.' + sfx for sfx in ALLOWED_CONFIG_FILE_SUFFIX]
        maybe_default_file = [sub for sub in subs if sub in allowed_default_filename]

        # get the configurations that are not default
        subs_fpath = [os.path.join(config_dir, sub) for sub in subs if sub not in maybe_default_file]

        # if there is no default.yaml in current sub folder, use the defaults from the parent folder
        default_config = root_default_config
        if len(maybe_default_file) > 0:
            default_file = maybe_default_file[0]
            default_fpath = os.path.join(config_dir, default_file)
            default_config = cls._load_config(
                config_filepath=default_fpath
            )

        # load dags from each yaml configuration files
        import_failures = {}
        for sub_fpath in subs_fpath:
            if os.path.isdir(sub_fpath):
                cls.from_directory(sub_fpath, globals, default_config)
            elif os.path.isfile(sub_fpath) and sub_fpath.split('.')[-1] in ALLOWED_CONFIG_FILE_SUFFIX:
                # catch the errors so the rest of the dags can still be imported
                try:
                    dag_factory = cls(config_filepath=sub_fpath, default_config=default_config)
                    dag_factory.generate_dags(globals)
                except Exception as e:
                    if cls.DAGBAG_IMPORT_ERROR_TRACEBACKS:
                        import_failures[sub_fpath] = traceback.format_exc(
                            limit=-cls.DAGBAG_IMPORT_ERROR_TRACEBACK_DEPTH
                        )
                    else:
                        import_failures[sub_fpath] = str(e)

        # in the end we want to surface the error messages if there's any
        if import_failures:
            # reformat import_failures so they are reader friendly
            import_failures_reformatted = ''
            for import_loc, import_trc in import_failures.items():
                import_failures_reformatted += '\n' + f'Failed to generate dag from {import_loc}' + \
                                               '-'*100 + '\n' + import_trc + '\n'

            alert_dag_id = (os.path.split(os.path.abspath(globals['__file__']))[-1]).split('.')[0] + \
                           '_dag_factory_import_error_messenger'
            with DAG(
                dag_id=alert_dag_id,
                schedule_interval="@once",
                default_args={
                    "depends_on_past": False,
                    "start_date": datetime.datetime(
                        2020, 1, 1, tzinfo=pendulum.timezone("America/New_York")
                    )
                },
                tags=[f"dag_factory_import_errors"]
            ) as alert_dag:
                DummyOperator(
                    task_id='import_error_messenger',
                    doc_json=import_failures_reformatted
                )
            globals[alert_dag_id] = alert_dag

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
        # pylint: disable=consider-using-with
        try:
            config: Dict[str, Any] = yaml.load(
                stream=open(config_filepath, "r", encoding="utf-8"),
                Loader=yaml.FullLoader,
            )
        except Exception as err:
            raise Exception("Invalid DAG Factory config file") from err
        return config

    def get_dag_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns configuration for each the DAG in factory

        :returns: dict with configuration for dags
        """
        return {
            dag: self.config[dag]
            for dag in self.config.keys()
            if dag not in SYSTEM_PARAMS
        }

    def get_default_config(self) -> Dict[str, Any]:
        """
        Returns defaults for the DAG factory. If no defaults exist, returns empty dict.

        :returns: dict with default configuration
        """
        if self.default_config:
            return self.default_config
        return self.config.get("default", {})

    def build_dags(self) -> Dict[str, DAG]:
        """Build DAGs using the config file."""
        dag_configs: Dict[str, Dict[str, Any]] = self.get_dag_configs()
        default_config: Dict[str, Any] = self.get_default_config()

        dags: Dict[str, Any] = {}

        for dag_name, dag_config in dag_configs.items():
            dag_config["task_groups"] = dag_config.get("task_groups", {})
            dag_builder: DagBuilder = DagBuilder(
                dag_name=dag_name,
                dag_config=dag_config,
                default_config=default_config,
            )
            try:
                dag: Dict[str, Union[str, DAG]] = dag_builder.build()
                dags[dag["dag_id"]]: DAG = dag["dag"]
            except Exception as err:
                raise Exception(
                    f"Failed to generate dag {dag_name}. verify config is correct"
                ) from err

        return dags

    # pylint: disable=redefined-builtin
    @staticmethod
    def register_dags(dags: Dict[str, DAG], globals: Dict[str, Any]) -> None:
        """Adds `dags` to `globals` so Airflow can discover them.

        :param: dags: Dict of DAGs to be registered.
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

    # pylint: enable=redefined-builtin
