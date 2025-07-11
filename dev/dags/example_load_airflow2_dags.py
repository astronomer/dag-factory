import os
from pathlib import Path

from dagfactory import load_yaml_dags

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"
CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))
config_dir = str(CONFIG_ROOT_DIR / "airflow2")

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=config_dir,
)
