from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent
config_dir = str(CONFIG_ROOT_DIR / "comparison")

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=config_dir,
)
