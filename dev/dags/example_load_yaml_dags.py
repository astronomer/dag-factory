import sys
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent
config_root_dir_str = str(CONFIG_ROOT_DIR)
# Astro DAG bundles deploy to a timestamped path not in sys.path; insert it so helper modules are importable.
if config_root_dir_str not in sys.path:
    sys.path.insert(0, config_root_dir_str)
config_dir = str(CONFIG_ROOT_DIR / "comparison")

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=config_dir,
)
