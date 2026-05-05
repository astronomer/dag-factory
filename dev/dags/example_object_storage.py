import sys
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent
CONFIG_ROOT_DIR_STR = str(CONFIG_ROOT_DIR)
# Astro DAG bundles deploy to a timestamped path not in sys.path; insert it so helper modules are importable.
if CONFIG_ROOT_DIR_STR not in sys.path:
    sys.path.insert(0, CONFIG_ROOT_DIR_STR)

config_file = str(CONFIG_ROOT_DIR / "example_object_storage.yml")

load_yaml_dags(
    globals_dict=globals(),
    config_filepath=config_file,
)
