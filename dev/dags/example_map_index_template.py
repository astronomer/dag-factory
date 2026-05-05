import sys
from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent
# Astro DAG bundles deploy to a timestamped path not in sys.path; insert it so helper modules are importable.
config_root_dir_str = str(CONFIG_ROOT_DIR)
if config_root_dir_str not in sys.path:
    sys.path.insert(0, config_root_dir_str)

config_file = str(CONFIG_ROOT_DIR / "example_map_index_template.yml")


load_yaml_dags(
    globals_dict=globals(),
    config_filepath=config_file,
)
