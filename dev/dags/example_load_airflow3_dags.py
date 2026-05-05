import sys
from pathlib import Path

from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent
# Astro DAG bundles deploy to a timestamped path not in sys.path; insert it so helper modules are importable.
sys.path.insert(0, str(CONFIG_ROOT_DIR))
config_dir = str(CONFIG_ROOT_DIR / "airflow3")

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=config_dir,
)
