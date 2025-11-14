from pathlib import Path

from dagfactory import load_yaml_dags

CONFIG_ROOT_DIR = Path(__file__).resolve().parent
config_dir = str(CONFIG_ROOT_DIR / "airflow2")

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=config_dir,
)
