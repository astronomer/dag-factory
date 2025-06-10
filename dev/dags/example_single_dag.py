import os
from pathlib import Path

from dagfactory import load_single_dag, load_single_dag_directory

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"
CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))

# Example 1: Load a single DAG file
dag_file = str(CONFIG_ROOT_DIR / "example.dag.yaml")
load_single_dag(globals_dict=globals(), dag_file_path=dag_file)

# Example 2: Load all DAGs from a directory
dags_dir = str(CONFIG_ROOT_DIR / "single_dags")
load_single_dag_directory(globals_dict=globals(), dags_folder=dags_dir)
