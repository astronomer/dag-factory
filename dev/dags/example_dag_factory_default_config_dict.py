# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory import load_yaml_dags

daily_etl = {
    "daily_etl": {
        "schedule": "@daily",
        "tasks": [
            {"task_id": "extract", "operator": "airflow.operators.bash.BashOperator", "bash_command": "echo extract"},
            {
                "task_id": "transform",
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo transform",
                "dependencies": ["extract"],
            },
            {
                "task_id": "load",
                "operator": "airflow.operators.bash.BashOperator",
                "bash_command": "echo load",
                "dependencies": ["transform"],
            },
        ],
    }
}

load_yaml_dags(
    globals_dict=globals(),
    config_dict=daily_etl,
    default_args_config_dict={"default_args": {"start_date": "2025-01-01", "owner": "global_owner"}},
)
