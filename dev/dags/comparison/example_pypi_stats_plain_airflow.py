from __future__ import annotations

from datetime import datetime
from typing import Any

try:
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.decorators import task
except ImportError:
    from airflow.decorators import task
    from airflow.models.dag import DAG
from dev.dags.pypi_stats import fetch_pypi_stats_data, get_pypi_projects_list, summarize

with DAG(dag_id="example_pypi_stats_plain_airflow", schedule=None, start_date=datetime(2022, 3, 4)) as dag:

    @task
    def get_pypi_projects_list_():
        return get_pypi_projects_list()

    @task
    def fetch_pypi_stats_data_(project_name: str):
        return fetch_pypi_stats_data(project_name)

    @task
    def summarize_(values: list[dict[str, Any]]):
        return summarize(values)

    pypi_stats_data = fetch_pypi_stats_data_.expand(project_name=get_pypi_projects_list_())
    summarize_(pypi_stats_data)
