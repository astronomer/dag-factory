"""
PyPI stats utility functions.
"""

from __future__ import annotations

from typing import Any

import httpx
import pandas as pd

DEFAULT_PYPI_PROJECTS = [
    "apache-airflow",
    "dag-factory",
    "astronomer-cosmos",
]


# ----8<---   [ start: pypi_stats ]


def get_pypi_projects_list(**kwargs: dict[str, Any]) -> list[str]:
    """
    Return a list of PyPI project names to be analysed.
    """
    projects_from_ui = kwargs.get("dag_run").conf.get("pypi_projects") if kwargs.get("dag_run") else None
    if projects_from_ui is None:
        pypi_projects = DEFAULT_PYPI_PROJECTS
    else:
        pypi_projects = projects_from_ui
    return pypi_projects


def fetch_pypi_stats_data(package_name: str) -> dict[str, Any]:
    """
    Given a PyPI project name, return the PyPI stats data associated to it.
    """
    url = f"https://pypistats.org/api/packages/{package_name}/recent"
    package_json = httpx.get(url).json()
    package_data = package_json["data"]
    package_data["package_name"] = package_name
    return package_data


def summarize(values: list[dict[str, Any]]):
    """
    Given a list with PyPI stats data, create a table summarizing it, sorting by the last day total downloads.
    """
    df = pd.DataFrame(values)
    first_column = "package_name"
    sorted_columns = [first_column] + [col for col in df.columns if col != first_column]
    df = df[sorted_columns].sort_values(by="last_day", ascending=False)
    markdown_output = df.to_markdown(index=False)
    print(markdown_output)
    return markdown_output


# ----8<--- [ end: pypi_stats ]

if __name__ == "__main__":
    pypi_projects_list = get_pypi_projects_list()
    all_data = []
    for pypi_project_name in pypi_projects_list:
        project_data = fetch_pypi_stats_data(pypi_project_name)
        all_data.append(project_data)
    summarize(data=all_data)
