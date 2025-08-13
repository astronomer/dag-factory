"""
PyPI stats utility functions.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pandas as pd

DEFAULT_PYPI_PROJECTS = [
    "apache-airflow",
    "dag-factory",
    "astronomer-cosmos",
]

logger = logging.getLogger(__name__)

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
    response = httpx.get(url)

    try:
        response.raise_for_status()
        package_json = response.json()
        package_data = package_json["data"]
        package_data["package_name"] = package_name
        return package_data

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP Error while fetching {package_name}: {e.response.status_code}")
        logger.error(f"Response content: {e.response.text}")
    except ValueError as e:
        logger.error(f"JSON Decode Error for {package_name}: {e}")
        logger.error(f"Response content: {response.text}")
    except KeyError as e:
        logger.error(f"Unexpected response format for {package_name}: {e}")
        logger.error(f"Response content: {response.text}")

    return {
        "package_name": package_name,
        "last_day": 0,
        "last_week": 0,
        "last_month": 0,
    }


def summarize(data: list[dict[str, Any]]) -> str:
    """
    Given a list with PyPI stats data, create a table summarizing it, sorting by the last day total downloads.
    """
    df = pd.DataFrame(data)
    first_column = "package_name"
    sorted_columns = [first_column] + [col for col in df.columns if col != first_column]
    df = df[sorted_columns].sort_values(by="last_day", ascending=False)
    markdown_output = df.to_markdown(index=False)
    logger.info(markdown_output)
    return markdown_output


# ----8<--- [ end: pypi_stats ]

if __name__ == "__main__":
    pypi_projects_list = get_pypi_projects_list()
    all_data = []
    for pypi_project_name in pypi_projects_list:
        project_data = fetch_pypi_stats_data(pypi_project_name)
        all_data.append(project_data)
    summarize(data=all_data)
