from __future__ import annotations

import json

import pandas as pd


def summarize(**kwargs):
    """
    Given the Airflow context is provided to this function, it will extract the XCom hackernews records from its
    upstream tasks and summarise in Markdown.
    """
    ti = kwargs["ti"]
    upstream_task_ids = ti.task.upstream_task_ids  # Get upstream task IDs dynamically
    values = [json.loads(ti.xcom_pull(task_ids=task_id)) for task_id in upstream_task_ids]

    df = pd.DataFrame(values)
    selected_columns = ["title", "url"]
    df = df[selected_columns]
    markdown_output = df.to_markdown(index=False)
    print(markdown_output)
    return markdown_output
