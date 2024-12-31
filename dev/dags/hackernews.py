from __future__ import annotations

import json

import pandas as pd


def summarize(**kwargs):
    """
    Given the Airflow context is provided to this function, it will extract the XCom hackernews records from its
    upstream tasks and summarise in Markdown.

    Example:

        summarize = PythonOperator(
            task_id="summarize",
            python_callable=summarize
        )

        [fetch_first_top_news, fetch_second_top_news] >> summarize

        Prints:

        | title                                                                       | url                                                                                                                    |
        |:----------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------|
        | I keep turning my Google Sheets into phone-friendly webapps                 | https://arstechnica.com/gadgets/2024/12/making-tiny-no-code-webapps-out-of-spreadsheets-is-a-weirdly-fulfilling-hobby/ |
        | Coconut by Meta AI – Better LLM Reasoning with Chain of Continuous Thought? | https://aipapersacademy.com/chain-of-continuous-thought/                                                               |

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
