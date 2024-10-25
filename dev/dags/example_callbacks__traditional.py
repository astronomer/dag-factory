"""
example_callbacks__traditional.py

Author: Jake Roach
Date: 2024-10-24
"""

# Import modules here
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

from datetime import datetime

from customized.callables.python import succeeding_task, failing_task

with DAG(
    dag_id="example_callbacks__traditional",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "on_failure_callback": send_slack_notification(
            slack_conn_id="example_slack_conn",
            text=f"""
                :red_circle: Task Failed.
                This task has failed and needs to be addressed.
                Please remediate this issue ASAP.
            """,
            username="Airflow",
            channel="cse-callback-demo"
        )
    }
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=succeeding_task
    )

    end = PythonOperator(
        task_id="end",
        python_callable=failing_task
    )

    start >> end
