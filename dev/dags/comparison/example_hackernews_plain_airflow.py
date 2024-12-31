from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from hackernews import summarize

with DAG(dag_id="example_hackernews_plain_airflow", schedule=None, start_date=datetime(2022, 3, 4)) as dag:

    fetch_top_ten_news = BashOperator(
        task_id="fetch_top_ten_news",
        bash_command="curl -s https://hacker-news.firebaseio.com/v0/topstories.json  | jq -c -r '.[0:10]'",
    )

    fetch_first_top_news = BashOperator(
        task_id="fetch_first_top_news",
        bash_command="""
            echo {{ task_instance.xcom_pull(task_ids='fetch_top_ten_news') }} | jq -c -r '.[0]' |  xargs -I {} curl -s 'https://hacker-news.firebaseio.com/v0/item/{}.json'
        """,
    )

    fetch_second_top_news = BashOperator(
        task_id="fetch_second_news",
        bash_command="""
            echo {{ task_instance.xcom_pull(task_ids='fetch_top_ten_news') }} | jq -c -r '.[1]' |  xargs -I {} curl -s 'https://hacker-news.firebaseio.com/v0/item/{}.json'
        """,
    )

    summarize = PythonOperator(task_id="summarize", python_callable=summarize)

    fetch_top_ten_news >> [fetch_first_top_news, fetch_second_top_news] >> summarize
