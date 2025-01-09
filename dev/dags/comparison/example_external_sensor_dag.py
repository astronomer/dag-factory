from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

with DAG(
    dag_id="example_external_task_sensor_plain_airflow_producer", start_date=datetime(2025, 1, 1), schedule="@daily"
) as producer_dag:
    producer_task = EmptyOperator(task_id="producer_task")


with DAG(
    dag_id="example_external_task_sensor_plain_airflow_consumer", start_date=datetime(2025, 1, 1), schedule="@daily"
) as consumer_dag:

    wait_for_producer_task = ExternalTaskSensor(
        task_id="wait_for_producer_task",
        external_dag_id="example_external_task_sensor_plain_airflow_producer",  # DAG ID of the producer
        external_task_id="producer_task",  # Task ID to wait for in the producer DAG
        mode="poke",  # Can also use "reschedule" for efficiency
        timeout=600,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        retries=2,  # Number of retries before failing
    )

    consumer_task = EmptyOperator(task_id="consumer_task")

    wait_for_producer_task >> consumer_task


def one_day_ago(execution_date: datetime) -> datetime:
    """
    One minute after the execution time.
    """
    return execution_date - timedelta(days=1)


with DAG(
    dag_id="example_external_task_sensor_plain_airflow_consumer2", start_date=datetime(2025, 1, 2), schedule="@daily"
) as consumer_dag:

    wait_for_producer_task = ExternalTaskSensor(
        task_id="wait_for_producer_task",
        external_dag_id="example_external_task_sensor_plain_airflow_producer",  # DAG ID of the producer
        external_task_id="producer_task",  # Task ID to wait for in the producer DAG
        execution_date_fn=one_day_ago,
    )

    consumer_task = EmptyOperator(task_id="consumer_task")

    wait_for_producer_task >> consumer_task


with DAG(
    dag_id="example_external_task_sensor_plain_airflow_consumer3", start_date=datetime(2025, 1, 3), schedule="@daily"
) as consumer_dag:

    wait_for_producer_task = ExternalTaskSensor(
        task_id="wait_for_producer_task",
        external_dag_id="example_external_task_sensor_plain_airflow_producer",  # DAG ID of the producer
        external_task_id="producer_task",  # Task ID to wait for in the producer DAG
        execution_delta=timedelta(days=2),  # Two days ago
    )

    consumer_task = EmptyOperator(task_id="consumer_task")

    wait_for_producer_task >> consumer_task
