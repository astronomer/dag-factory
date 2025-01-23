FROM quay.io/astronomer/astro-runtime:12.6.0

ENV CONFIG_ROOT_DIR=/usr/local/airflow/dags/

USER root

RUN apt-get update && apt-get install -y jq

USER astro

RUN pip install /usr/local/airflow/include/*.whl
