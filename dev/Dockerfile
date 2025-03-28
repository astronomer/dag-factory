FROM quay.io/astronomer/astro-runtime:12.2.0

ENV CONFIG_ROOT_DIR=/usr/local/airflow/dags/

USER root

RUN apt-get update && apt-get install -y jq

USER astro

# Install uv for faster package management
RUN pip install uv

RUN uv pip install --system /usr/local/airflow/include/*.whl
