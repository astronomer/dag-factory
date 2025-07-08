# FROM quay.io/astronomer/astro-runtime:12.7.0
FROM astrocrpublic.azurecr.io/runtime:3.0-4

ENV CONFIG_ROOT_DIR=/usr/local/airflow/dags/
ENV AUTO_CONVERT_TO_AF3=True

USER root

RUN apt-get update && apt-get install -y jq

# Install uv for faster package management
RUN pip install uv

RUN uv pip install --system /usr/local/airflow/include/*.whl

USER astro
