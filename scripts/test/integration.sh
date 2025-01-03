#!/bin/bash

set -x
set -e


pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check

# Necessary for overcoming the following issue with Airflow 2.2:
# ERROR: Cannot install apache-airflow==2.2.0, apache-airflow==2.2.1, apache-airflow==2.2.2, apache-airflow==2.2.3, apache-airflow==2.2.4, apache-airflow==2.2.5, httpx>=0.25.0 and tabulate>=0.9.0 because these package versions have conflicting dependencies.
pip install "tabulate>=0.9.0"

pytest -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    -m integration
