#!/bin/bash

set -x
set -e


pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check

ln -s dev/dags dags

pytest -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    -m integration
