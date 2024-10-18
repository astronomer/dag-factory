#!/bin/bash

set -x
set -e


pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check


pytest -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m integration
