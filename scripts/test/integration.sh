#!/bin/bash

set -x
set -e


pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check

# Necessary for overcoming the following issue with Airflow 2.3 and 2.4:
# ImportError: Pandas requires version '0.9.0' or newer of 'tabulate' (version '0.8.9' currently installed)
pip install "tabulate>=0.9.0"

pytest -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    -m integration
