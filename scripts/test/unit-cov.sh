#!/bin/bash

# Unset AIRFLOW_HOME to avoid using the local airflow installation
env -u AIRFLOW_HOME pytest \
    -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    --ignore=tests/test_example_dags.py
