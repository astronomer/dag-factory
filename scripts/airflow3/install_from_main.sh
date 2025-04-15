#!/bin/bash

set -v
set -x
set -e

: "${AIRFLOW_REPO_DIR:?Environment variable AIRFLOW_REPO_DIR is not set}"
echo "AIRFLOW_REPO_DIR is set to '$AIRFLOW_REPO_DIR'"

DAG_FACTORY_ROOT="$PWD"

cd "$AIRFLOW_REPO_DIR"
git checkout main && git pull

pip uninstall -y apache-airflow-core
pip uninstall -y apache-airflow-task-sdk
pip uninstall -y apache-airflow-providers-fab
pip uninstall -y apache-airflow
pip uninstall -y apache-airflow-providers-git

rm -rf dist

pip install uv

pip install -e "$AIRFLOW_REPO_DIR/dev/breeze" --force

breeze release-management prepare-provider-distributions \
      --distributions-list celery,common.io,common.compat,fab,standard,openlineage,git \
      --distribution-format wheel

breeze release-management prepare-airflow-distributions --distribution-format wheel

cd task-sdk
uv build --package apache-airflow-task-sdk --wheel

cd ..

pip install dist/*

cd "$DAG_FACTORY_ROOT"
