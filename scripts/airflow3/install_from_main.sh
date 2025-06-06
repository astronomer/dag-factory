#!/bin/bash

set -v
set -x
set -e

: "${AIRFLOW_REPO_DIR:?Environment variable AIRFLOW_REPO_DIR is not set}"
echo "AIRFLOW_REPO_DIR is set to '$AIRFLOW_REPO_DIR'"

DAG_FACTORY_ROOT="$PWD"

cd "$AIRFLOW_REPO_DIR"
git checkout main && git pull

uv pip uninstall apache-airflow-core || true
uv pip uninstall apache-airflow-task-sdk || true
uv pip uninstall apache-airflow-providers-fab || true
uv pip uninstall apache-airflow || true
uv pip uninstall apache-airflow-providers-git || true

rm -rf dist

uv pip install -e "$AIRFLOW_REPO_DIR/dev/breeze" --force

breeze release-management prepare-provider-distributions \
      --distributions-list celery,common.io,common.compat,fab,standard,openlineage,git \
      --distribution-format wheel

breeze release-management prepare-airflow-distributions --distribution-format wheel

cd task-sdk
uv build --package apache-airflow-task-sdk --wheel

cd ..

uv pip install dist/*

cd "$DAG_FACTORY_ROOT"
