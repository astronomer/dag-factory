#!/bin/bash

set -v
set -x
set -e

AIRFLOW_VERSION="$1"
PYTHON_VERSION="$2"

# Use this to set the appropriate Python environment in Github Actions,
# while also not assuming --system when running locally.
if [ "$GITHUB_ACTIONS" = "true" ] && [ -z "${VIRTUAL_ENV}" ]; then
  py_path=$(which python)
  virtual_env_dir=$(dirname "$(dirname "$py_path")")
  export VIRTUAL_ENV="$virtual_env_dir"
fi

echo "${VIRTUAL_ENV}"

if [ "$AIRFLOW_VERSION" = "3.0" ] ; then
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.2/constraints-$PYTHON_VERSION.txt"
else
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.0/constraints-$PYTHON_VERSION.txt"
fi;

curl -sSL "$CONSTRAINT_URL" -o /tmp/constraint.txt
# Workaround to remove PyYAML constraint that will work on both Linux and MacOS
sed '/PyYAML==/d' /tmp/constraint.txt > /tmp/constraint.txt.tmp
mv /tmp/constraint.txt.tmp /tmp/constraint.txt

pip install uv
uv pip install pip --upgrade


if [ "$AIRFLOW_VERSION" = "3.0" ]; then
  uv pip install --no-cache-dir "apache-airflow>=3.0.2" --constraint /tmp/constraint.txt
else
  uv pip install --no-cache-dir "apache-airflow==$AIRFLOW_VERSION" --constraint /tmp/constraint.txt
fi;

# uv pip install apache-airflow-providers-cncf-kubernetes --constraint /tmp/constraint.txt
rm /tmp/constraint.txt

actual_version=$(airflow version | cut -d. -f1,2)

if [ "$actual_version" = $AIRFLOW_VERSION ]; then
    echo "Version is as expected: $AIRFLOW_VERSION"
else
    echo "Version does not match. Expected: $AIRFLOW_VERSION, but got: $actual_version"
    exit 1
fi
